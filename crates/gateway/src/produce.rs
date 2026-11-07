use crate::server::{AppendBatch, State};
use anyhow::{ensure, Result};
use broker_protocol::{Reader, Writer};
use serde_json::Value;
use std::collections::BTreeMap;
use tokio::task::JoinSet;

// Compatibility fallback for older extensions without binary append. The
// shared database pool remains capped at 16; fallback requests use at most four.
const CONCURRENCY: usize = 4;
const MAX_PARTITIONS: usize = 4096;

pub async fn handle(state: &State, reader: &mut Reader<'_>) -> Result<Option<Writer>> {
    ensure!(
        reader.nullable_string()?.is_none(),
        "transactions unsupported"
    );
    let acks = reader.i16()?;
    ensure!([-1, 0, 1].contains(&acks), "invalid acks");
    reader.i32()?;
    let mut topics = Vec::new();
    let mut groups = BTreeMap::<(String, i32), Vec<(usize, Vec<u8>)>>::new();
    let mut count = 0;
    let mut expanded_bytes = 0usize;
    // Parse and bound the entire request before any database operation. Repeated
    // topic/partition entries share one ordered group, retaining their wire order.
    for _ in 0..reader.count()? {
        let topic = reader.string()?;
        let mut partitions = Vec::new();
        for _ in 0..reader.count()? {
            ensure!(count < MAX_PARTITIONS, "produce partition limit");
            let partition = reader.i32()?;
            let bytes = reader.bytes()?;
            expanded_bytes += topic.len() + bytes.len() + 16;
            ensure!(
                expanded_bytes <= broker_protocol::MAX_FRAME,
                "produce expansion limit"
            );
            groups
                .entry((topic.clone(), partition))
                .or_default()
                .push((count, bytes));
            partitions.push((partition, count));
            count += 1;
        }
        topics.push((topic, partitions));
    }

    // Reject a malformed frame before any partition can become durable.
    reader.done()?;

    let mut results = vec![None; count];
    if state.binary_append {
        let mut batch = AppendBatch {
            topics: Vec::new(),
            partitions: Vec::new(),
            records: Vec::new(),
        };
        let mut indices = Vec::with_capacity(count);
        // BTreeMap gives one global lock order across concurrent requests,
        // regardless of their wire order. Duplicates retain their wire order.
        for ((topic, partition), entries) in groups {
            for (index, bytes) in entries {
                batch.topics.push(topic.clone());
                batch.partitions.push(partition);
                batch.records.push(bytes);
                indices.push(index);
            }
        }
        for (index, result) in indices.into_iter().zip(state.append_many(&batch).await?) {
            results[index] = Some(result);
        }
    } else {
        let mut pending = groups.into_iter();
        let mut running = JoinSet::new();
        loop {
            while running.len() < CONCURRENCY {
                let Some(((topic, partition), entries)) = pending.next() else {
                    break;
                };
                let state = state.clone();
                running.spawn(async move {
                    let mut completed = Vec::with_capacity(entries.len());
                    for (index, bytes) in entries {
                        // The extension validates the whole record set at the durable
                        // boundary on both the binary and compatibility paths.
                        let result = state.append(&topic, partition, &bytes).await?;
                        completed.push((index, result));
                    }
                    Ok::<_, anyhow::Error>(completed)
                });
            }
            let Some(completed) = running.join_next().await else {
                break;
            };
            // On error/cancellation JoinSet aborts outstanding work. State::db drops
            // unconfirmed connections instead of returning them to the pool. Some
            // partitions may have committed: retries retain at-least-once semantics.
            for (index, result) in completed?? {
                results[index] = Some(result);
            }
        }
    }

    if acks == 0 {
        return Ok(None);
    }
    let mut response = Writer::default();
    response.array(topics.len());
    for (topic, partitions) in topics {
        response.string(&topic);
        response.array(partitions.len());
        for (partition, index) in partitions {
            let result: &Value = results[index].as_ref().expect("completed partition");
            response.i32(partition);
            response.i16(result["error"].as_i64().unwrap_or(0) as i16);
            response.i64(result["offset"].as_i64().unwrap_or(-1));
            response.i64(-1);
        }
    }
    response.i32(0);
    Ok(Some(response))
}

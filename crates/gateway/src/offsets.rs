use crate::server::State;
use anyhow::{ensure, Context, Result};
use broker_protocol::{Reader, Writer};
use serde_json::json;

const MAX_PARTITIONS: usize = 4096;

pub async fn commit(state: &State, reader: &mut Reader<'_>) -> Result<Writer> {
    let group = reader.string()?;
    let generation = reader.i32()?;
    let member = reader.string()?;
    reader.i64()?; // Deprecated Kafka retention override; broker policy owns retention.
    let mut topics = Vec::new();
    let mut requests = Vec::new();
    let mut expanded_bytes = 0usize;
    for _ in 0..reader.count()? {
        let topic = reader.string()?;
        let mut partitions = Vec::new();
        for _ in 0..reader.count()? {
            ensure!(
                requests.len() < MAX_PARTITIONS,
                "offset commit partition limit"
            );
            let partition = reader.i32()?;
            let offset = reader.i64()?;
            let metadata = reader.nullable_string()?.unwrap_or_default();
            // Shared wire strings are copied into each database operation. Bound
            // that expansion as well as the frame and partition count.
            expanded_bytes += group.len() + member.len() + topic.len() + metadata.len() + 64;
            ensure!(
                expanded_bytes <= broker_protocol::MAX_FRAME,
                "offset commit expansion limit"
            );
            requests.push(
                json!({"op":"commit", "group":group, "generation":generation,
                "member":member, "topic":topic, "partition":partition,
                "offset":offset, "metadata":metadata}),
            );
            partitions.push(partition);
        }
        topics.push((topic, partitions));
    }
    // A truncated, oversized or trailing-byte request cannot partially commit.
    reader.done()?;
    let result = state
        .db(json!({"op":"commit_many", "requests":requests}))
        .await?;
    let results = result
        .as_array()
        .context("invalid offset commit response")?;
    ensure!(
        results.len() == requests.len(),
        "offset commit response count mismatch"
    );
    let mut values = results.iter();
    let mut response = Writer::default();
    response.array(topics.len());
    for (topic, partitions) in topics {
        response.string(&topic);
        response.array(partitions.len());
        for partition in partitions {
            let value = values.next().expect("response count checked");
            let error = value["error"]
                .as_i64()
                .context("missing offset commit error")?;
            response.i32(partition);
            response.i16(i16::try_from(error)?);
        }
    }
    Ok(response)
}

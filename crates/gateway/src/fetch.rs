use crate::server::{DatabaseResponse, State};
use anyhow::{ensure, Result};
use serde_json::{json, Value};

pub struct Partition {
    pub error: i16,
    pub high: i64,
    pub data: Vec<u8>,
}

fn append(
    partition: &mut Partition,
    offset: i64,
    mut bytes: Vec<u8>,
    total: &mut usize,
) -> Result<()> {
    ensure!(bytes.len() >= 8 && offset >= 0, "invalid stored batch");
    ensure!(
        partition.data.len() + bytes.len() <= 1048576,
        "partition fetch byte limit"
    );
    ensure!(*total + bytes.len() <= 4194304, "fetch byte limit");
    bytes[..8].copy_from_slice(&offset.to_be_bytes());
    *total += bytes.len();
    partition.data.extend(bytes);
    Ok(())
}

impl State {
    pub async fn fetch_partitions(&self, requests: &[Value], max: i32) -> Result<Vec<Partition>> {
        let mut partitions: Vec<Partition> = Vec::with_capacity(requests.len());
        let mut total = 0;
        if self.binary_fetch {
            let DatabaseResponse::Rows(rows) = self
                .execute(json!({
                    "op":"fetch_binary", "requests":requests, "max":max
                }))
                .await?
            else {
                anyhow::bail!("expected binary fetch rows");
            };
            for row in rows {
                let index: i32 = row.try_get(0)?;
                let error: i32 = row.try_get(1)?;
                let high: i64 = row.try_get(2)?;
                let offset: Option<i64> = row.try_get(4)?;
                let data: Option<Vec<u8>> = row.try_get(5)?;
                ensure!(
                    index >= 0 && (index as usize) < requests.len(),
                    "invalid fetch index"
                );
                if offset.is_none() && data.is_none() {
                    ensure!(index as usize == partitions.len(), "fetch status order");
                    partitions.push(Partition {
                        error: i16::try_from(error)?,
                        high,
                        data: Vec::new(),
                    });
                } else {
                    ensure!(index as usize + 1 == partitions.len(), "fetch batch order");
                    let partition = partitions.last_mut().expect("index checked");
                    ensure!(
                        error == 0 && partition.error == 0 && partition.high == high,
                        "fetch batch metadata"
                    );
                    let (Some(offset), Some(data)) = (offset, data) else {
                        anyhow::bail!("incomplete batch");
                    };
                    append(partition, offset, data, &mut total)?;
                }
            }
        } else {
            let fetched = self
                .db(json!({"op":"fetch_many", "requests":requests, "max":max}))
                .await?;
            let rows = fetched
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("invalid fetch results"))?;
            for row in rows {
                let mut partition = Partition {
                    error: row["error"].as_i64().unwrap_or(0) as i16,
                    high: row["high"].as_i64().unwrap_or(0),
                    data: Vec::new(),
                };
                if let Some(batches) = row["batches"].as_array() {
                    for batch in batches {
                        append(
                            &mut partition,
                            batch["offset"].as_i64().unwrap_or(0),
                            hex::decode(batch["data"].as_str().unwrap_or(""))?,
                            &mut total,
                        )?;
                    }
                }
                partitions.push(partition);
            }
        }
        ensure!(
            partitions.len() == requests.len(),
            "fetch result count mismatch"
        );
        Ok(partitions)
    }
}

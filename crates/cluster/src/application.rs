//! Experimental application of committed entries to an existing broker partition.
//! No state-machine trait/snapshot or network consensus is supplied yet.
use crate::votes::operation;
use anyhow::{ensure, Result};
use deadpool_postgres::Pool;
use openraft::{BasicNode, LogId, StoredMembership};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Validated command for the partition journal. Construction and deserialization
/// validate record bytes and numeric bounds before a caller can propose it.
/// 500 KiB leaves room for hex encoding and the log-ID envelope within the
/// journal's 1 MiB per-entry JSON limit. SQL still validates during application.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "CommandWire", into = "CommandWire")]
pub struct PartitionCommand(CommandWire);

const MAX_RECORD_SET: usize = 500 * 1024;
const MAX_APPEND_TIME_MS: u64 = 253_402_300_799_999;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum CommandWire {
    Append {
        expected_offset: u64,
        appended_at_ms: u64,
        data: String,
    },
    Retain {
        expected_earliest: u64,
        new_earliest: u64,
    },
}

fn validate_retention(expected: u64, next: u64) -> Result<()> {
    ensure!(
        expected <= next && next <= i64::MAX as u64,
        "invalid retention offsets"
    );
    Ok(())
}

fn validate(expected_offset: u64, appended_at_ms: u64, bytes: &[u8]) -> Result<()> {
    ensure!(
        bytes.len() <= MAX_RECORD_SET,
        "replica record-set size limit"
    );
    ensure!(
        appended_at_ms <= MAX_APPEND_TIME_MS,
        "replica append time limit"
    );
    let batches = broker_protocol::batches(bytes)?;
    let count: u64 = batches
        .iter()
        .map(|(_, timestamps)| timestamps.len() as u64)
        .sum();
    ensure!(
        expected_offset
            .checked_add(count)
            .is_some_and(|end| end <= i64::MAX as u64),
        "replica offset overflow"
    );
    Ok(())
}

impl PartitionCommand {
    /// Propose an explicit retained prefix boundary, never a replica-local clock.
    /// SQL checks the current frontier, batch boundary and 4096-batch work bound.
    pub fn retain(expected_earliest: u64, new_earliest: u64) -> Result<Self> {
        validate_retention(expected_earliest, new_earliest)?;
        Ok(Self(CommandWire::Retain {
            expected_earliest,
            new_earliest,
        }))
    }
    pub fn append(expected_offset: u64, appended_at_ms: u64, bytes: &[u8]) -> Result<Self> {
        validate(expected_offset, appended_at_ms, bytes)?;
        Ok(Self(CommandWire::Append {
            expected_offset,
            appended_at_ms,
            data: hex::encode(bytes),
        }))
    }
}
impl TryFrom<CommandWire> for PartitionCommand {
    type Error = anyhow::Error;
    fn try_from(wire: CommandWire) -> Result<Self> {
        match &wire {
            CommandWire::Append {
                expected_offset,
                appended_at_ms,
                data,
            } => {
                ensure!(
                    data.len() <= MAX_RECORD_SET * 2,
                    "replica encoded record-set size limit"
                );
                let bytes = hex::decode(data)?;
                validate(*expected_offset, *appended_at_ms, &bytes)?;
            }
            CommandWire::Retain {
                expected_earliest,
                new_earliest,
            } => {
                validate_retention(*expected_earliest, *new_earliest)?;
            }
        }
        Ok(Self(wire))
    }
}
impl From<PartitionCommand> for CommandWire {
    fn from(command: PartitionCommand) -> Self {
        command.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendResult {
    pub error: i16,
    /// Append base offset, or the new earliest offset for a retention command.
    pub offset: i64,
}

#[derive(Debug, Deserialize)]
pub struct AppliedPartition {
    pub group_key: String,
    pub topic: String,
    pub topic_id: String,
    pub partition: i32,
    pub applied: Option<LogId<u64>>,
    pub membership: StoredMembership<u64, BasicNode>,
    pub result: Option<AppendResult>,
}

/// Shares the log owner's durable epoch. Creating a handle does not claim a
/// new owner; a subsequent claim fences both application and journal handles.
pub struct PartitionApplication {
    pool: Pool,
    key: String,
    epoch: i64,
}
impl PartitionApplication {
    pub(crate) async fn snapshot_page(&self, id: &str, start: i64) -> Result<serde_json::Value> {
        operation(
            &self.pool,
            "SELECT public.broker_raft_snapshot_read($1,$2,$3::text::uuid,$4)",
            &[&self.key, &self.epoch, &id, &start],
        )
        .await
    }
    pub(crate) async fn snapshot_operation(
        &mut self,
        op: &str,
        id: Option<&str>,
    ) -> Result<serde_json::Value> {
        if op == "begin" {
            return operation(
                &self.pool,
                "SELECT public.broker_raft_snapshot_begin($1,$2)",
                &[&self.key, &self.epoch],
            )
            .await;
        }
        let query = match op {
            "copy" => "SELECT public.broker_raft_snapshot_copy($1,$2,$3::text::uuid)",
            "release" => "SELECT public.broker_raft_snapshot_release($1,$2,$3::text::uuid)",
            _ => anyhow::bail!("invalid snapshot operation"),
        };
        let id = id.ok_or_else(|| anyhow::anyhow!("snapshot ID required"))?;
        operation(&self.pool, query, &[&self.key, &self.epoch, &id]).await
    }
    pub(crate) fn new(pool: Pool, key: String, epoch: i64) -> Self {
        Self { pool, key, epoch }
    }

    /// Bind one empty existing partition, or retrieve the same immutable binding.
    /// Topic UUID provisioning across independent nodes belongs to the control plane.
    pub async fn bind(
        &mut self,
        topic: &str,
        topic_id: &str,
        partition: i32,
    ) -> Result<AppliedPartition> {
        let result = operation(
            &self.pool,
            "SELECT public.broker_raft_partition_bind($1,$2,$3,$4::text::uuid,$5)",
            &[&self.key, &self.epoch, &topic, &topic_id, &partition],
        )
        .await?;
        Ok(serde_json::from_value(result)?)
    }

    /// Apply exactly the next locally committed journal entry. Its bytes are
    /// read from the journal, never substituted by this caller. The record data,
    /// membership and applied marker commit atomically, with synchronous commit.
    /// Repeating the last confirmed ID returns its saved response without append.
    pub async fn apply(&mut self, log_id: LogId<u64>) -> Result<AppliedPartition> {
        let result = operation(
            &self.pool,
            "SELECT public.broker_raft_partition_apply($1,$2,$3)",
            &[&self.key, &self.epoch, &json!(log_id)],
        )
        .await?;
        Ok(serde_json::from_value(result)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retention_command_round_trip_and_admission_bounds() {
        let command = PartitionCommand::retain(3, 7).unwrap();
        let value = serde_json::to_value(&command).unwrap();
        assert_eq!(
            value,
            json!({"op":"retain","expected_earliest":3,"new_earliest":7})
        );
        assert_eq!(
            serde_json::from_value::<PartitionCommand>(value).unwrap(),
            command
        );
        assert!(PartitionCommand::retain(7, 3).is_err());
        assert!(PartitionCommand::retain(0, i64::MAX as u64 + 1).is_err());
        assert!(PartitionCommand::retain(7, 7).is_ok());
        for value in [
            json!({"op":"retain","expected_earliest":-1,"new_earliest":7}),
            json!({"op":"retain","expected_earliest":3,"new_earliest":2}),
            json!({"op":"retain","expected_earliest":3,"new_earliest":7,"data":""}),
            json!({"op":"retain","expected_earliest":3,"new_earliest":7.5}),
        ] {
            assert!(serde_json::from_value::<PartitionCommand>(value).is_err());
        }
    }
    fn batch() -> Vec<u8> {
        hex::decode(include_str!("../tests/fixtures/append-batch.hex").trim()).unwrap()
    }
    #[test]
    fn validated_command_round_trips_and_preserves_wire_bytes() {
        let bytes = batch();
        let command = PartitionCommand::append(7, 1700000000123, &bytes).unwrap();
        let encoded = serde_json::to_value(&command).unwrap();
        assert_eq!(
            encoded,
            json!({"op":"append","expected_offset":7,
            "appended_at_ms":1700000000123u64,"data":hex::encode(bytes)})
        );
        assert_eq!(
            serde_json::from_value::<PartitionCommand>(encoded).unwrap(),
            command
        );
    }
    #[test]
    fn invalid_commands_are_rejected_before_proposal_or_decode_acceptance() {
        let valid = batch();
        let mut bad = valid.clone();
        *bad.last_mut().unwrap() ^= 1;
        for bytes in [Vec::new(), bad, vec![0; MAX_RECORD_SET + 1]] {
            assert!(PartitionCommand::append(0, 0, &bytes).is_err());
        }
        assert!(PartitionCommand::append(i64::MAX as u64, 0, &valid).is_err());
        assert!(PartitionCommand::append(0, MAX_APPEND_TIME_MS + 1, &valid).is_err());
        assert!(PartitionCommand::append(i64::MAX as u64 - 1, MAX_APPEND_TIME_MS, &valid).is_ok());
        let base =
            json!({"op":"append","expected_offset":0,"appended_at_ms":0,"data":hex::encode(valid)});
        for (key, value) in [
            ("expected_offset", json!(u64::MAX)),
            ("expected_offset", json!(-1)),
            ("expected_offset", json!(0.5)),
            ("appended_at_ms", json!(u64::MAX)),
            ("data", json!("00")),
            ("data", json!("zz")),
            ("unexpected", json!(true)),
        ] {
            let mut wire = base.clone();
            wire[key] = value;
            assert!(
                serde_json::from_value::<PartitionCommand>(wire).is_err(),
                "{key}"
            );
        }
    }
}

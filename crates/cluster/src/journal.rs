//! Typed local journal on opt-in SQL 0.1.10; the OpenRaft adapter in
//! `storage` additionally requires SQL 0.1.11 for readers. State-machine
//! application and snapshot installation are not supplied by this layer.
use crate::votes::VoteStorage;
use anyhow::{ensure, Result};
use deadpool_postgres::Pool;
use openraft::{LogId, RaftTypeConfig, Vote};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    io::{self, Write},
    marker::PhantomData,
};

const MAX_JSON: usize = 8 * 1024 * 1024;
const MAX_ENTRIES: usize = 256;

struct BoundedJson(Vec<u8>);
impl Write for BoundedJson {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.len() > MAX_JSON - self.0.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "journal JSON limit",
            ));
        }
        self.0.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Value> {
    let mut bytes = BoundedJson(Vec::new());
    serde_json::to_writer(&mut bytes, value)?;
    Ok(serde_json::from_slice(&bytes.0)?)
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct JournalState {
    pub last: Option<LogId<u64>>,
    pub purged: Option<LogId<u64>>,
    pub committed: Option<LogId<u64>>,
}

/// One non-cloneable local writer. All vote and journal operations require
/// exclusive mutable access and share the same durable writer epoch.
/// The generic entry type allows future deterministic application commands;
/// this layer does not interpret or apply them.
pub struct JournalStorage<C: RaftTypeConfig<NodeId = u64>> {
    owner: VoteStorage,
    config: PhantomData<C>,
}

impl<C> JournalStorage<C>
where
    C: RaftTypeConfig<NodeId = u64>,
    C::Entry: Serialize + DeserializeOwned,
{
    /// Requires opt-in SQL 0.1.12. Shares this local owner's epoch without
    /// claiming a second writer; not a RaftStateMachine implementation yet.
    pub fn application(&self) -> crate::application::PartitionApplication {
        self.owner.application()
    }
    /// Requires opt-in SQL 0.1.11 when used; does not claim a new writer epoch.
    pub fn reader(&self) -> crate::reader::JournalReader<C> {
        crate::reader::JournalReader::new(self.owner.reader())
    }
    pub async fn claim(pool: Pool, key: String) -> Result<Self> {
        let mut store = Self {
            owner: VoteStorage::claim(pool, key).await?,
            config: PhantomData,
        };
        store.state().await?; // Fail immediately if journal SQL is unavailable.
        Ok(store)
    }

    async fn change(&mut self, op: &str, value: Value) -> Result<JournalState> {
        Ok(serde_json::from_value(
            self.owner.journal(op, value).await?,
        )?)
    }

    pub async fn state(&mut self) -> Result<JournalState> {
        self.change("state", json!({})).await
    }

    /// Success follows PostgreSQL COMMIT confirmation, never an in-memory enqueue.
    pub async fn append(&mut self, entries: &[C::Entry]) -> Result<JournalState> {
        ensure!(entries.len() <= MAX_ENTRIES, "journal entry count limit");
        self.change("append", encode(entries)?).await
    }

    /// Inclusive range, bounded by both count and SQL's 8 MiB JSON response budget.
    /// Continue at the index following the last returned entry for another page.
    pub async fn read(&mut self, start: u64, end: u64, limit: usize) -> Result<Vec<C::Entry>> {
        ensure!(
            start <= end && (1..=MAX_ENTRIES).contains(&limit),
            "invalid journal read bounds"
        );
        let value = self
            .owner
            .journal(
                "read",
                json!({
                    "start":{"index":start}, "end":{"index":end}, "limit":limit
                }),
            )
            .await?;
        Ok(serde_json::from_value(value)?)
    }

    pub async fn truncate(&mut self, from: LogId<u64>) -> Result<JournalState> {
        self.change("truncate", encode(&from)?).await
    }

    pub async fn commit(&mut self, through: LogId<u64>) -> Result<JournalState> {
        self.change("commit", encode(&through)?).await
    }

    /// Caller must establish a durable applied state/snapshot first. The SQL
    /// committed boundary alone is not proof that state can be reconstructed.
    pub async fn purge(&mut self, through: LogId<u64>) -> Result<JournalState> {
        self.change("purge", encode(&through)?).await
    }

    pub async fn save_vote(&mut self, vote: Vote<u64>) -> Result<()> {
        self.owner.save_vote(vote).await
    }

    pub async fn read_vote(&mut self) -> Result<Option<Vote<u64>>> {
        self.owner.read_vote().await
    }
}

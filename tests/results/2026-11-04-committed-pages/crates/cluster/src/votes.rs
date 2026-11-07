//! Typed OpenRaft vote persistence on opt-in extension SQL 0.1.9.
//!
//! A claim fences older local storage owners without erasing the durable vote.
//! Only this adapter validates OpenRaft vote ordering; the SQL entry points must
//! be granted exclusively to trusted storage code. This is not a log-store or
//! state-machine implementation and is not wired into the Kafka gateway.
use anyhow::{ensure, Context, Result};
use deadpool_postgres::{Object, Pool};
use openraft::Vote;
use serde_json::Value;
use std::{cmp::Ordering, time::Duration};
use tokio::time::timeout;
use tokio_postgres::types::ToSql;

struct Unconfirmed(Option<Object>);
impl Drop for Unconfirmed {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            // Cancellation or an ambiguous commit must never recycle a connection.
            drop(Object::take(client));
        }
    }
}

pub(crate) async fn operation(
    pool: &Pool,
    sql: &str,
    args: &[&(dyn ToSql + Sync)],
) -> Result<Value> {
    let mut held = Unconfirmed(Some(timeout(Duration::from_secs(3), pool.get()).await??));
    let value = timeout(Duration::from_secs(10), async {
        let tx = held.0.as_mut().expect("held connection").transaction().await?;
        tx.batch_execute("SET LOCAL synchronous_commit=on; SET LOCAL statement_timeout='3s'; SET LOCAL lock_timeout='2s'").await?;
        let durable: bool = tx.query_one("SELECT NOT pg_is_in_recovery() AND current_setting('fsync')='on' AND current_setting('full_page_writes')='on'", &[]).await?.get(0);
        ensure!(durable, "Raft storage requires a writable PostgreSQL node with fsync and full_page_writes enabled");
        let value: Value = tx.query_one(sql, args).await?.try_get(0)?;
        // A statement timeout during SyncRep can emit a warning after local
        // commit. Disable it before COMMIT and bound confirmation at this layer.
        tx.batch_execute("SET LOCAL statement_timeout=0").await?;
        timeout(Duration::from_secs(5), tx.commit())
            .await.context("Raft commit confirmation timed out; outcome unknown")??;
        Ok::<_, anyhow::Error>(value)
    }).await.context("Raft storage operation timed out; outcome unknown")??;
    drop(held.0.take()); // Only confirmed transactions return to the bounded pool.
    Ok(value)
}

/// One local storage owner for a stable replica-group key.
///
/// Mutating methods require exclusive access so vote IO cannot be reordered.
/// Claiming again deliberately fences the previous owner; it is not a lease or
/// a distributed election. Do not use two live Raft instances for one identity.
pub struct VoteStorage {
    pool: Pool,
    key: String,
    epoch: i64,
    revision: i64,
    vote: Option<Vote<u64>>,
}

/// Read-only copy of the current owner identity, never a new writer claim.
#[derive(Clone)]
pub(crate) struct JournalReadOwner {
    pool: Pool,
    key: String,
    epoch: i64,
}
impl JournalReadOwner {
    pub(crate) async fn committed_page(&self, bounds: Value) -> Result<Value> {
        operation(
            &self.pool,
            "SELECT public.broker_raft_committed_page($1,$2,$3)",
            &[&self.key, &self.epoch, &bounds],
        )
        .await
    }

    pub(crate) async fn read(&self, bounds: Value, complete: bool) -> Result<Value> {
        operation(
            &self.pool,
            "SELECT public.broker_raft_log_read($1,$2,$3,$4)",
            &[&self.key, &self.epoch, &bounds, &complete],
        )
        .await
    }
}

impl VoteStorage {
    pub(crate) fn application(&self) -> crate::application::PartitionApplication {
        crate::application::PartitionApplication::new(
            self.pool.clone(),
            self.key.clone(),
            self.epoch,
        )
    }
    pub(crate) fn reader(&self) -> JournalReadOwner {
        JournalReadOwner {
            pool: self.pool.clone(),
            key: self.key.clone(),
            epoch: self.epoch,
        }
    }
    /// Share the fenced owner and confirmed-transaction boundary with log IO.
    pub(crate) async fn journal(&mut self, operation_name: &str, value: Value) -> Result<Value> {
        operation(
            &self.pool,
            "SELECT public.broker_raft_log($1,$2,$3,$4)",
            &[&self.key, &self.epoch, &operation_name, &value],
        )
        .await
    }

    pub async fn claim(pool: Pool, key: String) -> Result<Self> {
        ensure!(
            !key.is_empty() && key.len() <= 249,
            "invalid replica-group key"
        );
        ensure!(
            pool.status().max_size <= 16,
            "vote pool must be bounded to at most 16 connections"
        );
        let state = operation(&pool, "SELECT public.broker_raft_vote_claim($1)", &[&key]).await?;
        let mut store = Self {
            pool,
            key,
            epoch: 0,
            revision: 0,
            vote: None,
        };
        store.load(state)?;
        Ok(store)
    }

    fn load(&mut self, state: Value) -> Result<()> {
        let epoch = state["epoch"].as_i64().context("missing writer epoch")?;
        let revision = state["revision"]
            .as_i64()
            .context("missing vote revision")?;
        ensure!(epoch > 0 && revision >= 0, "invalid vote storage state");
        ensure!(
            self.epoch == 0 || epoch == self.epoch,
            "writer epoch changed"
        );
        let vote = serde_json::from_value(state["vote"].clone())?;
        self.epoch = epoch;
        self.revision = revision;
        self.vote = vote;
        Ok(())
    }

    pub async fn read_vote(&mut self) -> Result<Option<Vote<u64>>> {
        let state = operation(
            &self.pool,
            "SELECT public.broker_raft_vote_read($1,$2)",
            &[&self.key, &self.epoch],
        )
        .await?;
        self.load(state)?;
        Ok(self.vote)
    }

    pub async fn save_vote(&mut self, vote: Vote<u64>) -> Result<()> {
        if let Some(previous) = self.vote {
            ensure!(
                matches!(
                    vote.partial_cmp(&previous),
                    Some(Ordering::Equal | Ordering::Greater)
                ),
                "Raft vote must not regress or become incomparable"
            );
        }
        let value = serde_json::to_value(vote)?;
        // Even an identical vote checks the epoch and revision at the durable
        // boundary, so an old owner cannot report success from a stale cache.
        let state = operation(
            &self.pool,
            "SELECT public.broker_raft_vote_cas($1,$2,$3,$4)",
            &[&self.key, &self.epoch, &self.revision, &value],
        )
        .await?;
        self.load(state)
    }
}

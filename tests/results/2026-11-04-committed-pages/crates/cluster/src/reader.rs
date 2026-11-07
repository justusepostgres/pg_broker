//! Bounded OpenRaft log readers on opt-in SQL 0.1.11. No writer/replication engine.
use crate::votes::JournalReadOwner;
use openraft::{AnyError, RaftLogReader, RaftTypeConfig, StorageError, StorageIOError};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

#[derive(Clone)]
pub struct JournalReader<C: RaftTypeConfig<NodeId = u64>> {
    owner: JournalReadOwner,
    config: PhantomData<C>,
}

fn bounds<R: RangeBounds<u64>>(range: &R) -> Value {
    let start = match range.start_bound() {
        Bound::Unbounded => Some(0),
        Bound::Included(n) => Some(*n),
        Bound::Excluded(n) => n.checked_add(1),
    };
    let end = match range.end_bound() {
        Bound::Unbounded => Some(u64::MAX),
        Bound::Included(n) => Some(*n),
        Bound::Excluded(n) => n.checked_sub(1),
    };
    match (start, end) {
        (Some(start), Some(end)) if start <= end => {
            json!({"start":{"index":start},"end":{"index":end}})
        }
        _ => json!({"empty":true}),
    }
}

fn storage_error(error: impl std::fmt::Display) -> StorageError<u64> {
    StorageIOError::read_logs(AnyError::error(error.to_string())).into()
}

impl<C: RaftTypeConfig<NodeId = u64>> JournalReader<C> {
    pub(crate) fn new(owner: JournalReadOwner) -> Self {
        Self {
            owner,
            config: PhantomData,
        }
    }
}

impl<C> RaftLogReader<C> for JournalReader<C>
where
    C: RaftTypeConfig<NodeId = u64>,
    C::Entry: DeserializeOwned,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<u64>> {
        // One transaction/owner lock supplies a consistent range. Oversized
        // complete reads fail rather than splicing multiple changing snapshots.
        let value = self
            .owner
            .read(bounds(&range), true)
            .await
            .map_err(storage_error)?;
        serde_json::from_value(value).map_err(storage_error)
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<C::Entry>, StorageError<u64>> {
        let value = self
            .owner
            .read(bounds(&(start..end)), false)
            .await
            .map_err(storage_error)?;
        let entries: Vec<C::Entry> = serde_json::from_value(value).map_err(storage_error)?;
        if start < end && entries.is_empty() {
            return Err(storage_error(
                "requested replication range is no longer available",
            ));
        }
        Ok(entries)
    }
}

/// Lazy committed-page cursor on opt-in SQL 0.1.19. This is not the OpenRaft
/// 0.9 complete-read API: it is a building block for a future streaming adapter.
/// No transaction or pool slot is held between calls. A cancelled/failed call
/// terminates this cursor; reopen explicitly from the last durable applied ID.
pub struct CommittedPages<C: RaftTypeConfig<NodeId = u64>> {
    owner: JournalReadOwner,
    next: u64,
    end: openraft::LogId<u64>,
    finished: bool,
    config: PhantomData<C>,
}

impl<C> JournalReader<C>
where
    C: RaftTypeConfig<NodeId = u64>,
    C::Entry: DeserializeOwned,
{
    pub fn committed_pages(
        &self,
        start: u64,
        end: openraft::LogId<u64>,
    ) -> anyhow::Result<CommittedPages<C>> {
        anyhow::ensure!(start <= end.index, "invalid committed page interval");
        Ok(CommittedPages {
            owner: self.owner.clone(),
            next: start,
            end,
            finished: false,
            config: PhantomData,
        })
    }
}

impl<C> CommittedPages<C>
where
    C: RaftTypeConfig<NodeId = u64>,
    C::Entry: DeserializeOwned,
{
    /// Returns at most 256 entries / 8 MiB encoded JSON after confirmed COMMIT.
    /// Every page checks the owner epoch, durable committed frontier, and exact
    /// endpoint identity. Purged ranges fail instead of silently skipping data.
    pub async fn next_page(&mut self) -> anyhow::Result<Option<Vec<C::Entry>>> {
        use openraft::RaftLogId;
        if self.finished {
            return Ok(None);
        }
        self.finished = true; // Also terminal if this future is cancelled.
        let value = self
            .owner
            .committed_page(json!({
                "start": {"index":self.next}, "end":self.end
            }))
            .await?;
        let entries: Vec<C::Entry> = serde_json::from_value(value)?;
        anyhow::ensure!(
            !entries.is_empty() && entries.len() <= 256,
            "invalid committed page count"
        );
        let mut expected = Some(self.next);
        for entry in &entries {
            let id = entry.get_log_id();
            anyhow::ensure!(
                Some(id.index) == expected && id.index <= self.end.index,
                "committed page discontinuity"
            );
            if id.index == self.end.index {
                anyhow::ensure!(*id == self.end, "committed endpoint mismatch");
            }
            expected = id.index.checked_add(1);
        }
        if let Some(next) = expected.filter(|n| *n <= self.end.index) {
            self.next = next;
            self.finished = false;
        }
        Ok(Some(entries))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn range_bounds_never_wrap_at_u64_edges() {
        assert_eq!(
            bounds(&(..)),
            json!({"start":{"index":0},"end":{"index":u64::MAX}})
        );
        assert_eq!(bounds(&(..0)), json!({"empty":true}));
        assert_eq!(
            bounds(&(Bound::Excluded(u64::MAX), Bound::Unbounded)),
            json!({"empty":true})
        );
        assert_eq!(
            bounds(&(u64::MAX..=u64::MAX)),
            json!({"start":{"index":u64::MAX},"end":{"index":u64::MAX}})
        );
        assert_eq!(bounds(&(5..5)), json!({"empty":true}));
    }
}

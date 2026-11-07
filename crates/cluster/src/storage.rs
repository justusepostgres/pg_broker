//! OpenRaft log-storage adapter. PostgreSQL commit confirmation is the flush
//! boundary. This deliberately awaits each write: no detached writes may
//! overtake a vote, truncation, or owner replacement.
use crate::{journal::JournalStorage, reader::JournalReader};
use openraft::{
    storage::{LogFlushed, RaftLogStorage},
    AnyError, LogId, LogState, RaftLogReader, RaftTypeConfig, StorageError, StorageIOError, Vote,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, ops::RangeBounds};

fn cause(error: anyhow::Error) -> AnyError {
    // Preserve database causes, including fencing/constraint details.
    AnyError::error(format!("{error:#}"))
}
fn read_error(error: anyhow::Error) -> StorageError<u64> {
    StorageIOError::read_logs(cause(error)).into()
}
fn write_error(error: anyhow::Error) -> StorageError<u64> {
    StorageIOError::write_logs(cause(error)).into()
}

impl<C> RaftLogReader<C> for JournalStorage<C>
where
    C: RaftTypeConfig<NodeId = u64>,
    C::Entry: Serialize + DeserializeOwned,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<u64>> {
        self.reader().try_get_log_entries(range).await
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<C::Entry>, StorageError<u64>> {
        self.reader().limited_get_log_entries(start, end).await
    }
}

impl<C> RaftLogStorage<C> for JournalStorage<C>
where
    C: RaftTypeConfig<NodeId = u64>,
    C::Entry: Serialize + DeserializeOwned,
{
    type LogReader = JournalReader<C>;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<u64>> {
        let state = self.state().await.map_err(read_error)?;
        Ok(LogState {
            last_purged_log_id: state.purged,
            last_log_id: state.last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.reader()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        JournalStorage::save_vote(self, *vote)
            .await
            .map_err(|e| StorageIOError::write_vote(cause(e)).into())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        JournalStorage::read_vote(self)
            .await
            .map_err(|e| StorageIOError::read_vote(cause(e)).into())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        match committed {
            Some(id) => {
                self.commit(id).await.map_err(write_error)?;
            }
            None => {
                // None is valid only for an empty committed prefix. Still
                // check ownership; never silently erase a durable boundary.
                let state = self.state().await.map_err(read_error)?;
                if state.committed.is_some() {
                    return Err(write_error(anyhow::anyhow!(
                        "committed boundary cannot regress to None"
                    )));
                }
            }
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.state().await.map_err(read_error)?.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = C::Entry> + Send,
        I::IntoIter: Send,
    {
        // A finite prefix bounds even an untrusted/unbounded iterator. Do not
        // split one append across transactions: failure must not commit a prefix.
        let entries: Vec<_> = entries.into_iter().take(257).collect();
        let result = JournalStorage::append(self, &entries).await;
        match result {
            Ok(_) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                callback.log_io_completed(Err(std::io::Error::other(format!("{error:#}"))));
                Err(write_error(error))
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        JournalStorage::truncate(self, log_id)
            .await
            .map_err(write_error)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        // OpenRaft must only request this after durable apply/snapshot. The
        // adapter does not supply the missing application/snapshot component.
        JournalStorage::purge(self, log_id)
            .await
            .map_err(write_error)?;
        Ok(())
    }
}

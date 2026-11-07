//! Experimental SQL-backed alpha log adapter for single-node engine tests.
use super::{Next, Old};
use broker_cluster::journal::JournalStorage;
use futures_util::{stream, Stream};
use openraft_next::{
    storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage},
    type_config::alias::{EntryOf, LogIdOf, VoteOf},
};
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::VecDeque, io, ops::RangeBounds, sync::Arc};
use tokio::sync::Mutex;

pub fn error(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}
pub fn convert<A: Serialize, B: DeserializeOwned>(a: A) -> io::Result<B> {
    serde_json::from_value(serde_json::to_value(a).map_err(error)?).map_err(error)
}
#[derive(Clone)]
pub struct Store(pub Arc<Mutex<JournalStorage<Old>>>);
#[derive(Clone)]
pub struct Reader(pub Store);
impl RaftLogReader<Next> for Reader {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> io::Result<Vec<EntryOf<Next>>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    {
        use openraft_old::RaftLogReader;
        let mut reader = self.0 .0.lock().await.reader();
        convert(reader.try_get_log_entries(range).await.map_err(error)?)
    }
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> io::Result<Vec<EntryOf<Next>>> {
        use openraft_old::RaftLogReader;
        let mut reader = self.0 .0.lock().await.reader();
        convert(
            reader
                .limited_get_log_entries(start, end)
                .await
                .map_err(error)?,
        )
    }
    async fn read_vote(&mut self) -> io::Result<Option<VoteOf<Next>>> {
        convert(self.0 .0.lock().await.read_vote().await.map_err(error)?)
    }
    async fn entries_stream<RB>(
        &mut self,
        range: RB,
    ) -> impl Stream<Item = io::Result<EntryOf<Next>>> + Send
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    {
        use openraft_old::RaftLogReader;
        let mut reader = self.0 .0.lock().await.reader();
        let bounds = super::paged_stream::interval(&range);
        let cursor = async move {
            let Some((start, end)) = bounds else {
                return Ok(None);
            };
            let endpoint = reader.try_get_log_entries(end..=end).await.map_err(error)?;
            if endpoint.len() != 1 {
                return Err(error("committed stream endpoint missing"));
            }
            reader
                .committed_pages(start, endpoint[0].log_id)
                .map(Some)
                .map_err(error)
        }
        .await;
        stream::try_unfold(
            (cursor, VecDeque::new()),
            |(cursor, mut buffered)| async move {
                let Some(mut cursor) = cursor? else {
                    return Ok(None);
                };
                if buffered.is_empty() {
                    let Some(page) = cursor.next_page().await.map_err(error)? else {
                        return Ok(None);
                    };
                    for entry in page {
                        buffered.push_back(convert::<_, EntryOf<Next>>(entry)?);
                    }
                }
                let entry = buffered.pop_front().expect("nonempty SQL page checked");
                Ok(Some((entry, (Ok(Some(cursor)), buffered))))
            },
        )
    }
}
impl RaftLogStorage<Next> for Store {
    type LogReader = Reader;
    async fn get_log_reader(&mut self) -> Reader {
        Reader(self.clone())
    }
    async fn get_log_state(&mut self) -> io::Result<LogState<Next>> {
        let state = self.0.lock().await.state().await.map_err(error)?;
        Ok(LogState {
            last_purged_log_id: convert(state.purged)?,
            last_log_id: convert(state.last)?,
        })
    }
    async fn save_vote(&mut self, vote: &VoteOf<Next>) -> io::Result<()> {
        self.0
            .lock()
            .await
            .save_vote(convert(vote)?)
            .await
            .map_err(error)
    }
    async fn save_committed(&mut self, id: Option<LogIdOf<Next>>) -> io::Result<()> {
        let mut store = self.0.lock().await;
        if let Some(id) = id {
            store.commit(convert(id)?).await.map_err(error)?;
        } else if store.state().await.map_err(error)?.committed.is_some() {
            return Err(error("committed boundary cannot regress"));
        }
        Ok(())
    }
    async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<Next>>> {
        convert(self.0.lock().await.state().await.map_err(error)?.committed)
    }
    async fn append<I>(&mut self, entries: I, callback: IOFlushed<Next>) -> io::Result<()>
    where
        I: IntoIterator<Item = EntryOf<Next>> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().take(257).collect();
        let result = async {
            let entries = convert::<_, Vec<openraft_old::Entry<Old>>>(entries)?;
            self.0.lock().await.append(&entries).await.map_err(error)?;
            Ok::<(), io::Error>(())
        }
        .await;
        callback.io_completed(result.as_ref().map(|_| ()).map_err(error));
        result
    }
    async fn truncate_after(&mut self, last: Option<LogIdOf<Next>>) -> io::Result<()> {
        let start = match last {
            None => 0,
            Some(id) => match id.index.checked_add(1) {
                Some(index) => index,
                None => {
                    self.0.lock().await.state().await.map_err(error)?;
                    return Ok(());
                }
            },
        };
        self.0
            .lock()
            .await
            .truncate(openraft_old::LogId::new(
                openraft_old::CommittedLeaderId::new(0, 0),
                start,
            ))
            .await
            .map_err(error)?;
        Ok(())
    }
    async fn purge(&mut self, id: LogIdOf<Next>) -> io::Result<()> {
        self.0
            .lock()
            .await
            .purge(convert(id)?)
            .await
            .map_err(error)?;
        Ok(())
    }
}

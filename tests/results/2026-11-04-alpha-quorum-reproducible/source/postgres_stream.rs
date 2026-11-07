//! End-to-end SQL19 application stream probe, not a complete Raft storage adapter.
use super::{Next, Old};
use broker_cluster::{journal::JournalStorage, reader::JournalReader};
use deadpool_postgres::{Manager, Pool};
use futures_util::{stream, Stream, StreamExt};
use openraft_next::{
    storage::RaftLogReader,
    type_config::alias::{EntryOf, VoteOf},
};
use std::{
    collections::VecDeque,
    io,
    ops::RangeBounds,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Config, NoTls};

struct PgReader {
    reader: JournalReader<Old>,
    endpoint: openraft_old::LogId<u64>,
}

impl RaftLogReader<Next> for PgReader {
    async fn try_get_log_entries<RB>(&mut self, _range: RB) -> io::Result<Vec<EntryOf<Next>>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    {
        Err(io::Error::other(
            "probe supports committed application streams only",
        ))
    }
    async fn read_vote(&mut self) -> io::Result<Option<VoteOf<Next>>> {
        Err(io::Error::other("replication vote adapter not implemented"))
    }
    async fn entries_stream<RB>(
        &mut self,
        range: RB,
    ) -> impl Stream<Item = io::Result<EntryOf<Next>>> + Send
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    {
        // A running state-machine worker supplies a committed endpoint. For this
        // probe it is explicit: never infer a committed boundary from log tail.
        let cursor = match super::paged_stream::interval(&range) {
            None => Ok(None),
            Some((start, end)) if end == self.endpoint.index => self
                .reader
                .committed_pages(start, self.endpoint)
                .map(Some)
                .map_err(|e| io::Error::other(e.to_string())),
            Some(_) => Err(io::Error::other("range differs from committed endpoint")),
        };
        stream::try_unfold(
            (cursor, VecDeque::new()),
            |(cursor, mut buffered)| async move {
                let Some(mut cursor) = cursor? else {
                    return Ok(None);
                };
                if buffered.is_empty() {
                    let Some(page) = cursor
                        .next_page()
                        .await
                        .map_err(|e| io::Error::other(e.to_string()))?
                    else {
                        return Ok(None);
                    };
                    // Convert the actual persisted old-version envelope, one entry
                    // at a time. Production migration must remove this probe bridge.
                    for entry in page {
                        let value = serde_json::to_value(entry).map_err(io::Error::other)?;
                        buffered.push_back(
                            serde_json::from_value::<EntryOf<Next>>(value)
                                .map_err(io::Error::other)?,
                        );
                    }
                }
                let entry = buffered.pop_front().expect("nonempty SQL page checked");
                Ok(Some((entry, (Ok(Some(cursor)), buffered))))
            },
        )
    }
}

fn pool() -> Pool {
    let config: Config = std::env::var("DATABASE_URL").unwrap().parse().unwrap();
    Pool::builder(Manager::new(config, NoTls))
        .max_size(1)
        .build()
        .unwrap()
}
fn key() -> String {
    format!(
        "alpha-stream-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
fn id(index: u64) -> openraft_old::LogId<u64> {
    openraft_old::LogId::new(openraft_old::CommittedLeaderId::new(1, 1), index)
}
async fn populated(pool: Pool, key: String) -> JournalStorage<Old> {
    let mut store = JournalStorage::<Old>::claim(pool, key).await.unwrap();
    for start in (0..10001).step_by(256) {
        let entries: Vec<_> = (start..(start + 256).min(10001))
            .map(|index| openraft_old::Entry {
                log_id: id(index),
                payload: openraft_old::EntryPayload::Normal(super::Data(
                    serde_json::json!({"index":index}),
                )),
            })
            .collect();
        store.append(&entries).await.unwrap();
    }
    store.commit(id(10000)).await.unwrap();
    store
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL SQL0.1.19"]
async fn postgres_stream_reads_large_committed_jump_with_real_payloads() {
    let mut store = populated(pool(), key()).await;
    let mut reader = PgReader {
        reader: store.reader(),
        endpoint: id(10000),
    };
    let stream = reader.entries_stream(0..10001).await;
    futures_util::pin_mut!(stream);
    let mut next = 0;
    while let Some(entry) = stream.next().await {
        let value = serde_json::to_value(entry.unwrap()).unwrap();
        assert_eq!(value["log_id"]["index"], next);
        assert_eq!(value["payload"]["Normal"]["index"], next);
        next += 1;
        if next % 256 == 0 {
            assert_eq!(store.state().await.unwrap().committed, Some(id(10000)));
        }
    }
    assert_eq!(next, 10001);
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL SQL0.1.19"]
async fn postgres_stream_terminates_on_owner_replacement_between_pages() {
    let pool = pool();
    let key = key();
    let store = populated(pool.clone(), key.clone()).await;
    let mut reader = PgReader {
        reader: store.reader(),
        endpoint: id(10000),
    };
    let stream = reader.entries_stream(0..10001).await;
    futures_util::pin_mut!(stream);
    for n in 0..256 {
        assert_eq!(stream.next().await.unwrap().unwrap().log_id.index, n);
    }
    let _new_owner = JournalStorage::<Old>::claim(pool, key).await.unwrap();
    assert!(stream.next().await.unwrap().is_err());
    assert!(stream.next().await.is_none());
}

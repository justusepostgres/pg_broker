//! Exercises upstream defaults, not a PostgreSQL adapter or consensus node.
use super::Next;
use futures_util::StreamExt;
use openraft_next::storage::RaftLogReader;
use openraft_next::type_config::alias::{EntryOf, VoteOf};
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex};

struct CappedReader {
    requested: Arc<Mutex<Vec<(u64, u64)>>>,
}

impl RaftLogReader<Next> for CappedReader {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<Next>>, std::io::Error>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    {
        let (Bound::Included(start), Bound::Excluded(end)) =
            (range.start_bound(), range.end_bound())
        else {
            panic!("probe expects a bounded half-open range")
        };
        self.requested.lock().unwrap().push((*start, *end));
        if end - start > 4096 {
            return Err(std::io::Error::other("complete read exceeds entry cap"));
        }
        Ok((*start..*end)
            .map(|index| {
                serde_json::from_value(serde_json::json!({
                    "log_id": {"leader_id": {"term": 1, "node_id": 1}, "index": index},
                    "payload": "Blank"
                }))
                .unwrap()
            })
            .collect())
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<Next>>, std::io::Error> {
        Ok(None)
    }
}

#[tokio::test]
async fn default_stream_does_not_chunk_a_large_committed_range() {
    let requested = Arc::new(Mutex::new(Vec::new()));
    let mut reader = CappedReader {
        requested: requested.clone(),
    };
    let stream = reader.entries_stream(0..4097).await;
    futures_util::pin_mut!(stream);
    assert_eq!(
        stream.next().await.unwrap().unwrap_err().to_string(),
        "complete read exceeds entry cap"
    );
    assert!(stream.next().await.is_none());
    assert_eq!(*requested.lock().unwrap(), vec![(0, 4097)]);
}

#[tokio::test]
async fn default_stream_fetches_the_whole_range_before_first_poll() {
    let requested = Arc::new(Mutex::new(Vec::new()));
    let mut reader = CappedReader {
        requested: requested.clone(),
    };
    let stream = reader.entries_stream(0..4096).await;
    assert_eq!(*requested.lock().unwrap(), vec![(0, 4096)]);
    futures_util::pin_mut!(stream);
    let mut count = 0;
    while let Some(entry) = stream.next().await {
        entry.unwrap();
        count += 1;
    }
    assert_eq!(count, 4096);
}

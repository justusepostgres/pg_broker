//! Isolated application-stream prototype; not a PostgreSQL or replication adapter.
use super::Next;
use futures_util::{stream, Stream, StreamExt};
use openraft_next::storage::RaftLogReader;
use openraft_next::type_config::alias::{EntryOf, VoteOf};
use std::{
    collections::VecDeque,
    io,
    ops::{Bound, RangeBounds},
    sync::{Arc, Mutex},
};

const PAGE_COUNT: usize = 128;
const PAGE_BYTES: usize = 1024 * 1024;

#[derive(Default)]
struct Observations {
    pages: Vec<(u64, usize, usize)>,
    fail_at: Option<u64>,
    gap_at: Option<u64>,
    stall_at: Option<u64>,
    attempts: usize,
}

struct PagedReader {
    observations: Arc<Mutex<Observations>>,
    payload_bytes: usize,
}

fn interval(range: &impl RangeBounds<u64>) -> Option<(u64, u64)> {
    let start = match range.start_bound() {
        Bound::Unbounded => 0,
        Bound::Included(n) => *n,
        Bound::Excluded(n) => n.checked_add(1)?,
    };
    let end = match range.end_bound() {
        Bound::Unbounded => u64::MAX,
        Bound::Included(n) => *n,
        Bound::Excluded(n) => n.checked_sub(1)?,
    };
    (start <= end).then_some((start, end))
}

impl PagedReader {
    // Simulates a bounded storage page. A production backend must enforce byte
    // and row limits before constructing its response, in a short transaction.
    async fn page(&mut self, start: u64, end: u64) -> io::Result<Vec<EntryOf<Next>>> {
        let stall = {
            let mut observed = self.observations.lock().unwrap();
            observed.attempts += 1;
            observed.stall_at.is_some_and(|n| start >= n)
        };
        if stall {
            std::future::pending::<()>().await;
        }
        if self.payload_bytes > PAGE_BYTES {
            return Err(io::Error::other("entry exceeds page budget"));
        }
        let mut observed = self.observations.lock().unwrap();
        if observed.fail_at.is_some_and(|n| start >= n) {
            return Err(io::Error::other("owner changed or page unavailable"));
        }
        let mut bytes = 0;
        let mut entries = Vec::new();
        for index in start..=end {
            if entries.len() == PAGE_COUNT {
                break;
            }
            let actual = if observed.gap_at == Some(index) {
                index
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("index overflow"))?
            } else {
                index
            };
            let entry: EntryOf<Next> = serde_json::from_value(serde_json::json!({
                "log_id": {"leader_id": {"term": 1, "node_id": 1}, "index": actual},
                "payload": {"Normal": "x".repeat(self.payload_bytes)}
            }))
            .unwrap();
            let size = serde_json::to_vec(&entry).unwrap().len();
            if size > PAGE_BYTES {
                return Err(io::Error::other("entry exceeds page budget"));
            }
            if bytes + size > PAGE_BYTES {
                break;
            }
            bytes += size;
            entries.push(entry);
        }
        observed.pages.push((start, entries.len(), bytes));
        Ok(entries)
    }
}

impl RaftLogReader<Next> for PagedReader {
    async fn try_get_log_entries<RB>(&mut self, _range: RB) -> io::Result<Vec<EntryOf<Next>>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    {
        Err(io::Error::other(
            "complete read deliberately unavailable in stream probe",
        ))
    }
    async fn read_vote(&mut self) -> io::Result<Option<VoteOf<Next>>> {
        Ok(None)
    }

    async fn entries_stream<RB>(
        &mut self,
        range: RB,
    ) -> impl Stream<Item = io::Result<EntryOf<Next>>> + Send
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    {
        // No page is fetched until polled. The only resident queue is one page.
        stream::try_unfold(
            (self, interval(&range), VecDeque::new()),
            |(reader, remaining, mut buffered)| async move {
                let Some((cursor, end)) = remaining else {
                    return Ok(None);
                };
                if buffered.is_empty() {
                    let page = reader.page(cursor, end).await?;
                    if page.is_empty() || page.len() > PAGE_COUNT {
                        return Err(io::Error::other("empty or oversized storage page"));
                    }
                    // Validate the whole page before exposing even its prefix.
                    let mut expected = Some(cursor);
                    for entry in &page {
                        if Some(entry.log_id.index) != expected || entry.log_id.index > end {
                            return Err(io::Error::other("nonconsecutive storage page"));
                        }
                        expected = entry.log_id.index.checked_add(1);
                    }
                    buffered = page.into();
                }
                let entry = buffered.pop_front().expect("nonempty page checked");
                let remaining = if cursor == end {
                    None
                } else {
                    Some((cursor + 1, end))
                };
                Ok(Some((entry, (reader, remaining, buffered))))
            },
        )
    }
}

fn reader(payload_bytes: usize) -> (PagedReader, Arc<Mutex<Observations>>) {
    let observations = Arc::new(Mutex::new(Observations::default()));
    (
        PagedReader {
            observations: observations.clone(),
            payload_bytes,
        },
        observations,
    )
}

#[tokio::test]
async fn committed_jump_streams_over_4096_entries_with_bounded_pages() {
    let (mut reader, observed) = reader(0);
    let stream = reader.entries_stream(0..10001).await;
    assert!(observed.lock().unwrap().pages.is_empty());
    futures_util::pin_mut!(stream);
    let mut count = 0;
    while let Some(entry) = stream.next().await {
        assert_eq!(entry.unwrap().log_id.index, count);
        count += 1;
    }
    assert_eq!(count, 10001);
    let pages = &observed.lock().unwrap().pages;
    assert_eq!(pages.len(), 79);
    assert!(pages
        .iter()
        .all(|(_, n, bytes)| *n <= PAGE_COUNT && *bytes <= PAGE_BYTES));
}

#[tokio::test]
async fn byte_budget_and_drop_prevent_prefetch() {
    let (mut reader, observed) = reader(300_000);
    {
        let stream = reader.entries_stream(0..10001).await;
        futures_util::pin_mut!(stream);
        assert_eq!(stream.next().await.unwrap().unwrap().log_id.index, 0);
        assert_eq!(observed.lock().unwrap().pages.len(), 1);
    }
    assert_eq!(observed.lock().unwrap().pages[0].1, 3);
    // Dropping the stream starts no background reads and keeps no DB transaction.
    assert_eq!(observed.lock().unwrap().pages.len(), 1);
}

#[tokio::test]
async fn page_failure_and_gap_are_terminal_without_partial_bad_page() {
    for gap in [false, true] {
        let (mut reader, observed) = reader(0);
        let stream = reader.entries_stream(0..10001).await;
        futures_util::pin_mut!(stream);
        for n in 0..128 {
            assert_eq!(stream.next().await.unwrap().unwrap().log_id.index, n);
        }
        if gap {
            observed.lock().unwrap().gap_at = Some(129);
        } else {
            observed.lock().unwrap().fail_at = Some(128);
        }
        assert!(stream.next().await.unwrap().is_err());
        assert!(stream.next().await.is_none());
    }
}

#[tokio::test]
async fn empty_and_maximum_index_ranges_do_not_wrap() {
    let (mut reader, observed) = reader(0);
    {
        let stream = reader.entries_stream(0..0).await;
        futures_util::pin_mut!(stream);
        assert!(stream.next().await.is_none());
    }
    assert!(observed.lock().unwrap().pages.is_empty());
    let stream = reader.entries_stream(u64::MAX..=u64::MAX).await;
    futures_util::pin_mut!(stream);
    assert_eq!(stream.next().await.unwrap().unwrap().log_id.index, u64::MAX);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn cancellation_during_page_read_stops_without_background_work() {
    let (mut reader, observed) = reader(0);
    observed.lock().unwrap().stall_at = Some(128);
    {
        let stream = reader.entries_stream(0..10001).await;
        futures_util::pin_mut!(stream);
        for _ in 0..128 {
            stream.next().await.unwrap().unwrap();
        }
        assert!(futures_util::poll!(stream.next()).is_pending());
        assert_eq!(observed.lock().unwrap().attempts, 2);
    }
    assert_eq!(observed.lock().unwrap().attempts, 2);
    assert_eq!(observed.lock().unwrap().pages.len(), 1);
}

#[tokio::test]
async fn oversized_entry_is_rejected_without_yielding_or_looping() {
    let (mut reader, observed) = reader(PAGE_BYTES);
    let stream = reader.entries_stream(0..1).await;
    futures_util::pin_mut!(stream);
    assert!(stream.next().await.unwrap().is_err());
    assert!(stream.next().await.is_none());
    assert!(observed.lock().unwrap().pages.is_empty());
}

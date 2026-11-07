use broker_cluster::journal::JournalStorage;
use deadpool_postgres::{Manager, Pool};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_postgres::{Config, NoTls};
openraft::declare_raft_types!(pub TestConfig: D = String, R = (), SnapshotData = std::io::Cursor<Vec<u8>>);
type Store = JournalStorage<TestConfig>;
fn pool() -> Pool {
    let config: Config = std::env::var("DATABASE_URL").unwrap().parse().unwrap();
    Pool::builder(Manager::new(config, NoTls))
        .max_size(1)
        .build()
        .unwrap()
}
fn key() -> String {
    format!(
        "reader-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
fn id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, 1), index)
}
fn entry(index: u64) -> Entry<TestConfig> {
    Entry {
        log_id: id(index),
        payload: EntryPayload::Blank,
    }
}

#[tokio::test]
#[ignore = "requires disposable SQL0.1.19"]
async fn committed_pages_are_bounded_release_pool_and_fence_owners() {
    let pool = pool();
    let key = key();
    let mut store = Store::claim(pool.clone(), key.clone()).await.unwrap();
    for start in (0..10001).step_by(256) {
        store
            .append(
                &(start..(start + 256).min(10001))
                    .map(entry)
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
    }
    store.commit(id(10000)).await.unwrap();
    let reader = store.reader();
    let mut pages = reader.committed_pages(0, id(10000)).unwrap();
    let mut count = 0;
    while let Some(entries) = pages.next_page().await.unwrap() {
        assert!(entries.len() <= 256);
        for entry in entries {
            assert_eq!(entry.log_id.index, count);
            count += 1;
        }
        // The single-slot pool must be available between pages.
        assert_eq!(store.state().await.unwrap().committed, Some(id(10000)));
    }
    assert_eq!(count, 10001);
    let mut pages = reader.committed_pages(0, id(10000)).unwrap();
    pages.next_page().await.unwrap().unwrap();
    let _successor = Store::claim(pool, key).await.unwrap();
    assert!(pages.next_page().await.is_err());
    assert!(pages.next_page().await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires disposable SQL0.1.19"]
async fn committed_pages_reject_purge_and_cancellation_is_terminal() {
    let pool = pool();
    let key = key();
    let mut store = Store::claim(pool.clone(), key.clone()).await.unwrap();
    for start in [0, 256] {
        store
            .append(&(start..start + 256).map(entry).collect::<Vec<_>>())
            .await
            .unwrap();
    }
    store.commit(id(511)).await.unwrap();
    let reader = store.reader();
    let mut pages = reader.committed_pages(0, id(511)).unwrap();
    pages.next_page().await.unwrap().unwrap();
    store.purge(id(256)).await.unwrap();
    assert!(pages.next_page().await.is_err());
    assert!(pages.next_page().await.unwrap().is_none());
    let lock_pool = {
        let config: Config = std::env::var("DATABASE_URL").unwrap().parse().unwrap();
        Pool::builder(Manager::new(config, NoTls))
            .max_size(1)
            .build()
            .unwrap()
    };
    let mut held = lock_pool.get().await.unwrap();
    let tx = held.transaction().await.unwrap();
    tx.query_one(
        "SELECT group_key FROM broker.raft_votes WHERE group_key=$1 FOR UPDATE",
        &[&key],
    )
    .await
    .unwrap();
    // A second pool lets the cursor reach the owner-row lock rather than pool wait.
    let mut pages = reader.committed_pages(257, id(511)).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), pages.next_page())
            .await
            .is_err()
    );
    assert!(pages.next_page().await.unwrap().is_none());
    drop(tx);
    drop(held);
    let mut fresh = reader.committed_pages(257, id(511)).unwrap();
    assert_eq!(fresh.next_page().await.unwrap().unwrap().len(), 255);
}

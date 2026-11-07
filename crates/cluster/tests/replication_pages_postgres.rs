use broker_cluster::journal::JournalStorage;
use deadpool_postgres::{Manager, Pool};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use std::time::{SystemTime, UNIX_EPOCH};
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
#[ignore = "requires disposable SQL0.1.20"]
async fn replication_pages_check_votes_release_pool_and_fence_owners() {
    use broker_cluster::reader::ReplicationPage;
    use openraft::Vote;
    let pool = pool();
    let key = key();
    let mut store = Store::claim(pool.clone(), key.clone()).await.unwrap();
    let reader = store.reader();
    let expected = Vote::new_committed(1, 1);
    assert!(matches!(
        reader
            .replication_page(0, id(5000), expected)
            .await
            .unwrap(),
        ReplicationPage::LeaderChanged(None)
    ));
    store.save_vote(expected).await.unwrap();
    for start in (0..5001).step_by(256) {
        store
            .append(
                &(start..(start + 256).min(5001))
                    .map(entry)
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
    }
    let mut count = 0;
    while count < 5001 {
        let ReplicationPage::Entries(entries) = reader
            .replication_page(count, id(5000), expected)
            .await
            .unwrap()
        else {
            panic!("unexpected leader change")
        };
        for entry in entries {
            assert_eq!(entry.log_id, id(count));
            count += 1;
        }
        assert_eq!(store.state().await.unwrap().committed, None);
    }
    let next = Vote::new(2, 2);
    store.save_vote(next).await.unwrap();
    assert!(
        matches!(reader.replication_page(0, id(5000), expected).await.unwrap(), ReplicationPage::LeaderChanged(Some(v)) if v == next)
    );
    assert!(reader.replication_page(0, id(5000), next).await.is_err());
    let _new_owner = Store::claim(pool, key).await.unwrap();
    assert!(reader
        .replication_page(0, id(5000), expected)
        .await
        .is_err());
}

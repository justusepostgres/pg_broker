//! Explicit disposable-PostgreSQL tests; no network-consensus claim.
use broker_cluster::application::PartitionCommand;
use broker_cluster::journal::JournalStorage;
use deadpool_postgres::{Manager, Pool};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::{Config, NoTls};

openraft::declare_raft_types!(pub TestConfig:
    D = PartitionCommand, R = (), SnapshotData = std::io::Cursor<Vec<u8>>
);
type Store = JournalStorage<TestConfig>;

fn pool() -> Pool {
    let config: Config = std::env::var("DATABASE_URL")
        .expect("disposable DATABASE_URL")
        .parse()
        .unwrap();
    Pool::builder(Manager::new(config, NoTls))
        .max_size(1)
        .build()
        .unwrap()
}
fn key() -> String {
    format!(
        "journal-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
fn id(index: u64, term: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}
fn entry(index: u64, term: u64) -> Entry<TestConfig> {
    Entry {
        log_id: id(index, term),
        payload: EntryPayload::Normal(
            PartitionCommand::append(
                0,
                1700000000123,
                &hex::decode(include_str!("fixtures/append-batch.hex").trim()).unwrap(),
            )
            .unwrap(),
        ),
    }
}

async fn setup(pool: &Pool, topic: &str) -> String {
    let c = pool.get().await.unwrap();
    let tid: String = c
        .query_one(
            "INSERT INTO broker.topics(name,partitions) VALUES($1,1) RETURNING topic_id::text",
            &[&topic],
        )
        .await
        .unwrap()
        .get(0);
    c.execute(
        "INSERT INTO broker.partitions(topic,partition) VALUES($1,0)",
        &[&topic],
    )
    .await
    .unwrap();
    tid
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL with opt-in SQL 0.1.13"]
async fn typed_snapshot_capture_and_release_follow_the_same_owner() {
    use broker_cluster::snapshot::CaptureState;
    let pool = pool();
    let key = key();
    let tid = setup(&pool, &key).await;
    let mut store = Store::claim(pool.clone(), key.clone()).await.unwrap();
    let mut app = store.application();
    app.bind(&key, &tid, 0).await.unwrap();
    store.append(&[entry(0, 1)]).await.unwrap();
    store.commit(id(0, 1)).await.unwrap();
    app.apply(id(0, 1)).await.unwrap();
    let capture = app.begin_snapshot().await.unwrap();
    assert_eq!(capture.state, CaptureState::Capturing);
    assert_eq!(capture.cursor_offset, 0);
    assert_eq!(capture.end_offset, 1);
    assert_eq!(
        app.begin_snapshot().await.unwrap().snapshot_id,
        capture.snapshot_id
    );
    let successor = Store::claim(pool, key.clone()).await.unwrap();
    let mut next = successor.application();
    assert!(app.copy_snapshot(&capture.snapshot_id).await.is_err());
    let done = next.copy_snapshot(&capture.snapshot_id).await.unwrap();
    assert_eq!(done.state, CaptureState::Complete);
    assert_eq!(done.cursor_offset, 1);
    assert_eq!(done.manifest, capture.manifest);
    assert!(next.release_snapshot(&capture.snapshot_id).await.unwrap());
    assert!(next.release_snapshot(&capture.snapshot_id).await.unwrap());
}

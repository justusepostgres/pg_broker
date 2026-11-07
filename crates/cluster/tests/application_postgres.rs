//! Explicit disposable-PostgreSQL tests; no network-consensus claim.
use broker_cluster::application::PartitionCommand;
use broker_cluster::journal::JournalStorage;
use deadpool_postgres::{Manager, Pool};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Vote};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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
#[ignore = "requires disposable PostgreSQL with opt-in SQL 0.1.12"]
async fn typed_application_commits_data_and_marker_and_fences_retries() {
    let pool = pool();
    let key = key();
    let tid = setup(&pool, &key).await;
    let mut store = Store::claim(pool.clone(), key.clone()).await.unwrap();
    let mut app = store.application();
    let initial = app.bind(&key, &tid, 0).await.unwrap();
    assert!(initial.applied.is_none());
    store.save_vote(Vote::new_committed(1, 1)).await.unwrap();
    store.append(&[entry(0, 1)]).await.unwrap();
    assert!(app.apply(id(0, 1)).await.is_err());
    store.commit(id(0, 1)).await.unwrap();
    // Trigger observes the effective setting of the actual append, not a
    // wrapper's claim; a session-level async default must be overridden.
    pool.get().await.unwrap().batch_execute("CREATE FUNCTION broker.test_apply_sync() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF current_setting('synchronous_commit')<>'on' THEN RAISE EXCEPTION 'async application'; END IF; RETURN NEW; END $$; CREATE TRIGGER test_apply_sync BEFORE INSERT ON broker.batches FOR EACH ROW EXECUTE FUNCTION broker.test_apply_sync(); SET synchronous_commit=off").await.unwrap();
    let applied = app.apply(id(0, 1)).await.unwrap();
    assert_eq!(applied.applied, Some(id(0, 1)));
    assert_eq!(applied.result.unwrap().offset, 0);
    assert_eq!(app.apply(id(0, 1)).await.unwrap().result.unwrap().offset, 0);
    pool.get().await.unwrap().batch_execute("DROP TRIGGER test_apply_sync ON broker.batches; DROP FUNCTION broker.test_apply_sync()").await.unwrap();
    let successor = Store::claim(pool.clone(), key.clone()).await.unwrap();
    let mut next = successor.application();
    assert_eq!(
        next.bind(&key, &tid, 0).await.unwrap().applied,
        Some(id(0, 1))
    );
    assert_eq!(
        next.apply(id(0, 1)).await.unwrap().result.unwrap().offset,
        0
    );
    assert!(app.apply(id(0, 1)).await.is_err());
    let n: i64 = pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT next_offset FROM broker.partitions WHERE topic=$1 AND partition=0",
            &[&key],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 1);
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL with opt-in SQL 0.1.12"]
async fn cancelled_application_discards_connection_and_does_not_advance() {
    let pool = pool();
    let key = key();
    let tid = setup(&pool, &key).await;
    let mut store = Store::claim(pool.clone(), key.clone()).await.unwrap();
    let mut app = store.application();
    app.bind(&key, &tid, 0).await.unwrap();
    store.append(&[entry(0, 1)]).await.unwrap();
    store.commit(id(0, 1)).await.unwrap();
    let pid: i32 = pool
        .get()
        .await
        .unwrap()
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let blocker_pool = crate::pool();
    let mut blocker = blocker_pool.get().await.unwrap();
    let tx = blocker.transaction().await.unwrap();
    tx.query_one(
        "SELECT 1 FROM broker.partitions WHERE topic=$1 AND partition=0 FOR UPDATE",
        &[&key],
    )
    .await
    .unwrap();
    let write = tokio::spawn(async move { app.apply(id(0, 1)).await });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let blocked: bool = tx
            .query_one("SELECT cardinality(pg_blocking_pids($1))>0", &[&pid])
            .await
            .unwrap()
            .get(0);
        if blocked {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    write.abort();
    assert!(write.await.unwrap_err().is_cancelled());
    tx.rollback().await.unwrap();
    let new_pid: i32 = pool
        .get()
        .await
        .unwrap()
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    assert_ne!(pid, new_pid);
    let mut app = store.application();
    assert!(app.bind(&key, &tid, 0).await.unwrap().applied.is_none());
    assert_eq!(app.apply(id(0, 1)).await.unwrap().result.unwrap().offset, 0);
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL with opt-in SQL 0.1.16"]
async fn typed_retention_commits_frontier_and_replays_after_owner_change() {
    let pool = pool();
    let key = key();
    let tid = setup(&pool, &key).await;
    let mut store = Store::claim(pool.clone(), key.clone()).await.unwrap();
    let mut app = store.application();
    app.bind(&key, &tid, 0).await.unwrap();
    store.append(&[entry(0, 1)]).await.unwrap();
    store.commit(id(0, 1)).await.unwrap();
    app.apply(id(0, 1)).await.unwrap();
    store
        .append(&[Entry {
            log_id: id(1, 1),
            payload: EntryPayload::Normal(PartitionCommand::retain(0, 1).unwrap()),
        }])
        .await
        .unwrap();
    assert!(app.apply(id(1, 1)).await.is_err());
    store.commit(id(1, 1)).await.unwrap();
    assert_eq!(app.apply(id(1, 1)).await.unwrap().result.unwrap().offset, 1);
    let next = Store::claim(pool.clone(), key.clone()).await.unwrap();
    assert_eq!(
        next.application()
            .apply(id(1, 1))
            .await
            .unwrap()
            .result
            .unwrap()
            .offset,
        1
    );
    assert!(app.apply(id(1, 1)).await.is_err());
    let row = pool.get().await.unwrap().query_one(
        "SELECT earliest,next_offset,(SELECT count(*) FROM broker.batches WHERE topic=$1) FROM broker.partitions WHERE topic=$1 AND partition=0", &[&key]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, i64>(1), 1);
    assert_eq!(row.get::<_, i64>(2), 0);
}

//! Explicit disposable-PostgreSQL tests; no network-consensus claim.
use broker_cluster::journal::JournalStorage;
use deadpool_postgres::{Manager, Pool};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Membership, Vote};
use std::{
    collections::BTreeSet,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Config, NoTls};

openraft::declare_raft_types!(pub TestConfig:
    D = Vec<u8>, R = (), SnapshotData = std::io::Cursor<Vec<u8>>
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
        payload: EntryPayload::Normal(vec![0, 1, 255]),
    }
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL with opt-in SQL 0.1.10"]
async fn typed_entries_votes_and_prefixes_survive_owner_replacement() {
    let pool = pool();
    let key = key();
    let mut store = Store::claim(pool.clone(), key.clone()).await.unwrap();
    let vote = Vote::new_committed(u64::MAX, u64::MAX);
    store.save_vote(vote).await.unwrap();
    let entries = vec![
        Entry {
            log_id: id(0, 1),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: id(1, 1),
            payload: EntryPayload::Membership(Membership::new(
                vec![BTreeSet::from([1, 2, 3])],
                BTreeSet::new(),
            )),
        },
        entry(2, u64::MAX),
    ];
    store.append(&entries).await.unwrap();
    store.append(&entries).await.unwrap();
    assert_eq!(store.read(0, u64::MAX, 256).await.unwrap(), entries);
    store.commit(id(1, 1)).await.unwrap();
    assert!(store.truncate(id(1, 1)).await.is_err());
    store.truncate(id(2, u64::MAX)).await.unwrap();
    store.append(&[entry(2, u64::MAX)]).await.unwrap();
    // This isolated test stands in for an external applied-state proof. It
    // does not provide a snapshot implementation to a running Raft node.
    store.purge(id(1, 1)).await.unwrap();
    let mut successor = Store::claim(pool.clone(), key).await.unwrap();
    assert_eq!(successor.read_vote().await.unwrap(), Some(vote));
    assert_eq!(
        successor.read(0, u64::MAX, 256).await.unwrap(),
        vec![entry(2, u64::MAX)]
    );
    let state = successor.state().await.unwrap();
    assert_eq!(state.purged, Some(id(1, 1)));
    assert_eq!(state.committed, Some(id(1, 1)));
    assert_eq!(state.last, Some(id(2, u64::MAX)));
    assert!(store.append(&[]).await.is_err());
    assert!(store.read(0, 3, 3).await.is_err());
    assert!(store.save_vote(vote).await.is_err());
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL with opt-in SQL 0.1.10"]
async fn cancellation_discards_connection_and_does_not_commit_append_later() {
    let pool = pool();
    let key = key();
    let mut store = Store::claim(pool.clone(), key.clone()).await.unwrap();
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
        "SELECT group_key FROM broker.raft_votes WHERE group_key=$1 FOR UPDATE",
        &[&key],
    )
    .await
    .unwrap();
    let write = tokio::spawn(async move { store.append(&[entry(0, 1)]).await });
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
        assert!(
            tokio::time::Instant::now() < deadline,
            "append never blocked on owner row"
        );
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
    let mut successor = Store::claim(pool, key).await.unwrap();
    assert_eq!(successor.state().await.unwrap().last, None);
    successor.append(&[entry(0, 1)]).await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL with opt-in SQL 0.1.10"]
async fn confirmed_durability_and_request_bounds() {
    let pool = pool();
    let key = key();
    let mut store = Store::claim(pool.clone(), key).await.unwrap();
    pool.get().await.unwrap().batch_execute(
        "CREATE FUNCTION broker.test_journal_sync() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN IF current_setting('synchronous_commit')<>'on' THEN RAISE EXCEPTION 'async journal write'; END IF; RETURN NEW; END $$;
         CREATE TRIGGER test_journal_sync BEFORE INSERT ON broker.raft_log_entries FOR EACH ROW EXECUTE FUNCTION broker.test_journal_sync();
         SET synchronous_commit=off").await.unwrap();
    store.append(&[entry(0, 1)]).await.unwrap();
    let setting: String = pool
        .get()
        .await
        .unwrap()
        .query_one("SHOW synchronous_commit", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(setting, "off");
    assert!(store.append(&vec![entry(1, 1); 257]).await.is_err());
    let oversized = Entry {
        log_id: id(1, 1),
        payload: EntryPayload::Normal(vec![255; 3 * 1024 * 1024]),
    };
    assert!(store.append(&[oversized]).await.is_err());
    assert!(store.read(0, 1, 257).await.is_err());
    assert_eq!(store.state().await.unwrap().last, Some(id(0, 1)));
    pool.get().await.unwrap().batch_execute("DROP TRIGGER test_journal_sync ON broker.raft_log_entries; DROP FUNCTION broker.test_journal_sync()").await.unwrap();
}

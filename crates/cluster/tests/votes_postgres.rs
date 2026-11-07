//! Run explicitly with DATABASE_URL pointing at disposable SQL 0.1.9 storage.
use broker_cluster::votes::VoteStorage;
use deadpool_postgres::{Manager, Pool};
use openraft::Vote;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_postgres::{Config, NoTls};

fn pool(max: usize) -> Pool {
    let config: Config = std::env::var("DATABASE_URL")
        .expect("disposable DATABASE_URL required")
        .parse()
        .unwrap();
    Pool::builder(Manager::new(config, NoTls))
        .max_size(max)
        .build()
        .unwrap()
}

fn key(label: &str) -> String {
    format!(
        "vote-test-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL with opt-in extension SQL 0.1.9"]
async fn persisted_vote_survives_new_owner_and_fences_old_cached_operations() {
    let key = key("takeover");
    let mut first = VoteStorage::claim(pool(2), key.clone()).await.unwrap();
    assert_eq!(first.read_vote().await.unwrap(), None);
    let vote = Vote::new_committed(4, 7);
    first.save_vote(vote).await.unwrap();
    let mut successor = VoteStorage::claim(pool(2), key).await.unwrap();
    assert_eq!(successor.read_vote().await.unwrap(), Some(vote));
    assert!(first.read_vote().await.is_err());
    assert!(
        first.save_vote(vote).await.is_err(),
        "identical cached vote must still be fenced"
    );
    assert!(first.save_vote(Vote::new(5, 7)).await.is_err());
    assert!(successor.save_vote(Vote::new(3, 7)).await.is_err());
    assert!(
        successor.save_vote(Vote::new(4, 7)).await.is_err(),
        "committed vote cannot regress"
    );
    successor
        .save_vote(Vote::new(u64::MAX, u64::MAX))
        .await
        .unwrap();
    assert_eq!(
        successor.read_vote().await.unwrap(),
        Some(Vote::new(u64::MAX, u64::MAX))
    );
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL with opt-in extension SQL 0.1.9"]
async fn cancelling_blocked_vote_discards_connection_and_never_commits_it_later() {
    let pool = pool(1);
    let key = key("cancel");
    let mut owner = VoteStorage::claim(pool.clone(), key.clone()).await.unwrap();
    let vote = Vote::new(1, 1);
    owner.save_vote(vote).await.unwrap();
    let old_pid: i32 = pool
        .get()
        .await
        .unwrap()
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let blocker_pool = crate::pool(1);
    let mut blocker = blocker_pool.get().await.unwrap();
    let tx = blocker.transaction().await.unwrap();
    tx.query_one(
        "SELECT group_key FROM broker.raft_votes WHERE group_key=$1 FOR UPDATE",
        &[&key],
    )
    .await
    .unwrap();
    let write = tokio::spawn(async move { owner.save_vote(Vote::new(2, 1)).await });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let blocked: bool = tx
            .query_one("SELECT cardinality(pg_blocking_pids($1))>0", &[&old_pid])
            .await
            .unwrap()
            .get(0);
        if blocked {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "write never reached the held row lock"
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
    assert_ne!(
        old_pid, new_pid,
        "cancelled connection must not return to pool"
    );
    let mut successor = VoteStorage::claim(pool.clone(), key).await.unwrap();
    assert_eq!(successor.read_vote().await.unwrap(), Some(vote));
    successor.save_vote(Vote::new(3, 1)).await.unwrap();
    assert!(pool.status().size <= 1);
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL with opt-in extension SQL 0.1.9"]
async fn session_async_commit_is_overridden_and_invalid_inputs_are_bounded() {
    let pool = pool(1);
    pool.get()
        .await
        .unwrap()
        .batch_execute(
            "CREATE FUNCTION broker.test_vote_sync_guard() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN IF current_setting('synchronous_commit') <> 'on' THEN
          RAISE EXCEPTION 'vote persisted without synchronous commit'; END IF; RETURN NEW; END $$;
         CREATE TRIGGER test_vote_sync_guard BEFORE INSERT OR UPDATE ON broker.raft_votes
         FOR EACH ROW EXECUTE FUNCTION broker.test_vote_sync_guard();",
        )
        .await
        .unwrap();
    pool.get()
        .await
        .unwrap()
        .batch_execute("SET synchronous_commit=off")
        .await
        .unwrap();
    assert!(VoteStorage::claim(pool.clone(), String::new())
        .await
        .is_err());
    assert!(VoteStorage::claim(pool.clone(), "x".repeat(250))
        .await
        .is_err());
    assert!(VoteStorage::claim(crate::pool(17), key("oversized-pool"))
        .await
        .is_err());
    let mut owner = VoteStorage::claim(pool.clone(), key("settings"))
        .await
        .unwrap();
    owner.save_vote(Vote::new(1, 1)).await.unwrap();
    assert_eq!(owner.read_vote().await.unwrap(), Some(Vote::new(1, 1)));
    let setting: String = pool
        .get()
        .await
        .unwrap()
        .query_one("SHOW synchronous_commit", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(setting, "off", "override must remain transaction-local");
    pool.get().await.unwrap().batch_execute(
        "DROP TRIGGER test_vote_sync_guard ON broker.raft_votes; DROP FUNCTION broker.test_vote_sync_guard();"
    ).await.unwrap();
}

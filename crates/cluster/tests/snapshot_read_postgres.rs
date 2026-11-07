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
#[ignore = "requires disposable PostgreSQL with opt-in SQL 0.1.14"]
async fn typed_snapshot_pages_validate_bytes_metadata_and_fence_old_owner() {
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
    assert!(app.read_snapshot(&capture.snapshot_id, 0).await.is_err());
    app.copy_snapshot(&capture.snapshot_id).await.unwrap();
    let page = app.read_snapshot(&capture.snapshot_id, 0).await.unwrap();
    assert!(page.eof);
    assert_eq!(page.next_offset, 1);
    assert_eq!(page.batches.len(), 1);
    assert_eq!(
        page.batches[0].data,
        include_str!("fixtures/append-batch.hex").trim()
    );
    assert_eq!(page.batches[0].appended_at_micros, 1700000000123000);
    assert!(app
        .read_snapshot(&capture.snapshot_id, 1)
        .await
        .unwrap()
        .batches
        .is_empty());
    assert!(app.read_snapshot(&capture.snapshot_id, 2).await.is_err());
    // Transfer through a tiny pipe: exporter must handle backpressure with no
    // database connection held while waiting for receiver IO (pool size is 1).
    let (mut sink, source) = tokio::io::duplex(32);
    let sid = capture.snapshot_id.clone();
    let export = broker_cluster::snapshot_stream::export_snapshot(&app, &sid, 0, &mut sink, 10000);
    let receive = async {
        use tokio::io::AsyncReadExt;
        let mut source = source;
        let mut prefix = [0; 12];
        source.read_exact(&mut prefix).await.unwrap();
        // Frame length is emitted only after the SQL page read. With a 32-byte
        // pipe the exporter now blocks on payload IO, so a leaked connection
        // would deadlock this single-connection pool request.
        pool.get()
            .await
            .unwrap()
            .simple_query("SELECT 1")
            .await
            .unwrap();
        let source = std::io::Cursor::new(prefix).chain(source);
        let mut stream =
            broker_cluster::snapshot_stream::SnapshotStream::new(source, sid.clone(), 0, 10000)
                .await
                .unwrap();
        let received = stream.next_page().await.unwrap().unwrap();
        assert_eq!(received.batches[0].data, page.batches[0].data);
        assert_eq!(received.snapshot.manifest, page.snapshot.manifest);
        assert!(stream.next_page().await.unwrap().is_none());
    };
    let (sent, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(export, receive)
    })
    .await
    .unwrap();
    assert!(sent.is_ok());
    // Stage a real database image and validate the durable file after reopening.
    let mut encoded = Vec::new();
    broker_cluster::snapshot_stream::export_snapshot(&app, &sid, 0, &mut encoded, 10000)
        .await
        .unwrap();
    let directory = std::env::temp_dir().join(format!("broker-snapshot-file-{key}"));
    tokio::fs::create_dir(&directory).await.unwrap();
    let staged = broker_cluster::snapshot_file::receive_snapshot_file(
        &encoded[..],
        &directory,
        &sid,
        0,
        10000,
    )
    .await
    .unwrap();
    assert_eq!(staged.manifest, page.snapshot.manifest);
    let path = directory.join("snapshot.bin");
    broker_cluster::snapshot_file::validate_snapshot_file(&path, &sid, 0, 10000)
        .await
        .unwrap();
    let mut staged_reader = broker_cluster::snapshot_stream::SnapshotStream::new(
        tokio::fs::File::open(&path).await.unwrap(),
        sid.clone(),
        0,
        10000,
    )
    .await
    .unwrap();
    assert_eq!(
        staged_reader.next_page().await.unwrap().unwrap().batches[0].data,
        page.batches[0].data
    );
    assert!(staged_reader.next_page().await.unwrap().is_none());
    drop(staged_reader);
    tokio::fs::remove_dir_all(directory).await.unwrap();
    // The typed reader detects corrupted stored bytes before returning a page.
    pool.get().await.unwrap().execute("UPDATE broker.raft_snapshot_batches SET data=set_byte(data,octet_length(data)-1,get_byte(data,octet_length(data)-1)#1) WHERE snapshot_id=$1::text::uuid", &[&capture.snapshot_id]).await.unwrap();
    assert!(app.read_snapshot(&capture.snapshot_id, 0).await.is_err());
    let successor = Store::claim(pool, key).await.unwrap();
    assert!(app.read_snapshot(&capture.snapshot_id, 1).await.is_err());
    let next = successor.application();
    assert!(
        next.read_snapshot(&capture.snapshot_id, 1)
            .await
            .unwrap()
            .eof
    );
}

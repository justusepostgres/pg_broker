//! Real TCP / three database consensus probe. One physical PG server, no RF3 host claim.
use super::{
    alpha_log::Store,
    alpha_machine::Machine,
    alpha_node_tests::{command, offset, setup},
    alpha_tcp::{self, Network, Server},
    Next, Old,
};
use broker_cluster::journal::JournalStorage;
use deadpool_postgres::{Manager, Pool};
use openraft_next::{BasicNode, Config, Raft, SnapshotPolicy};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{net::TcpListener, sync::Mutex};
use tokio_postgres::{Config as PgConfig, NoTls};
struct Node {
    raft: Raft<Next, Machine>,
    server: Server,
}
impl Node {
    async fn stop(self) {
        self.server.shutdown().await;
        self.raft.shutdown().await.unwrap();
    }
}
fn pool(config: PgConfig) -> Pool {
    Pool::builder(Manager::new(config, NoTls))
        .max_size(4)
        .build()
        .unwrap()
}
async fn start(pool: Pool, id: u64, topic_id: &str, listener: TcpListener) -> Node {
    let store = JournalStorage::<Old>::claim(pool, "quorum".into())
        .await
        .unwrap();
    let mut app = store.application();
    app.bind("quorum", topic_id, 0).await.unwrap();
    let config = Config {
        snapshot_policy: SnapshotPolicy::Never,
        max_payload_entries: 128,
        election_timeout_min: 400,
        election_timeout_max: 800,
        heartbeat_interval: 100,
        ..Config::default()
    };
    let raft = Raft::new(
        id,
        Arc::new(config.validate().unwrap()),
        Network::factory(),
        Store(Arc::new(Mutex::new(store))),
        Machine {
            app,
            topic: "quorum".into(),
            topic_id: topic_id.into(),
        },
    )
    .await
    .unwrap();
    let server = alpha_tcp::serve(listener, raft.clone());
    Node { raft, server }
}
async fn leader(nodes: &[Option<Node>]) -> usize {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            for (index, node) in nodes.iter().enumerate() {
                if let Some(node) = node {
                    if node.raft.current_leader().await == Some(index as u64 + 1)
                        && matches!(
                            tokio::time::timeout(
                                Duration::from_secs(2),
                                node.raft
                                    .ensure_linearizable(openraft_next::ReadPolicy::ReadIndex)
                            )
                            .await,
                            Ok(Ok(_))
                        )
                    {
                        return index;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}
async fn applied(pools: &[Pool], indices: &[usize], expected: i64) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let mut done = true;
            for &i in indices {
                done &= offset(&pools[i], "quorum").await == expected;
            }
            if done {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
#[ignore = "requires disposable SQL0.1.20 with database creation privilege"]
async fn tcp_quorum_replicates_elects_recovers_and_requires_majority() {
    let config: PgConfig = std::env::var("DATABASE_URL").unwrap().parse().unwrap();
    let admin = pool(config.clone());
    let prefix = format!(
        "quorum_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut pools = Vec::new();
    let mut ids = Vec::new();
    let mut addresses = Vec::new();
    let mut nodes = Vec::new();
    for i in 0..3 {
        let name = format!("{prefix}_{i}");
        admin
            .get()
            .await
            .unwrap()
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .await
            .unwrap();
        let mut cfg = config.clone();
        cfg.dbname(&name);
        let p = pool(cfg);
        p.get()
            .await
            .unwrap()
            .batch_execute("CREATE EXTENSION pg_broker VERSION '0.1.20'")
            .await
            .unwrap();
        let tid = setup(&p, "quorum").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(listener.local_addr().unwrap());
        nodes.push(Some(start(p.clone(), i + 1, &tid, listener).await));
        pools.push(p);
        ids.push(tid);
    }
    let members: BTreeMap<_, _> = addresses
        .iter()
        .enumerate()
        .map(|(i, a)| (i as u64 + 1, BasicNode::new(a)))
        .collect();
    nodes[0]
        .as_ref()
        .unwrap()
        .raft
        .initialize(members)
        .await
        .unwrap();
    let first = leader(&nodes).await;
    nodes[first]
        .as_ref()
        .unwrap()
        .raft
        .client_write(command(0))
        .await
        .unwrap();
    nodes[first].take().unwrap().stop().await;
    let second = leader(&nodes).await;
    assert_ne!(first, second);
    let survivors: Vec<_> = (0..3).filter(|&i| i != first).collect();
    applied(&pools, &survivors, 1).await;
    nodes[second]
        .as_ref()
        .unwrap()
        .raft
        .client_write(command(1))
        .await
        .unwrap();
    applied(&pools, &survivors, 2).await;
    nodes[first] = Some(
        start(
            pools[first].clone(),
            first as u64 + 1,
            &ids[first],
            TcpListener::bind(addresses[first]).await.unwrap(),
        )
        .await,
    );
    applied(&pools, &[0, 1, 2], 2).await;
    let current = leader(&nodes).await;
    nodes[current]
        .as_ref()
        .unwrap()
        .raft
        .client_write(command(2))
        .await
        .unwrap();
    applied(&pools, &[0, 1, 2], 3).await;
    let mut copies = Vec::new();
    for p in &pools {
        let rows = p.get().await.unwrap().query("SELECT base_offset,data FROM broker.batches WHERE topic='quorum' AND partition=0 ORDER BY base_offset", &[]).await.unwrap();
        copies.push(
            rows.into_iter()
                .map(|r| (r.get::<_, i64>(0), r.get::<_, Vec<u8>>(1)))
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(
        copies[0].iter().map(|r| r.0).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(copies[0], copies[1]);
    assert_eq!(copies[1], copies[2]);
    for (i, node) in nodes.iter_mut().enumerate() {
        if i != current {
            node.take().unwrap().stop().await;
        }
    }
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        nodes[current]
            .as_ref()
            .unwrap()
            .raft
            .client_write(command(3)),
    )
    .await;
    assert!(
        !matches!(result, Ok(Ok(_))),
        "minority must not acknowledge writes"
    );
    assert_eq!(offset(&pools[current], "quorum").await, 3);
    nodes[current].take().unwrap().stop().await;
    for (i, p) in pools.into_iter().enumerate() {
        p.close();
        drop(p);
        admin
            .get()
            .await
            .unwrap()
            .batch_execute(&format!("DROP DATABASE {prefix}_{i} WITH (FORCE)"))
            .await
            .unwrap();
    }
}

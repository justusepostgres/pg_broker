use anyhow::{ensure, Context, Result};
use broker_protocol::{Reader, Writer, APIS, MAX_FRAME};
use deadpool_postgres::Pool;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    time::timeout,
};
use tokio_postgres::IsolationLevel;

#[derive(Clone)]
pub struct State {
    pub pool: Pool,
    pub host: String,
    pub port: i32,
}
impl State {
    pub async fn db(&self, q: Value) -> Result<Value> {
        let mut client = timeout(Duration::from_secs(3), self.pool.get()).await??;
        let mut builder = client.build_transaction();
        if q["op"] == "fetch" {
            builder = builder.isolation_level(IsolationLevel::RepeatableRead);
        }
        let tx = builder.start().await?;
        tx.batch_execute("SET LOCAL synchronous_commit=on; SET LOCAL statement_timeout='3s'; SET LOCAL lock_timeout='2s'").await?;
        let v: Value = tx
            .query_one("SELECT public.broker_call($1)", &[&q])
            .await?
            .get(0);
        // A PostgreSQL statement timeout during SyncRep can produce only a
        // WARNING after local commit. Never turn that into a durability ack.
        // Disable server cancellation for COMMIT and enforce our bound by
        // discarding the physical connection if confirmation does not arrive.
        tx.batch_execute("SET LOCAL statement_timeout=0").await?;
        let committed = timeout(Duration::from_secs(5), tx.commit()).await;
        if !matches!(&committed, Ok(Ok(()))) {
            drop(deadpool_postgres::Object::take(client));
        }
        committed.context("commit confirmation timed out; outcome unknown")??;
        Ok(v)
    }
    pub async fn poll(&self, q: Value) -> Result<Value> {
        let end = tokio::time::Instant::now() + Duration::from_secs(305);
        loop {
            let v = self.db(q.clone()).await?;
            if v["wait"] != true {
                return Ok(v);
            }
            if tokio::time::Instant::now() >= end {
                return Ok(json!({"error":27}));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
pub async fn run() -> Result<()> {
    let pool = crate::database::pool()?;
    let state = State {
        pool,
        host: std::env::var("ADVERTISED_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        port: std::env::var("ADVERTISED_PORT")
            .unwrap_or_else(|_| "9092".into())
            .parse()?,
    };
    state
        .db(json!({"op":"metadata"}))
        .await
        .context("extension not ready")?;
    if let Ok(address) = std::env::var("HEALTH_ADDR") {
        crate::health::serve(state.clone(), &address).await?;
    }
    let listener =
        TcpListener::bind(std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:9092".into()))
            .await?;
    eprintln!("pg-broker listening on {}", listener.local_addr()?);
    let retain = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let work = async {
                let rows = retain.db(json!({"op":"retention_scan"})).await?;
                for mut q in rows.as_array().cloned().unwrap_or_default() {
                    q["op"] = json!("retain");
                    retain.db(q).await?;
                }
                Ok::<(), anyhow::Error>(())
            };
            if let Err(e) = work.await {
                eprintln!("retention: {e:#}")
            }
        }
    });
    let slots = Arc::new(Semaphore::new(128));
    loop {
        let (stream, _) = listener.accept().await?;
        let Ok(permit) = slots.clone().try_acquire_owned() else {
            drop(stream);
            continue;
        };
        let state = state.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = connection(stream, state).await {
                eprintln!("connection: {e:#}")
            }
        });
    }
}
async fn connection(mut stream: TcpStream, state: State) -> Result<()> {
    stream.set_nodelay(true)?;
    loop {
        let len = match timeout(Duration::from_secs(60), stream.read_i32()).await? {
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        ensure!((10..=MAX_FRAME as i32).contains(&len), "frame size limit");
        let mut frame = vec![0; len as usize];
        timeout(Duration::from_secs(10), stream.read_exact(&mut frame)).await??;
        let mut r = Reader::new(&frame);
        let api = r.i16()?;
        let version = r.i16()?;
        let correlation = r.i32()?;
        r.nullable_string()?;
        if api == 18 && version == 3 {
            r.tags()?;
        }
        ensure!(
            APIS.iter()
                .any(|&(k, min, max)| k == api && version >= min && version <= max),
            "unsupported API {api} version {version}"
        );
        let response = timeout(
            Duration::from_secs(310),
            crate::handlers::handle(&state, api, version, &mut r),
        )
        .await??;
        r.done()?;
        if let Some(body) = response {
            let mut w = Writer::default();
            w.i32(correlation);
            w.0.extend(body.0);
            ensure!(w.0.len() <= MAX_FRAME, "response size limit");
            timeout(Duration::from_secs(10), async {
                stream.write_i32(w.0.len() as i32).await?;
                stream.write_all(&w.0).await
            })
            .await??;
        }
    }
}

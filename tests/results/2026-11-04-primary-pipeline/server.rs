use anyhow::{ensure, Context, Result};
use broker_protocol::{Reader, Writer, APIS, MAX_FRAME};
use deadpool_postgres::Pool;
use serde_json::{json, Value};
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    time::timeout,
};
use tokio_postgres::IsolationLevel;

// Fetch all requested partitions in one short transaction. Recursion carries the
// byte budget forward, so later partitions cannot each allocate another max-size
// response. The existing extension function remains the storage boundary.
const FETCH_MANY: &str = r#"
WITH RECURSIVE fetched(ord, used, response) AS (
    VALUES (0, 0, NULL::jsonb)
    UNION ALL
    SELECT f.ord + 1, f.used + coalesce((r.value->>'size')::int, 0), r.value
    FROM fetched f
    CROSS JOIN LATERAL (
        SELECT public.broker_call(
            ($1::jsonb->'requests'->f.ord) || jsonb_build_object(
                'op', 'fetch', 'allow_first', f.used = 0,
                'max', least(
                    ($1::jsonb->'requests'->f.ord->>'max')::int,
                    greatest(($1::jsonb->>'max')::int - f.used, 0)
                )
            )
        ) AS value
    ) r
    WHERE f.ord < jsonb_array_length($1::jsonb->'requests')
)
SELECT coalesce(jsonb_agg(response ORDER BY ord), '[]'::jsonb)
FROM fetched WHERE ord > 0
"#;

// One Kafka offset-commit request uses one short transaction. Recursive steps
// preserve wire order, including repeated partitions; broker_call still owns
// membership/generation checks and each partition's error mapping.
const COMMIT_MANY: &str = r#"
WITH RECURSIVE committed(ord, response) AS (
    VALUES (0, NULL::jsonb)
    UNION ALL
    SELECT c.ord + 1, public.broker_call($1::jsonb->'requests'->c.ord)
    FROM committed c
    WHERE c.ord < jsonb_array_length($1::jsonb->'requests')
)
SELECT coalesce(jsonb_agg(response ORDER BY ord), '[]'::jsonb)
FROM committed WHERE ord > 0
"#;

// Ordered binary arrays avoid hex/JSON expansion of record bytes. The caller
// sorts partition keys, preserving duplicate-partition order, before acquiring
// locks. All results become visible after one confirmed commit.
const APPEND_MANY: &str = r#"
WITH RECURSIVE appended(ord, response) AS (
    VALUES (0, NULL::jsonb)
    UNION ALL
    SELECT a.ord + 1, public.broker_append_bytes(
        ($1::text[])[a.ord+1], ($2::integer[])[a.ord+1], ($3::bytea[])[a.ord+1])
    FROM appended a WHERE a.ord < cardinality($1::text[])
)
SELECT coalesce(jsonb_agg(response ORDER BY ord), '[]'::jsonb)
FROM appended WHERE ord > 0
"#;

pub(crate) struct AppendBatch {
    pub topics: Vec<String>,
    pub partitions: Vec<i32>,
    pub records: Vec<Vec<u8>>,
}
enum AppendInput<'a> {
    Single(&'a [u8]),
    Many(&'a AppendBatch),
}

pub(crate) enum DatabaseResponse {
    Json(Value),
    Rows(Vec<tokio_postgres::Row>),
}

struct UnconfirmedConnection(Option<deadpool_postgres::Object>);
impl Drop for UnconfirmedConnection {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            drop(deadpool_postgres::Object::take(client));
        }
    }
}

#[derive(Clone)]
pub struct State {
    pub pool: Pool,
    pub host: String,
    pub port: i32,
    pub events: tokio::sync::broadcast::Sender<(String, i32)>,
    pub bulk_fetch: bool,
    pub sql_fetch: bool,
    pub binary_fetch: bool,
    pub binary_append: bool,
    pub bulk_commit: bool,
}
impl State {
    pub async fn db(&self, q: Value) -> Result<Value> {
        match self.execute(q).await? {
            DatabaseResponse::Json(value) => Ok(value),
            DatabaseResponse::Rows(_) => anyhow::bail!("unexpected row result"),
        }
    }

    pub async fn append(&self, topic: &str, partition: i32, data: &[u8]) -> Result<Value> {
        if data.is_empty() || data.len() > broker_protocol::MAX_BATCH {
            return Ok(json!({"error":2}));
        }
        if !self.binary_append {
            return self.db(json!({"op":"append", "topic":topic, "partition":partition, "data":hex::encode(data)})).await;
        }
        match self
            .execute_with_bytes(
                json!({"op":"append_binary", "topic":topic, "partition":partition}),
                Some(AppendInput::Single(data)),
            )
            .await?
        {
            DatabaseResponse::Json(value) => Ok(value),
            DatabaseResponse::Rows(_) => anyhow::bail!("unexpected append result"),
        }
    }

    pub(crate) async fn append_many(&self, batch: &AppendBatch) -> Result<Vec<Value>> {
        ensure!(self.binary_append, "binary append required");
        ensure!(
            batch.topics.len() == batch.partitions.len()
                && batch.topics.len() == batch.records.len()
                && batch.topics.len() <= 4096,
            "invalid append batch shape"
        );
        let response = self
            .execute_with_bytes(json!({"op":"append_many"}), Some(AppendInput::Many(batch)))
            .await?;
        let DatabaseResponse::Json(value) = response else {
            anyhow::bail!("unexpected append result");
        };
        let rows = value.as_array().context("invalid append results")?;
        ensure!(
            rows.len() == batch.topics.len(),
            "append result count mismatch"
        );
        Ok(rows.clone())
    }

    pub(crate) async fn execute(&self, q: Value) -> Result<DatabaseResponse> {
        self.execute_with_bytes(q, None).await
    }

    async fn execute_with_bytes(
        &self,
        q: Value,
        bytes: Option<AppendInput<'_>>,
    ) -> Result<DatabaseResponse> {
        let client = timeout(Duration::from_secs(3), self.pool.get()).await??;
        // Cancellation can happen outside this function (readiness probes or
        // request deadlines). An unfinished transaction must never re-enter
        // the pool, even when its peer has become a silent network blackhole.
        let mut connection = UnconfirmedConnection(Some(client));
        let work = async {
            let client = connection.0.as_mut().expect("connection is present");
            let mut builder = client.build_transaction();
            if q["op"] == "fetch" || q["op"] == "fetch_many" || q["op"] == "fetch_binary" {
                builder = builder.isolation_level(IsolationLevel::RepeatableRead);
            }
            let tx = builder.start().await?;
            let settings = if q["op"] == "fetch_binary" {
                "SET LOCAL synchronous_commit=on; SET LOCAL statement_timeout='3s'; SET LOCAL lock_timeout='2s'; SET LOCAL work_mem='16MB'; SELECT 1 / (NOT pg_catalog.pg_is_in_recovery())::int"
            } else {
                "SET LOCAL synchronous_commit=on; SET LOCAL statement_timeout='3s'; SET LOCAL lock_timeout='2s'; SELECT 1 / (NOT pg_catalog.pg_is_in_recovery())::int"
            };
            let query_work = async {
                let query = if q["op"] == "fetch_capabilities" {
                    "SELECT jsonb_build_object('sql_fetch', coalesce(has_function_privilege(to_regprocedure('public.broker_fetch_many(jsonb)'), 'EXECUTE'), false), 'binary_fetch', coalesce(has_function_privilege(to_regprocedure('public.broker_fetch_binary(jsonb)'), 'EXECUTE'), false), 'binary_append', coalesce(has_function_privilege(to_regprocedure('public.broker_append_bytes(text,integer,bytea)'), 'EXECUTE'), false), 'bulk_commit', coalesce(has_function_privilege(to_regprocedure('public.broker_commit_many(jsonb)'), 'EXECUTE'), false), 'op', $1::jsonb->>'op')"
                } else if q["op"] == "commit_many" && self.bulk_commit {
                    "SELECT public.broker_commit_many($1)"
                } else if q["op"] == "commit_many" {
                    COMMIT_MANY
                } else if q["op"] == "fetch_many" && self.sql_fetch {
                    "SELECT public.broker_fetch_many($1)"
                } else if q["op"] == "fetch_many" && !self.bulk_fetch {
                    FETCH_MANY
                } else {
                    "SELECT public.broker_call($1)"
                };
                let v = if let Some(AppendInput::Many(batch)) = bytes {
                    let statement = tx.prepare_cached(APPEND_MANY).await?;
                    DatabaseResponse::Json(
                        tx.query_one(
                            &statement,
                            &[&batch.topics, &batch.partitions, &batch.records],
                        )
                        .await?
                        .get(0),
                    )
                } else if let Some(AppendInput::Single(bytes)) = bytes {
                    let topic = q["topic"].as_str().context("missing append topic")?;
                    let partition = i32::try_from(
                        q["partition"]
                            .as_i64()
                            .context("missing append partition")?,
                    )?;
                    let statement = tx
                        .prepare_cached("SELECT public.broker_append_bytes($1,$2,$3)")
                        .await?;
                    DatabaseResponse::Json(
                        tx.query_one(&statement, &[&topic, &partition, &bytes])
                            .await?
                            .get(0),
                    )
                } else if q["op"] == "fetch_binary" {
                    let statement = tx.prepare_cached("SELECT * FROM public.broker_fetch_binary($1) ORDER BY request_index, base_offset NULLS FIRST").await?;
                    DatabaseResponse::Rows(tx.query(&statement, &[&q]).await?)
                } else {
                    // Only fixed SQL strings enter this per-connection cache. Request
                    // values stay bound parameters; discarded connections discard
                    // their plans too, including after cancellation or failover.
                    let statement = tx.prepare_cached(query).await?;
                    DatabaseResponse::Json(tx.query_one(&statement, &[&q]).await?.get(0))
                };
                Ok::<DatabaseResponse, anyhow::Error>(v)
            };
            // The pinned driver's batch_execute enqueues its request before its
            // first await. Poll settings first so PostgreSQL applies them before
            // preparing/executing the query, without a client round-trip wait.
            // Keep commit outside the pipeline and discard on either error.
            let (_, v) = tokio::try_join!(biased;
                async { tx.batch_execute(settings).await.context("transaction settings failed") },
                query_work,
            )?;
            // Server-side cancellation of SyncRep can return only a WARNING
            // after local commit. Bound confirmation at the gateway instead.
            tx.batch_execute("SET LOCAL statement_timeout=0").await?;
            timeout(Duration::from_secs(5), tx.commit())
                .await
                .context("commit confirmation timed out; outcome unknown")??;
            Ok::<DatabaseResponse, anyhow::Error>(v)
        };
        let result = timeout(Duration::from_secs(10), work)
            .await
            .context("database transaction timed out; outcome unknown")??;
        // Only fully confirmed work returns a connection to the pool.
        drop(connection.0.take());
        Ok(result)
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
    let (events, _) = tokio::sync::broadcast::channel(1024);
    tokio::spawn(crate::database::notifications(events.clone()));
    let mut state = State {
        pool,
        events,
        bulk_fetch: false,
        sql_fetch: false,
        binary_fetch: false,
        binary_append: false,
        bulk_commit: false,
        host: std::env::var("ADVERTISED_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        port: std::env::var("ADVERTISED_PORT")
            .unwrap_or_else(|_| "9092".into())
            .parse()?,
    };
    let metadata = state
        .db(json!({"op":"metadata"}))
        .await
        .context("extension not ready")?;
    // Older extension versions retain the bounded gateway-side fetch path.
    // This capability is durable extension metadata, not a Kafka API claim.
    state.bulk_fetch = metadata["fetch_many"] == true;
    if state.bulk_fetch {
        let capabilities = state.db(json!({"op":"fetch_capabilities"})).await?;
        state.sql_fetch = capabilities["sql_fetch"] == true;
        state.binary_fetch = capabilities["binary_fetch"] == true;
        state.binary_append = capabilities["binary_append"] == true;
        state.bulk_commit = capabilities["bulk_commit"] == true;
    }
    if let Ok(address) = std::env::var("HEALTH_ADDR") {
        crate::health::serve(state.clone(), &address).await?;
    }
    let listener =
        TcpListener::bind(std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:9092".into()))
            .await?;
    eprintln!("pg-broker listening on {}", listener.local_addr()?);
    let retain = state.clone();
    let trace_retention = std::env::var("PG_BROKER_TRACE_RETENTION").as_deref() == Ok("1");
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let started = trace_retention.then(|| {
                eprintln!("unix_ms={} retention_start", unix_millis());
                Instant::now()
            });
            let mut completed_partitions = 0;
            let work = async {
                let rows = retain.db(json!({"op":"retention_scan"})).await?;
                for mut q in rows.as_array().cloned().unwrap_or_default() {
                    q["op"] = json!("retain");
                    retain.db(q).await?;
                    completed_partitions += 1;
                }
                Ok::<(), anyhow::Error>(())
            };
            let result = work.await;
            if let Some(started) = started {
                eprintln!(
                    "unix_ms={} retention_end elapsed_us={} completed_partitions={} success={}",
                    unix_millis(),
                    started.elapsed().as_micros(),
                    completed_partitions,
                    result.is_ok()
                );
            }
            if let Err(e) = result {
                eprintln!("unix_ms={} retention: {e:#}", unix_millis())
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
                eprintln!("unix_ms={} connection: {e:#}", unix_millis())
            }
        });
    }
}
fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
async fn connection(mut stream: TcpStream, state: State) -> Result<()> {
    stream.set_nodelay(true)?;
    loop {
        let len = match timeout(Duration::from_secs(60), stream.read_i32())
            .await
            .context("frame-length read exceeded 60s (idle or incomplete header)")?
        {
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        ensure!((10..=MAX_FRAME as i32).contains(&len), "frame size limit");
        let mut frame = vec![0; len as usize];
        timeout(Duration::from_secs(10), stream.read_exact(&mut frame))
            .await
            .context("frame-payload read exceeded 10s")??;
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
        .await
        .with_context(|| {
            format!("request exceeded 310s: api={api} version={version} correlation={correlation}")
        })?
        .with_context(|| {
            format!("request failed: api={api} version={version} correlation={correlation}")
        })?;
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
            .await
            .with_context(|| format!("response write exceeded 10s: api={api} version={version} correlation={correlation}"))??;
        }
    }
}

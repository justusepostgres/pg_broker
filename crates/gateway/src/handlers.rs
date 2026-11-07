use crate::server::State;
use anyhow::{ensure, Result};
use broker_protocol::{Reader, Writer, APIS};
use serde_json::{json, Value};
fn error(v: &Value) -> i16 {
    v["error"].as_i64().unwrap_or(0) as i16
}
fn num(v: &Value, k: &str, default: i64) -> i64 {
    v[k].as_i64().unwrap_or(default)
}
fn text<'a>(v: &'a Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or("")
}
fn broker(w: &mut Writer, s: &State) {
    w.i32(1);
    w.string(&s.host);
    w.i32(s.port)
}
pub async fn handle(
    s: &State,
    api: i16,
    version: i16,
    r: &mut Reader<'_>,
) -> Result<Option<Writer>> {
    let mut w = Writer::default();
    match api {
        18 => {
            if version == 3 {
                r.compact_string()?;
                r.compact_string()?;
                r.tags()?;
            }
            w.i16(0);
            if version == 3 {
                w.unsigned_varint(APIS.len() as u32 + 1);
            } else {
                w.array(APIS.len());
            }
            for &(key, min, max) in APIS {
                w.i16(key);
                w.i16(min);
                w.i16(max);
                if version == 3 {
                    w.unsigned_varint(0);
                }
            }
            if version >= 1 {
                w.i32(0);
            }
            if version == 3 {
                w.unsigned_varint(0);
            }
        }
        3 => metadata(s, r, &mut w, version).await?,
        19 => {
            let n = r.count()?;
            let mut results = vec![];
            for _ in 0..n {
                let topic = r.string()?;
                let partitions = r.i32()?;
                let replication = r.i16()?;
                let assignments = r.count()?;
                for _ in 0..assignments {
                    r.i32()?;
                    for _ in 0..r.count()? {
                        r.i32()?;
                    }
                }
                let configs = r.count()?;
                for _ in 0..configs {
                    r.string()?;
                    r.nullable_string()?;
                }
                let v = if replication != 1 {
                    json!({"error":38})
                } else if assignments > 0 || configs > 0 {
                    json!({"error":40})
                } else {
                    s.db(json!({"op":"create","topic":topic,"partitions":partitions}))
                        .await?
                };
                results.push((topic, error(&v)));
            }
            r.i32()?;
            w.array(n);
            for (t, e) in results {
                w.string(&t);
                w.i16(e)
            }
        }
        20 => {
            let mut topics = vec![];
            for _ in 0..r.count()? {
                topics.push(r.string()?)
            }
            r.i32()?;
            w.array(topics.len());
            for t in topics {
                let v = s.db(json!({"op":"delete","topic":t})).await?;
                w.string(&t);
                w.i16(error(&v))
            }
        }
        0 => return crate::produce::handle(s, r).await,
        1 => fetch(s, r, &mut w).await?,
        2 => {
            ensure!(r.i32()? == -1, "replica fetch unsupported");
            let n = r.count()?;
            w.array(n);
            for _ in 0..n {
                let t = r.string()?;
                w.string(&t);
                let n = r.count()?;
                w.array(n);
                for _ in 0..n {
                    let p = r.i32()?;
                    let ts = r.i64()?;
                    let v = s
                        .db(json!({"op":"lookup","topic":t,"partition":p,"timestamp":ts}))
                        .await?;
                    w.i32(p);
                    w.i16(error(&v));
                    w.i64(num(&v, "timestamp", -1));
                    w.i64(num(&v, "offset", -1));
                }
            }
        }
        10 => {
            r.string()?;
            w.i16(0);
            broker(&mut w, s)
        }
        8 => {
            w = crate::offsets::commit(s, r).await?;
        }
        9 => {
            let g = r.string()?;
            let n = r.count()?;
            w.array(n);
            for _ in 0..n {
                let t = r.string()?;
                w.string(&t);
                let n = r.count()?;
                w.array(n);
                for _ in 0..n {
                    let p = r.i32()?;
                    let v = s
                        .db(json!({"op":"offset_fetch","group":g,"topic":t,"partition":p}))
                        .await?;
                    w.i32(p);
                    w.i64(num(&v, "offset", -1));
                    w.string(text(&v, "metadata"));
                    w.i16(error(&v));
                }
            }
        }
        11 => join(s, r, &mut w, version).await?,
        12 => {
            if version >= 1 {
                w.i32(0)
            }
            let g = r.string()?;
            let generation = r.i32()?;
            let member = r.string()?;
            let v = s
                .db(json!({"op":"heartbeat","group":g,"generation":generation,"member":member}))
                .await?;
            w.i16(error(&v))
        }
        13 => {
            if version >= 1 {
                w.i32(0)
            }
            let g = r.string()?;
            let member = r.string()?;
            let v = s
                .db(json!({"op":"leave","group":g,"member":member}))
                .await?;
            w.i16(error(&v))
        }
        14 => {
            if version >= 1 {
                w.i32(0)
            }
            let g = r.string()?;
            let generation = r.i32()?;
            let member = r.string()?;
            let mut assignments = vec![];
            let mut owners = std::collections::HashSet::new();
            let mut valid = true;
            for _ in 0..r.count()? {
                let m = r.string()?;
                let a = r.bytes()?;
                let partitions = validate_assignment(&a)?;
                for (t, p) in &partitions {
                    if !owners.insert((t.clone(), *p)) {
                        valid = false
                    }
                }
                assignments
                    .push(json!({"member":m,"assignment":hex::encode(a),"owned":partitions}));
            }
            if !valid {
                w.i16(23);
                w.bytes(&[]);
                return Ok(Some(w));
            }
            let v=s.poll(json!({"op":"sync","group":g,"generation":generation,"member":member,"assignments":assignments})).await?;
            w.i16(error(&v));
            w.bytes(&hex::decode(text(&v, "assignment"))?);
        }
        _ => unreachable!(),
    }
    Ok(Some(w))
}
async fn metadata(s: &State, r: &mut Reader<'_>, w: &mut Writer, version: i16) -> Result<()> {
    let count = r.nullable_count()?;
    let mut requested = vec![];
    if let Some(n) = count {
        for _ in 0..n {
            requested.push(r.string()?);
        }
    }
    if version >= 4 {
        r.i8()?;
    }
    let all = s.db(json!({"op":"metadata"})).await?;
    let all = all["topics"].as_array().unwrap();
    let topics: Vec<Value> = if count.is_none() || (version == 0 && requested.is_empty()) {
        all.clone()
    } else {
        requested
            .iter()
            .map(|t| {
                all.iter()
                    .find(|a| a["name"] == *t)
                    .cloned()
                    .unwrap_or(json!({"name":t,"error":3,"partitions":0}))
            })
            .collect()
    };
    if version >= 3 {
        w.i32(0)
    }
    w.array(1);
    broker(w, s);
    if version >= 1 {
        w.null_string();
    }
    if version >= 2 {
        w.string("pg-broker")
    }
    if version >= 1 {
        w.i32(1)
    }
    w.array(topics.len());
    for t in topics {
        w.i16(error(&t));
        w.string(text(&t, "name"));
        if version >= 1 {
            w.i8(0)
        }
        let n = num(&t, "partitions", 0) as usize;
        w.array(n);
        for p in 0..n {
            w.i16(0);
            w.i32(p as i32);
            w.i32(1);
            w.array(1);
            w.i32(1);
            w.array(1);
            w.i32(1);
        }
    }
    Ok(())
}
async fn fetch(s: &State, r: &mut Reader<'_>, w: &mut Writer) -> Result<()> {
    ensure!(r.i32()? == -1, "replication unsupported");
    let wait = r.i32()?.clamp(0, 5000) as u64;
    let min = r.i32()?.clamp(0, 4194304);
    let max = r.i32()?.clamp(1, 4194304);
    // Kafka transactions are rejected on produce, so all visible batches are committed.
    ensure!((0..=1).contains(&r.i8()?), "invalid isolation level");
    let mut topics = vec![];
    let mut requests = vec![];
    for _ in 0..r.count()? {
        let t = r.string()?;
        let mut ps = vec![];
        for _ in 0..r.count()? {
            let p = r.i32()?;
            let off = r.i64()?;
            let limit = r.i32()?.clamp(1, 1048576);
            ensure!(requests.len() < 4096, "fetch partition count limit");
            requests.push(json!({"topic":t,"partition":p,"offset":off,"max":limit}));
            ps.push(p);
        }
        topics.push((t, ps));
    }
    let end = tokio::time::Instant::now() + std::time::Duration::from_millis(wait);
    let requested: std::collections::HashSet<_> = topics
        .iter()
        .flat_map(|(topic, partitions)| partitions.iter().map(|p| (topic.clone(), *p)))
        .collect();
    loop {
        // Subscribe before the snapshot: a commit during the SQL call must not
        // be lost between checking the log and starting the wait.
        let mut events = s.events.subscribe();
        let fetched = s.fetch_partitions(&requests, max).await?;
        let mut rows = fetched.iter();
        let mut total = 0;
        let mut failed = false;
        let mut body = Writer::default();
        body.i32(0);
        body.array(topics.len());
        for (t, ps) in &topics {
            body.string(t);
            body.array(ps.len());
            for &p in ps {
                let v = rows.next().expect("fetch result count checked");
                failed |= v.error != 0;
                body.i32(p);
                body.i16(v.error);
                body.i64(v.high);
                body.i64(v.high);
                body.i32(-1);
                total += v.data.len() as i32;
                body.bytes(&v.data);
            }
        }
        if failed || total >= min || tokio::time::Instant::now() >= end {
            *w = body;
            return Ok(());
        }
        let changed = async {
            loop {
                match events.recv().await {
                    Ok(partition) if requested.contains(&partition) => break,
                    Ok(_) => {}
                    Err(_) => break, // Lagged notifications require a fresh read.
                }
            }
        };
        tokio::select! {
            _ = changed => {},
            _ = tokio::time::sleep_until(end.min(tokio::time::Instant::now()
                + std::time::Duration::from_millis(25))) => {},
        }
    }
}
async fn join(s: &State, r: &mut Reader<'_>, w: &mut Writer, version: i16) -> Result<()> {
    let group = r.string()?;
    let session = r.i32()?;
    let rebalance = if version >= 1 {
        r.i32()?.clamp(1000, 300000)
    } else {
        session
    };
    let mut member = r.string()?;
    let new = member.is_empty();
    if new {
        member = uuid::Uuid::new_v4().to_string()
    };
    let kind = r.string()?;
    let mut metadata = None;
    for _ in 0..r.count()? {
        let name = r.string()?;
        let bytes = r.bytes()?;
        ensure!(bytes.len() <= 16384, "membership metadata limit");
        if name == "range" {
            metadata = Some(bytes)
        }
    }
    let v = if let Some(meta) = metadata.filter(|_| kind == "consumer") {
        let subscriptions = subscription(&meta)?;
        let v=s.db(json!({"op":"join","group":group,"member":member,"new":new,"session_ms":session,"rebalance_ms":rebalance,"metadata":hex::encode(meta),"subscriptions":subscriptions})).await?;
        if error(&v) != 0 {
            v
        } else {
            s.poll(json!({"op":"join_poll","group":group,"member":member,"generation":v["generation"]})).await?
        }
    } else {
        json!({"error":23})
    };
    if version >= 2 {
        w.i32(0)
    }
    w.i16(error(&v));
    w.i32(num(&v, "generation", -1) as i32);
    w.string("range");
    w.string(text(&v, "leader"));
    w.string(&member);
    if text(&v, "leader") == member {
        let members = v["members"].as_array().unwrap();
        w.array(members.len());
        for m in members {
            w.string(text(m, "member"));
            w.bytes(&hex::decode(text(m, "metadata"))?);
        }
    } else {
        w.array(0)
    }
    Ok(())
}
fn validate_assignment(a: &[u8]) -> Result<Vec<(String, i32)>> {
    let mut r = Reader::new(a);
    ensure!((0..=3).contains(&r.i16()?), "assignment version");
    let mut partitions = vec![];
    for _ in 0..r.count()? {
        let topic = r.string()?;
        for _ in 0..r.count()? {
            let p = r.i32()?;
            ensure!(p >= 0, "partition");
            partitions.push((topic.clone(), p));
        }
    }
    r.bytes()?;
    r.done()?;
    Ok(partitions)
}

fn subscription(data: &[u8]) -> Result<Vec<String>> {
    let mut r = Reader::new(data);
    let version = r.i16()?;
    ensure!((0..=3).contains(&version), "subscription version");
    let mut topics = vec![];
    for _ in 0..r.count()? {
        topics.push(r.string()?)
    }
    r.bytes()?;
    if version >= 1 {
        for _ in 0..r.count()? {
            r.string()?;
            for _ in 0..r.count()? {
                ensure!(r.i32()? >= 0, "owned partition");
            }
        }
    }
    if version >= 2 {
        r.i32()?;
    }
    if version >= 3 {
        r.nullable_string()?;
    }
    r.done()?;
    Ok(topics)
}

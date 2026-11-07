use pgrx::prelude::*;
use pgrx::JsonB;

pgrx::pg_module_magic!();

// Validate the complete record set before the first write. Bytea parameters
// avoid encoding record bytes as hex/JSON at either side of SPI. All batches
// remain in the caller's single transaction, with its partition lock held until
// commit. The helper is private so SQL callers cannot supply invented counts.
#[pg_extern]
fn broker_append_bytes(topic: &str, partition: i32, data: &[u8]) -> JsonB {
    let Ok(batches) = broker_protocol::batches(data) else {
        return JsonB(serde_json::json!({"error": 2}));
    };
    let mut first = None;
    for (bytes, timestamps) in batches {
        let count = timestamps.len() as i32;
        let maximum = *timestamps.iter().max().expect("validated nonempty batch");
        let timestamps = JsonB(serde_json::json!(timestamps));
        let result = Spi::get_one_with_args::<JsonB>(
            "SELECT broker.append_bytes($1,$2,$3,$4,$5,$6)",
            &[
                topic.into(),
                partition.into(),
                bytes.into(),
                count.into(),
                maximum.into(),
                timestamps.into(),
            ],
        )
        .expect("append failed")
        .expect("append returned NULL");
        if result.0["error"] != 0 {
            if first.is_some() {
                error!("partition changed during a multi-batch append");
            }
            return result;
        }
        if first.is_none() {
            first = Some(result);
        }
    }
    first.expect("validated nonempty record set")
}

// All storage functions are extension-owned SQL. SPI preserves the caller's
// transaction; no backend pointers, custom WAL, hooks, or unsafe Rust are needed.
#[pg_extern]
fn broker_call(mut request: JsonB) -> JsonB {
    // Validate at the storage boundary too: callers cannot supply false counts
    // or timestamps and thereby corrupt the offset index.
    if request.0["op"] == "append" {
        let raw = request.0["data"].as_str().unwrap_or("");
        let decoded = hex::decode(raw).unwrap_or_default();
        let Ok(batches) = broker_protocol::batches(&decoded) else {
            return JsonB(serde_json::json!({"error": 2}));
        };
        // dispatch consumes the validated batches, not the original record set.
        // Keeping both would serialize the entire payload twice through SPI.
        request
            .0
            .as_object_mut()
            .expect("append request is an object")
            .remove("data");
        request.0["batches"] = serde_json::Value::Array(
            batches
                .into_iter()
                .map(|(bytes, ts)| {
                    serde_json::json!({"data":hex::encode(bytes), "count":ts.len(),
                "timestamp":ts.iter().max(), "timestamps":ts})
                })
                .collect(),
        );
    }
    Spi::get_one_with_args::<JsonB>("SELECT broker.dispatch($1)", &[request.into()])
        .expect("broker operation failed")
        .expect("dispatch returned NULL")
}

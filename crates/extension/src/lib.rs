use pgrx::prelude::*;
use pgrx::JsonB;

pgrx::pg_module_magic!();

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

//! Separate bounded HTTP probes; Kafka socket acceptance is not DB readiness.
use crate::server::State;
use anyhow::Result;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Semaphore,
    time::timeout,
};

pub async fn serve(state: State, address: &str) -> Result<()> {
    let listener = TcpListener::bind(address).await?;
    let slots = Arc::new(Semaphore::new(8));
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let Ok(permit) = slots.clone().try_acquire_owned() else {
                continue;
            };
            let state = state.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let work = async {
                    let mut request = [0u8; 1024];
                    let n = stream.read(&mut request).await?;
                    let path = std::str::from_utf8(&request[..n]).unwrap_or("");
                    let status = if path.starts_with("GET /live ") {
                        "200 OK"
                    } else if path.starts_with("GET /ready ") {
                        if matches!(
                            timeout(Duration::from_secs(4), state.db(json!({"op":"metadata"})))
                                .await,
                            Ok(Ok(_))
                        ) {
                            "200 OK"
                        } else {
                            "503 Service Unavailable"
                        }
                    } else {
                        "404 Not Found"
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    );
                    stream.write_all(response.as_bytes()).await
                };
                let _ = timeout(Duration::from_secs(5), work).await;
            });
        }
    });
    Ok(())
}

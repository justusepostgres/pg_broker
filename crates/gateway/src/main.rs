mod database;
mod handlers;
mod health;
mod server;
use anyhow::Result;
#[tokio::main]
async fn main() -> Result<()> {
    server::run().await
}

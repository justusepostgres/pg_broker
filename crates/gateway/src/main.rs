mod database;
mod fetch;
mod handlers;
mod health;
mod offsets;
mod produce;
mod server;
use anyhow::Result;
#[tokio::main]
async fn main() -> Result<()> {
    server::run().await
}

use anyhow::{Context, Result};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::{env, time::Duration};
use tokio_postgres::{
    config::{SslMode, TargetSessionAttrs},
    Config,
};

/// Accept CNPG's generated application-secret fields without concatenating
/// credentials into a connection string. TLS always validates CA and hostname.
fn connection_config() -> Result<(Config, MakeTlsConnector)> {
    let mut config: Config = if let Ok(url) = env::var("DATABASE_URL") {
        url.parse().context("invalid DATABASE_URL")?
    } else if let Ok(host) = env::var("PGHOST") {
        let mut c = Config::new();
        c.host(&host)
            .port(
                env::var("PGPORT")
                    .unwrap_or_else(|_| "5432".into())
                    .parse()?,
            )
            .user(&env::var("PGUSER").context("PGUSER required with PGHOST")?)
            .password(env::var("PGPASSWORD").context("PGPASSWORD required with PGHOST")?)
            .dbname(&env::var("PGDATABASE").context("PGDATABASE required with PGHOST")?);
        c
    } else {
        "host=127.0.0.1 port=55432 user=postgres dbname=postgres sslmode=disable".parse()?
    };
    config.connect_timeout(Duration::from_secs(3));
    config.target_session_attrs(TargetSessionAttrs::ReadWrite);
    let mut tls = TlsConnector::builder();
    if let Ok(path) = env::var("PGSSLROOTCERT") {
        tls.add_root_certificate(Certificate::from_pem(&std::fs::read(path)?)?);
        config.ssl_mode(SslMode::Require);
    }
    Ok((config, MakeTlsConnector::new(tls.build()?)))
}

pub fn pool() -> Result<Pool> {
    let (config, tls) = connection_config()?;
    let manager = Manager::from_config(
        config,
        tls,
        ManagerConfig {
            // State checks primary status inside every transaction's settings
            // exchange. Avoid a separate checkout round trip; unfinished work
            // still discards its physical connection rather than recycling it.
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Ok(Pool::builder(manager).max_size(16).build()?)
}

/// Notifications only shorten polling; PostgreSQL reads remain authoritative.
/// One dedicated connection avoids leaving LISTEN state on pooled connections.
pub async fn notifications(events: tokio::sync::broadcast::Sender<(String, i32)>) {
    loop {
        if let Err(error) = listen_once(&events).await {
            eprintln!("notification listener: {error:#}");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn listen_once(events: &tokio::sync::broadcast::Sender<(String, i32)>) -> Result<()> {
    let (mut config, tls) = connection_config()?;
    config.application_name("pg-broker-notifications");
    let (client, mut connection) =
        tokio::time::timeout(Duration::from_secs(5), config.connect(tls)).await??;
    let receive = async {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(tokio_postgres::AsyncMessage::Notification(notification))) => {
                    if notification.channel() == "pg_broker_records" {
                        if let Some((topic, partition)) = notification.payload().rsplit_once(':') {
                            if topic.len() <= 249 {
                                if let Ok(partition) = partition.parse::<i32>() {
                                    let _ = events.send((topic.to_owned(), partition));
                                }
                            }
                        }
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err::<(), anyhow::Error>(error.into()),
                None => anyhow::bail!("notification connection closed"),
            }
        }
    };
    let maintain = async {
        tokio::time::timeout(
            Duration::from_secs(5),
            client.batch_execute("LISTEN pg_broker_records"),
        )
        .await??;
        while !client.is_closed() {
            tokio::time::sleep(Duration::from_secs(15)).await;
            // Detect a blackhole or a demoted primary, then reconnect. Fetch
            // polling continues independently throughout listener failures.
            tokio::time::timeout(
                Duration::from_secs(5),
                client.simple_query("SELECT 1 / (NOT pg_is_in_recovery())::int"),
            )
            .await??;
        }
        Err::<(), anyhow::Error>(anyhow::anyhow!("notification connection closed"))
    };
    tokio::try_join!(receive, maintain)?;
    Ok(())
}

/// The prepared query must be enqueued before disabling its statement timeout.
/// Preparation happens at the caller before this pipeline, including cold plans.
/// Both responses must succeed before the caller sends COMMIT. Errors or
/// cancellation leave the caller's unconfirmed-connection guard responsible for
/// discarding the physical connection.
pub async fn query(
    tx: &deadpool_postgres::Transaction<'_>,
    statement: &tokio_postgres::Statement,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<Vec<tokio_postgres::Row>> {
    let (rows, _) = tokio::try_join!(biased;
        tx.query(statement, params),
        tx.batch_execute("SET LOCAL statement_timeout=0"),
    )?;
    Ok(rows)
}

pub async fn query_one(
    tx: &deadpool_postgres::Transaction<'_>,
    statement: &tokio_postgres::Statement,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<tokio_postgres::Row> {
    let mut rows = query(tx, statement, params).await?;
    anyhow::ensure!(rows.len() == 1, "expected exactly one database row");
    Ok(rows.remove(0))
}

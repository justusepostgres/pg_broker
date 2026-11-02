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
pub fn pool() -> Result<Pool> {
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
    let manager = Manager::from_config(
        config,
        MakeTlsConnector::new(tls.build()?),
        ManagerConfig {
            // Discard a pooled connection if its old primary has become a standby.
            recycling_method: RecyclingMethod::Custom(
                "SELECT 1 / (NOT pg_is_in_recovery())::int".into(),
            ),
        },
    );
    Ok(Pool::builder(manager).max_size(16).build()?)
}

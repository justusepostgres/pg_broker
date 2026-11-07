//! Durable file staging for replica bootstrap; does not install PostgreSQL state.
//! Use a trusted, private, per-transfer directory on a filesystem that supports
//! hard links and fsync. Callers bound concurrent transfers and set IO deadlines.
use crate::{snapshot::SnapshotCapture, snapshot_stream::SnapshotStream};
use anyhow::{ensure, Context, Result};
use std::path::Path;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
};

/// Validate a staged file after a crash/restart before attempting installation.
/// No table mutations occur. Authentication and manifest authorization belong
/// to the installer. Concurrent modification of files in this directory is forbidden.
pub async fn validate_snapshot_file(
    path: &Path,
    expected_id: &str,
    earliest: i64,
    max_bytes: u64,
) -> Result<SnapshotCapture> {
    let file = File::open(path).await?;
    ensure!(
        file.metadata().await?.len() <= max_bytes,
        "snapshot disk quota exceeded"
    );
    let mut reader = SnapshotStream::new(file, expected_id.into(), earliest, max_bytes).await?;
    let mut capture = None;
    while let Some(page) = reader.next_page().await? {
        capture = Some(page.snapshot);
    }
    let mut file = reader.finish()?;
    let mut extra = [0];
    ensure!(
        file.read(&mut extra).await? == 0,
        "trailing snapshot file bytes"
    );
    capture.context("snapshot has no manifest")
}

/// Receive an EOF-delimited image into `incoming.partial`, verify, sync, then
/// atomically publish `snapshot.bin` without overwriting any existing image.
/// Memory is bounded by a 64 KiB copy buffer plus one validated snapshot page.
///
/// Failed/cancelled downloads remain partial for explicit recovery/cleanup;
/// they are never installable merely because a file exists. A crash after
/// publication may leave both hard links. On recovery validate snapshot.bin;
/// a fully received partial may be completed with `publish_snapshot_file`.
/// A returned error after publication is ambiguous: inspect and validate the
/// existing image instead of replaying or overwriting the destination.
pub async fn receive_snapshot_file<R: AsyncRead + Unpin>(
    mut source: R,
    directory: &Path,
    expected_id: &str,
    earliest: i64,
    max_bytes: u64,
) -> Result<SnapshotCapture> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join("incoming.partial"))
        .await?;
    let mut remaining = max_bytes;
    let mut buffer = [0; 64 * 1024];
    loop {
        // Probe at most one byte beyond the limit, without persisting that byte.
        let wanted = remaining.saturating_add(1).min(buffer.len() as u64) as usize;
        let count = source.read(&mut buffer[..wanted]).await?;
        if count == 0 {
            break;
        }
        ensure!(count as u64 <= remaining, "snapshot disk quota exceeded");
        file.write_all(&buffer[..count]).await?;
        remaining -= count as u64;
    }
    file.flush().await?;
    drop(file);
    publish_snapshot_file(directory, expected_id, earliest, max_bytes).await
}

/// Retry publication of an already fully received partial after restart. This
/// always revalidates contents and syncs before linking; corrupt/truncated input
/// never becomes snapshot.bin. Existing destinations are never replaced.
pub async fn publish_snapshot_file(
    directory: &Path,
    expected_id: &str,
    earliest: i64,
    max_bytes: u64,
) -> Result<SnapshotCapture> {
    let partial = directory.join("incoming.partial");
    let capture = validate_snapshot_file(&partial, expected_id, earliest, max_bytes).await?;
    OpenOptions::new()
        .write(true)
        .open(&partial)
        .await?
        .sync_all()
        .await?;
    fs::hard_link(&partial, directory.join("snapshot.bin")).await?;
    File::open(directory).await?.sync_all().await?;
    fs::remove_file(&partial).await?;
    File::open(directory).await?.sync_all().await?;
    Ok(capture)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    struct Directory(std::path::PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "pg-broker-snapshot-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn image() -> Vec<u8> {
        let page = serde_json::json!({"snapshot":{"snapshot_id":"id","group_key":"g","manifest":{},"topic":"t","partition":0,"earliest":4,"end_offset":4,"cursor_offset":4,"state":"complete"},"batches":[],"next_offset":4,"eof":true});
        let page = serde_json::to_vec(&page).unwrap();
        let mut image = b"PGBRSN01".to_vec();
        image.extend_from_slice(&(page.len() as u32).to_be_bytes());
        image.extend(page);
        let digest = Sha256::digest(&image);
        image.extend_from_slice(&digest);
        image
    }
    #[tokio::test]
    async fn publish_round_trip_and_reopen_without_overwrite() {
        let dir = Directory::new();
        let image = image();
        let capture = receive_snapshot_file(&image[..], &dir.0, "id", 4, image.len() as u64)
            .await
            .unwrap();
        assert_eq!(capture.earliest, 4);
        assert!(!dir.0.join("incoming.partial").exists());
        assert_eq!(fs::read(dir.0.join("snapshot.bin")).await.unwrap(), image);
        validate_snapshot_file(&dir.0.join("snapshot.bin"), "id", 4, 10000)
            .await
            .unwrap();
        assert!(receive_snapshot_file(&image[..], &dir.0, "id", 4, 10000)
            .await
            .is_err());
        assert_eq!(fs::read(dir.0.join("snapshot.bin")).await.unwrap(), image);
    }
    #[tokio::test]
    async fn rejects_bad_checksum_truncation_trailing_bytes_and_quota() {
        let image = image();
        let mut corrupt = image.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        let mut extra = image.clone();
        extra.push(0);
        for bytes in [corrupt, extra, image[..image.len() - 1].to_vec()] {
            let dir = Directory::new();
            assert!(receive_snapshot_file(&bytes[..], &dir.0, "id", 4, 10000)
                .await
                .is_err());
            assert!(!dir.0.join("snapshot.bin").exists());
        }
        let dir = Directory::new();
        assert!(receive_snapshot_file(&image[..], &dir.0, "id", 4, 20)
            .await
            .is_err());
        assert!(
            fs::metadata(dir.0.join("incoming.partial"))
                .await
                .unwrap()
                .len()
                <= 20
        );
        assert!(!dir.0.join("snapshot.bin").exists());
    }
    #[tokio::test]
    async fn restart_revalidates_partial_before_publication() {
        let dir = Directory::new();
        let image = image();
        fs::write(dir.0.join("incoming.partial"), &image[..20])
            .await
            .unwrap();
        assert!(publish_snapshot_file(&dir.0, "id", 4, 10000).await.is_err());
        assert!(!dir.0.join("snapshot.bin").exists());
        fs::write(dir.0.join("incoming.partial"), &image)
            .await
            .unwrap();
        publish_snapshot_file(&dir.0, "id", 4, 10000).await.unwrap();
        validate_snapshot_file(&dir.0.join("snapshot.bin"), "id", 4, 10000)
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn cancellation_never_publishes_partial() {
        let dir = Directory::new();
        let (mut tx, rx) = tokio::io::duplex(64);
        tx.write_all(b"PGBRSN01").await.unwrap();
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(20),
            receive_snapshot_file(rx, &dir.0, "id", 4, 10000)
        )
        .await
        .is_err());
        assert!(!dir.0.join("snapshot.bin").exists());
        assert!(publish_snapshot_file(&dir.0, "id", 4, 10000).await.is_err());
    }
}

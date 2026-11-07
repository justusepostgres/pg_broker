//! Versioned snapshot transfer framing. Received pages are provisional until
//! `next_page` returns None: stage them outside live state, then atomically install.
//! This module does not implement installation, authentication, or Raft transport.
use crate::{application::PartitionApplication, snapshot::SnapshotPage};
use anyhow::{ensure, Context, Result};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAGIC: &[u8; 8] = b"PGBRSN01";
const MAX_FRAME: usize = 8 * 1024 * 1024;

/// Export one completed image with bounded memory. Each database transaction
/// closes before writing to the sink. Caller must prevent snapshot release,
/// set an IO deadline, and discard partial output on cancellation/error.
/// `max_bytes` bounds the entire stream, including framing and checksum.
pub async fn export_snapshot<W: AsyncWrite + Unpin>(
    app: &PartitionApplication,
    snapshot_id: &str,
    earliest: i64,
    sink: &mut W,
    max_bytes: u64,
) -> Result<[u8; 32]> {
    let mut budget = Budget(max_bytes);
    budget.take(8)?;
    sink.write_all(MAGIC).await?;
    let mut hash = Sha256::new();
    hash.update(MAGIC);
    let mut cursor = earliest;
    let mut identity = None;
    loop {
        let page = app.read_snapshot(snapshot_id, cursor).await?;
        check_identity(&page, &mut identity, earliest)?;
        let bytes = serde_json::to_vec(&page)?;
        ensure!(bytes.len() <= MAX_FRAME, "snapshot frame exceeds limit");
        budget.take(4 + bytes.len() as u64)?;
        let length = (bytes.len() as u32).to_be_bytes();
        hash.update(length);
        hash.update(&bytes);
        sink.write_all(&length).await?;
        sink.write_all(&bytes).await?;
        cursor = page.next_offset;
        if page.eof {
            break;
        }
    }
    budget.take(32)?;
    let digest: [u8; 32] = hash.finalize().into();
    sink.write_all(&digest).await?;
    sink.flush().await?;
    Ok(digest)
}

struct Budget(u64);
impl Budget {
    fn take(&mut self, bytes: u64) -> Result<()> {
        self.0 = self
            .0
            .checked_sub(bytes)
            .context("snapshot byte budget exceeded")?;
        Ok(())
    }
}

fn check_identity(
    page: &SnapshotPage,
    identity: &mut Option<serde_json::Value>,
    earliest: i64,
) -> Result<()> {
    ensure!(
        page.snapshot.earliest == earliest
            && earliest >= 0
            && page.snapshot.cursor_offset == page.snapshot.end_offset,
        "snapshot capture bounds mismatch"
    );
    let value = serde_json::to_value(&page.snapshot)?;
    if let Some(previous) = identity {
        ensure!(
            *previous == value,
            "snapshot manifest changed during transfer"
        );
    } else {
        *identity = Some(value);
    }
    Ok(())
}

/// Bounded reader for exactly one framed image. A trailing stream belongs to the
/// transport, not this image. SHA-256 detects corruption, not a malicious peer;
/// the installer must authorize topic UUID, group, applied marker and membership.
/// Any read error poisons this reader. Caller supplies timeout and staging quota.
pub struct SnapshotStream<R> {
    source: R,
    hash: Sha256,
    budget: Budget,
    id: String,
    earliest: i64,
    cursor: i64,
    identity: Option<serde_json::Value>,
    done: bool,
    poisoned: bool,
}
impl<R: AsyncRead + Unpin> SnapshotStream<R> {
    pub async fn new(mut source: R, id: String, earliest: i64, max_bytes: u64) -> Result<Self> {
        let mut budget = Budget(max_bytes);
        budget.take(8)?;
        let mut magic = [0; 8];
        source.read_exact(&mut magic).await?;
        ensure!(&magic == MAGIC, "unsupported snapshot format");
        let mut hash = Sha256::new();
        hash.update(magic);
        Ok(Self {
            source,
            hash,
            budget,
            id,
            earliest,
            cursor: earliest,
            identity: None,
            done: false,
            poisoned: false,
        })
    }

    /// Recover the transport only after full-image verification. A file receiver
    /// can use this to reject bytes beyond the framed image.
    pub fn finish(self) -> Result<R> {
        ensure!(self.done && !self.poisoned, "snapshot stream is incomplete");
        Ok(self.source)
    }

    pub async fn next_page(&mut self) -> Result<Option<SnapshotPage>> {
        ensure!(!self.poisoned, "snapshot reader failed or was cancelled");
        if self.done {
            return Ok(None);
        }
        // Cancellation at any await also leaves this reader poisoned.
        self.poisoned = true;
        self.budget.take(4)?;
        let mut length = [0; 4];
        self.source.read_exact(&mut length).await?;
        let size = u32::from_be_bytes(length) as usize;
        ensure!(
            size > 0 && size <= MAX_FRAME,
            "snapshot frame exceeds limit"
        );
        self.budget.take(size as u64)?;
        let mut bytes = vec![0; size];
        self.source.read_exact(&mut bytes).await?;
        self.hash.update(length);
        self.hash.update(&bytes);
        let page: SnapshotPage = serde_json::from_slice(&bytes)?;
        page.validate(&self.id, self.cursor)?;
        check_identity(&page, &mut self.identity, self.earliest)?;
        if page.eof {
            self.budget.take(32)?;
            let mut digest = [0; 32];
            self.source.read_exact(&mut digest).await?;
            ensure!(
                <[u8; 32]>::from(self.hash.clone().finalize()) == digest,
                "snapshot checksum mismatch"
            );
            self.done = true;
        }
        self.cursor = page.next_offset;
        self.poisoned = false;
        Ok(Some(page))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn page(base: i64, end: i64) -> serde_json::Value {
        let data = include_str!("../tests/fixtures/append-batch.hex").trim();
        let timestamps = broker_protocol::validate_batch(&hex::decode(data).unwrap()).unwrap();
        json!({
            "snapshot": {"snapshot_id":"test", "group_key":"group", "manifest":{"applied":0},
                "topic":"topic", "partition":0,"earliest":0,"end_offset":end,
                "cursor_offset":end,"state":"complete"},
            "batches":[{"base_offset":base,"record_count":1,"data":data,
                "max_timestamp":timestamps[0],"timestamps":timestamps,"appended_at_micros":123}],
            "next_offset":base+1,"eof":base+1==end
        })
    }
    fn stream(pages: &[serde_json::Value]) -> Vec<u8> {
        let mut bytes = MAGIC.to_vec();
        for page in pages {
            let data = serde_json::to_vec(page).unwrap();
            bytes.extend_from_slice(&(data.len() as u32).to_be_bytes());
            bytes.extend(data);
        }
        let digest = Sha256::digest(&bytes);
        bytes.extend_from_slice(&digest);
        bytes
    }
    async fn drain(bytes: &[u8], budget: u64) -> Result<usize> {
        let mut reader = SnapshotStream::new(bytes, "test".into(), 0, budget).await?;
        let mut count = 0;
        while let Some(page) = reader.next_page().await? {
            count += page.batches.len();
        }
        Ok(count)
    }
    #[tokio::test]
    async fn round_trip_empty_and_multiple_pages() {
        let bytes = stream(&[page(0, 2), page(1, 2)]);
        assert_eq!(drain(&bytes, bytes.len() as u64).await.unwrap(), 2);
        let mut empty = page(0, 0);
        empty["batches"] = json!([]);
        empty["next_offset"] = json!(0);
        empty["eof"] = json!(true);
        assert_eq!(drain(&stream(&[empty]), 10000).await.unwrap(), 0);
    }
    #[tokio::test]
    async fn rejects_every_truncation_corruption_and_budget_shortfall() {
        let bytes = stream(&[page(0, 1)]);
        for end in 0..bytes.len() {
            assert!(
                drain(&bytes[..end], 10000).await.is_err(),
                "truncation {end}"
            );
        }
        assert!(drain(&bytes, bytes.len() as u64 - 1).await.is_err());
        let mut corrupt = bytes.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        let mut reader = SnapshotStream::new(&corrupt[..], "test".into(), 0, 10000)
            .await
            .unwrap();
        assert!(reader.next_page().await.is_err());
        assert!(reader.next_page().await.is_err());
        let mut oversized = MAGIC.to_vec();
        oversized.extend_from_slice(&((MAX_FRAME + 1) as u32).to_be_bytes());
        assert!(drain(&oversized, u64::MAX).await.is_err());
    }
    #[tokio::test]
    async fn rejects_reordering_gaps_and_changed_manifest_even_with_valid_checksum() {
        for pages in [vec![page(1, 2), page(0, 2)], vec![page(0, 3), page(2, 3)]] {
            assert!(drain(&stream(&pages), 10000).await.is_err());
        }
        let mut changed = page(1, 2);
        changed["snapshot"]["manifest"]["applied"] = json!(1);
        assert!(drain(&stream(&[page(0, 2), changed]), 10000).await.is_err());
    }
    #[tokio::test]
    async fn cancellation_poisons_partial_reader() {
        let (mut tx, rx) = tokio::io::duplex(64);
        tx.write_all(MAGIC).await.unwrap();
        let mut reader = SnapshotStream::new(rx, "test".into(), 0, 10000)
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), reader.next_page())
                .await
                .is_err()
        );
        assert!(reader.next_page().await.is_err());
    }
}

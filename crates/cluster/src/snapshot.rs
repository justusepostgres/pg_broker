//! Local durable paged capture. This is not RaftSnapshotBuilder or snapshot install.
use crate::application::PartitionApplication;
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CaptureState {
    Capturing,
    Complete,
    Releasing,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotCapture {
    pub snapshot_id: String,
    pub group_key: String,
    pub manifest: Value,
    pub topic: String,
    pub partition: i32,
    pub earliest: i64,
    pub end_offset: i64,
    pub cursor_offset: i64,
    pub state: CaptureState,
}

impl PartitionApplication {
    /// Capture the applied marker and retained partition bounds, or resume the
    /// existing capture. Requires opt-in SQL 0.1.13 and exclusive application
    /// ownership of the partition (legacy mutations are not replicated).
    pub async fn begin_snapshot(&mut self) -> Result<SnapshotCapture> {
        Ok(serde_json::from_value(
            self.snapshot_operation("begin", None).await?,
        )?)
    }

    /// Copy at most 128 batches and 8 MiB of source data/metadata in one short,
    /// confirmed transaction. The durable cursor is safe to retry after an
    /// ambiguous response. New appends can proceed between calls.
    pub async fn copy_snapshot(&mut self, snapshot_id: &str) -> Result<SnapshotCapture> {
        Ok(serde_json::from_value(
            self.snapshot_operation("copy", Some(snapshot_id)).await?,
        )?)
    }

    /// Abort capture or release obsolete data in pages of at most 128 batches.
    /// Returns true when removed, false when another call is needed. Callers
    /// must not release a snapshot being exported or used by recovery.
    pub async fn release_snapshot(&mut self, snapshot_id: &str) -> Result<bool> {
        self.snapshot_operation("release", Some(snapshot_id))
            .await?["released"]
            .as_bool()
            .context("invalid snapshot release response")
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotBatch {
    pub base_offset: i64,
    pub record_count: i32,
    pub data: String,
    pub max_timestamp: i64,
    pub timestamps: Vec<i64>,
    pub appended_at_micros: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotPage {
    pub snapshot: SnapshotCapture,
    pub batches: Vec<SnapshotBatch>,
    pub next_offset: i64,
    pub eof: bool,
}

impl SnapshotPage {
    pub(crate) fn validate(&self, id: &str, start: i64) -> Result<()> {
        ensure!(
            self.snapshot.snapshot_id == id && self.snapshot.state == CaptureState::Complete,
            "snapshot identity/state mismatch"
        );
        ensure!(
            self.snapshot.earliest <= start && start <= self.snapshot.end_offset,
            "snapshot cursor outside interval"
        );
        ensure!(self.batches.len() <= 128, "snapshot count limit");
        let mut cursor = start;
        for batch in &self.batches {
            ensure!(
                batch.base_offset == cursor && batch.record_count > 0,
                "snapshot offset discontinuity"
            );
            let bytes = hex::decode(&batch.data)?;
            let timestamps = broker_protocol::validate_batch(&bytes)?;
            ensure!(
                timestamps == batch.timestamps
                    && timestamps.len() == batch.record_count as usize
                    && timestamps.iter().max() == Some(&batch.max_timestamp),
                "snapshot record metadata mismatch"
            );
            cursor = cursor
                .checked_add(i64::from(batch.record_count))
                .context("snapshot offset overflow")?;
            ensure!(
                cursor <= self.snapshot.end_offset,
                "snapshot exceeds captured boundary"
            );
        }
        ensure!(
            cursor == self.next_offset && self.eof == (cursor == self.snapshot.end_offset),
            "snapshot continuation mismatch"
        );
        ensure!(self.eof || cursor > start, "snapshot page made no progress");
        Ok(())
    }
}

impl PartitionApplication {
    /// Read at most 128 captured batches/2 MiB raw data+metadata. The complete
    /// SQL response is capped at 8 MiB including hex and manifest. The database
    /// transaction is confirmed and closed before this page is returned for IO.
    /// A later caller must use next_offset and reject a changing snapshot ID.
    pub async fn read_snapshot(&self, snapshot_id: &str, start: i64) -> Result<SnapshotPage> {
        let page: SnapshotPage =
            serde_json::from_value(self.snapshot_page(snapshot_id, start).await?)?;
        page.validate(snapshot_id, start)?;
        Ok(page)
    }
}

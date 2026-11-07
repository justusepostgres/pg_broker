//! Durable application probe. Snapshot operations deliberately remain unsupported.
use super::{
    alpha_log::{convert, error},
    Next,
};
use broker_cluster::application::PartitionApplication;
use futures_util::{Stream, StreamExt};
use openraft_next::{
    storage::{EntryResponder, RaftStateMachine},
    type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf},
    RaftSnapshotBuilder,
};
use std::io;

pub struct Machine {
    pub app: PartitionApplication,
    pub topic: String,
    pub topic_id: String,
}
pub struct NoSnapshots;
impl RaftSnapshotBuilder<Next> for NoSnapshots {
    type SnapshotData = ();
    async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<Next, ()>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "snapshot builder not implemented",
        ))
    }
}
impl RaftStateMachine<Next> for Machine {
    type SnapshotData = ();
    type SnapshotBuilder = NoSnapshots;
    async fn applied_state(
        &mut self,
    ) -> io::Result<(Option<LogIdOf<Next>>, StoredMembershipOf<Next>)> {
        let state = self
            .app
            .bind(&self.topic, &self.topic_id, 0)
            .await
            .map_err(error)?;
        Ok((convert(state.applied)?, convert(state.membership)?))
    }
    async fn apply<S>(&mut self, mut entries: S) -> io::Result<()>
    where
        S: Stream<Item = io::Result<EntryResponder<Next>>> + Unpin + Send,
    {
        while let Some(entry) = entries.next().await {
            let (entry, responder) = entry?;
            // SQL loads authoritative payload bytes by ID. Its data, membership
            // and applied marker commit together; this await confirms COMMIT.
            self.app
                .apply(convert(entry.log_id)?)
                .await
                .map_err(error)?;
            if let Some(responder) = responder {
                responder.send(());
            }
        }
        Ok(())
    }
    async fn get_snapshot_builder(&mut self) -> NoSnapshots {
        NoSnapshots
    }
    async fn install_snapshot(&mut self, _: &SnapshotMetaOf<Next>, _: ()) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "snapshot install not implemented",
        ))
    }
    async fn get_current_snapshot(&mut self) -> io::Result<Option<SnapshotOf<Next, ()>>> {
        Ok(None)
    }
}

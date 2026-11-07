//! Single-node-only probe: every network operation fails explicitly.
use super::Next;
use openraft_next::{
    errors::{RPCError, ReplicationClosed, StreamingError, Unreachable},
    network::{RPCOption, RaftNetworkFactory, RaftNetworkV2},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    type_config::alias::{SnapshotOf, VoteOf},
};
use std::{future::Future, io};
pub struct NoNetwork;
fn unavailable() -> Unreachable<Next> {
    Unreachable::new(&io::Error::new(
        io::ErrorKind::Unsupported,
        "single-node probe has no network",
    ))
}
impl RaftNetworkFactory<Next> for NoNetwork {
    type Network = Self;
    async fn new_client(&mut self, _: u64, _: &openraft_next::BasicNode) -> Self {
        Self
    }
}
impl RaftNetworkV2<Next> for NoNetwork {
    type SnapshotData = ();
    async fn append_entries(
        &mut self,
        _: AppendEntriesRequest<Next>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<Next>, RPCError<Next>> {
        Err(unavailable().into())
    }
    async fn vote(
        &mut self,
        _: VoteRequest<Next>,
        _: RPCOption,
    ) -> Result<VoteResponse<Next>, RPCError<Next>> {
        Err(unavailable().into())
    }
    async fn pre_vote(
        &mut self,
        _: VoteRequest<Next>,
        _: RPCOption,
    ) -> Result<VoteResponse<Next>, RPCError<Next>> {
        Err(unavailable().into())
    }
    async fn full_snapshot(
        &mut self,
        _: VoteOf<Next>,
        _: SnapshotOf<Next, ()>,
        _: impl Future<Output = ReplicationClosed> + Send + 'static,
        _: RPCOption,
    ) -> Result<SnapshotResponse<Next>, StreamingError<Next>> {
        Err(unavailable().into())
    }
}

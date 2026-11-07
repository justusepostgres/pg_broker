//! Loopback-only experimental Raft RPC transport. No TLS, snapshots or production deployment.
use super::{alpha_machine::Machine, Next};
use openraft_next::{
    errors::{RPCError, ReplicationClosed, StreamingError, Unreachable},
    network::{RPCOption, RaftNetworkFactory, RaftNetworkV2},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    type_config::alias::{SnapshotOf, VoteOf},
    Raft,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{future::Future, io, net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::{JoinHandle, JoinSet},
};
const LIMIT: usize = 16 * 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(2);
#[derive(Serialize, Deserialize)]
enum Request {
    Append(AppendEntriesRequest<Next>),
    Vote(VoteRequest<Next>),
    PreVote(VoteRequest<Next>),
}
#[derive(Serialize, Deserialize)]
enum Response {
    Append(AppendEntriesResponse<Next>),
    Vote(VoteResponse<Next>),
    Error(String),
}
fn io_error(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}
async fn read<T: DeserializeOwned>(stream: &mut TcpStream) -> io::Result<T> {
    let len = stream.read_u32().await? as usize;
    if len == 0 || len > LIMIT {
        return Err(io_error("invalid RPC frame size"));
    }
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes).await?;
    serde_json::from_slice(&bytes).map_err(io_error)
}
fn encode<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    struct Capped(Vec<u8>);
    impl io::Write for Capped {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > LIMIT - self.0.len() {
                return Err(io_error("RPC frame exceeds limit"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Capped(Vec::new());
    serde_json::to_writer(&mut writer, value).map_err(io_error)?;
    Ok(writer.0)
}
async fn write<T: Serialize>(stream: &mut TcpStream, value: &T) -> io::Result<()> {
    let bytes = encode(value)?;
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await
}
pub struct Network {
    address: Option<SocketAddr>,
}
impl Network {
    pub fn factory() -> Self {
        Self { address: None }
    }
}
impl RaftNetworkFactory<Next> for Network {
    type Network = Self;
    async fn new_client(&mut self, _: u64, node: &openraft_next::BasicNode) -> Self {
        Self {
            address: node
                .addr
                .parse::<SocketAddr>()
                .ok()
                .filter(|a| a.ip().is_loopback()),
        }
    }
}
impl Network {
    async fn call(&self, request: Request) -> Result<Response, RPCError<Next>> {
        let work = async {
            let address = self
                .address
                .ok_or_else(|| io_error("probe requires a loopback peer"))?;
            let mut stream = TcpStream::connect(address).await?;
            stream.set_nodelay(true)?;
            write(&mut stream, &request).await?;
            read(&mut stream).await
        };
        tokio::time::timeout(DEADLINE, work)
            .await
            .map_err(io_error)
            .and_then(|r| r)
            .map_err(|e| Unreachable::new(&e).into())
    }
}
impl RaftNetworkV2<Next> for Network {
    type SnapshotData = ();
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<Next>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<Next>, RPCError<Next>> {
        match self.call(Request::Append(rpc)).await? {
            Response::Append(r) => Ok(r),
            _ => Err(Unreachable::new(&io_error("invalid or failed append reply")).into()),
        }
    }
    async fn vote(
        &mut self,
        rpc: VoteRequest<Next>,
        _: RPCOption,
    ) -> Result<VoteResponse<Next>, RPCError<Next>> {
        match self.call(Request::Vote(rpc)).await? {
            Response::Vote(r) => Ok(r),
            _ => Err(Unreachable::new(&io_error("invalid or failed vote reply")).into()),
        }
    }
    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<Next>,
        _: RPCOption,
    ) -> Result<VoteResponse<Next>, RPCError<Next>> {
        match self.call(Request::PreVote(rpc)).await? {
            Response::Vote(r) => Ok(r),
            _ => Err(Unreachable::new(&io_error("invalid or failed pre-vote reply")).into()),
        }
    }
    async fn full_snapshot(
        &mut self,
        _: VoteOf<Next>,
        _: SnapshotOf<Next, ()>,
        _: impl Future<Output = ReplicationClosed> + Send + 'static,
        _: RPCOption,
    ) -> Result<SnapshotResponse<Next>, StreamingError<Next>> {
        Err(Unreachable::new(&io_error("snapshot transport unsupported")).into())
    }
}
pub struct Server {
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}
impl Server {
    pub async fn shutdown(self) {
        let _ = self.stop.send(());
        self.task.await.unwrap();
    }
}
pub fn serve(listener: TcpListener, raft: Raft<Next, Machine>) -> Server {
    assert!(listener.local_addr().unwrap().ip().is_loopback());
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut requests = JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                _ = requests.join_next(), if !requests.is_empty() => {},
                accepted = listener.accept(), if requests.len() < 16 => {
                    let Ok((mut stream, _)) = accepted else { break };
                    let raft = raft.clone();
                    requests.spawn(async move {
                        let _ = tokio::time::timeout(DEADLINE, async {
                            let reply = match read::<Request>(&mut stream).await? {
                                Request::Append(r) => raft.append_entries(r).await.map(Response::Append),
                                Request::Vote(r) => raft.vote(r).await.map(Response::Vote),
                                Request::PreVote(r) => raft.pre_vote(r).await.map(Response::Vote),
                            }.unwrap_or_else(|e| Response::Error(e.to_string()));
                            write(&mut stream, &reply).await
                        }).await;
                    });
                }
            }
        }
        requests.abort_all();
        while requests.join_next().await.is_some() {}
    });
    Server { stop, task }
}

#[test]
fn outgoing_frame_is_bounded_during_serialization() {
    assert_eq!(encode(&"small").unwrap(), b"\"small\"");
    assert!(encode(&"x".repeat(LIMIT)).is_err());
}

#[tokio::test]
async fn incoming_frames_reject_oversized_and_truncated_payloads() {
    for (length, bytes) in [((LIMIT + 1) as u32, vec![]), (100, b"{}".to_vec())] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let sender = tokio::spawn(async move {
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream.write_u32(length).await.unwrap();
            stream.write_all(&bytes).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        let (mut stream, _) = listener.accept().await.unwrap();
        assert!(tokio::time::timeout(DEADLINE, read::<Request>(&mut stream))
            .await
            .unwrap()
            .is_err());
        sender.await.unwrap();
    }
}

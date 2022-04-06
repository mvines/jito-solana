//! The `mev_stage` maintains a connection with the validator
//! interface and streams packets from TPU proxy to the banking stage.

use {
    crate::{
        backoff::BackoffStrategy,
        bundle::Bundle,
        proto::validator_interface::{
            subscribe_packets_response::Msg, validator_interface_client::ValidatorInterfaceClient,
            GetTpuConfigsRequest, SubscribeBundlesRequest, SubscribeBundlesResponse,
            SubscribePacketsRequest, SubscribePacketsResponse,
        },
        proto_packet_to_packet,
    },
    crossbeam_channel::{select, tick, unbounded, Receiver, RecvError, Sender},
    log::*,
    solana_perf::packet::PacketBatch,
    solana_sdk::{
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
    },
    std::{
        net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, RwLockReadGuard},
        thread::{self, sleep, JoinHandle},
        time::Duration,
    },
    thiserror::Error,
    tokio::runtime::{Builder, Runtime},
    tonic::{
        codegen::{http::uri::InvalidUri, InterceptedService},
        metadata::MetadataValue,
        service::Interceptor,
        transport::{Channel, Endpoint, Error},
        Status,
    },
};

#[derive(Error, Debug)]
pub enum MevStageError {
    #[error("bad uri error: {0}")]
    BadUrl(#[from] InvalidUri),
    #[error("connecting error: {0}")]
    ConnectionError(#[from] Error),
    #[error("grpc error: {0}")]
    GrpcError(#[from] Status),
    #[error("missing tpu socket: {0}")]
    MissingTpuSocket(String),
    #[error("invalid tpu socket: {0}")]
    BadTpuSocket(#[from] AddrParseError),
    #[error("stream disconnected")]
    GrpcStreamDisconnected,
    #[error("bad packet message")]
    BadMessage,
    #[error("error sending message to another part of the system")]
    ChannelError,
    #[error("backend sent disconnection through heartbeat")]
    HeartbeatError,
}

type Result<T> = std::result::Result<T, MevStageError>;

#[derive(Clone)]
struct AuthenticationInjector {
    msg: Vec<u8>,
    sig: Signature,
    pubkey: Pubkey,
}

impl AuthenticationInjector {
    pub fn new(msg: Vec<u8>, sig: Signature, pubkey: Pubkey) -> Self {
        AuthenticationInjector { msg, sig, pubkey }
    }
}

impl Interceptor for AuthenticationInjector {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, Status> {
        request.metadata_mut().append_bin(
            "public-key-bin",
            MetadataValue::from_bytes(&self.pubkey.to_bytes()),
        );
        request.metadata_mut().append_bin(
            "message-bin",
            MetadataValue::from_bytes(self.msg.as_slice()),
        );
        request.metadata_mut().append_bin(
            "signature-bin",
            MetadataValue::from_bytes(self.sig.as_ref()),
        );
        Ok(request)
    }
}

type ValidatorInterfaceClientType =
    ValidatorInterfaceClient<InterceptedService<Channel, AuthenticationInjector>>;

struct BlockingProxyClient {
    rt: Runtime,
    client: ValidatorInterfaceClientType,
}

/// Blocking interface to the validator interface server
impl BlockingProxyClient {
    pub fn new(
        validator_interface_address: &str,
        auth_interceptor: &AuthenticationInjector,
    ) -> Result<Self> {
        let rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let channel =
            rt.block_on(Endpoint::from_shared(validator_interface_address.to_string())?.connect())?;
        let client = ValidatorInterfaceClient::with_interceptor(channel, auth_interceptor.clone());
        Ok(Self { rt, client })
    }

    pub fn fetch_tpu_config(&mut self) -> Result<(SocketAddr, SocketAddr)> {
        let tpu_configs = self
            .rt
            .block_on(self.client.get_tpu_configs(GetTpuConfigsRequest {}))?
            .into_inner();

        let tpu_addr = tpu_configs
            .tpu
            .ok_or(MevStageError::MissingTpuSocket("tpu".into()))?;
        let tpu_forward_addr = tpu_configs
            .tpu_forward
            .ok_or(MevStageError::MissingTpuSocket("tpu_fwd".into()))?;

        let tpu_ip = IpAddr::from(tpu_addr.ip.parse::<Ipv4Addr>()?);
        let tpu_forward_ip = IpAddr::from(tpu_forward_addr.ip.parse::<Ipv4Addr>()?);

        let tpu_socket = SocketAddr::new(tpu_ip, tpu_addr.port as u16);
        let tpu_forward_socket = SocketAddr::new(tpu_forward_ip, tpu_forward_addr.port as u16);

        Ok((tpu_socket, tpu_forward_socket))
    }

    pub fn subscribe_packets(
        &mut self,
    ) -> Result<(
        tokio::task::JoinHandle<()>,
        Receiver<std::result::Result<Option<SubscribePacketsResponse>, Status>>,
    )> {
        let mut packet_subscription = self
            .rt
            .block_on(self.client.subscribe_packets(SubscribePacketsRequest {}))?
            .into_inner();

        let (sender, receiver) = unbounded();
        let handle = self.rt.spawn(async move {
            loop {
                let msg = packet_subscription.message().await;
                let error = msg.is_err();
                if sender.send(msg).is_err() || error {
                    break;
                }
            }
        });

        Ok((handle, receiver))
    }

    pub fn subscribe_bundles(
        &mut self,
    ) -> Result<(
        tokio::task::JoinHandle<()>,
        Receiver<std::result::Result<Option<SubscribeBundlesResponse>, Status>>,
    )> {
        let mut bundle_subscription = self
            .rt
            .block_on(self.client.subscribe_bundles(SubscribeBundlesRequest {}))?
            .into_inner();

        let (sender, receiver) = unbounded();
        let handle = self.rt.spawn(async move {
            loop {
                let msg = bundle_subscription.message().await;
                let error = msg.is_err();
                if sender.send(msg).is_err() || error {
                    break;
                }
            }
        });

        Ok((handle, receiver))
    }
}

enum HeartbeatEvent {
    Tick {
        tpu: SocketAddr,
        tpu_fwd: SocketAddr,
    },
}

pub struct MevStage {
    proxy_thread: JoinHandle<()>,
}

impl MevStage {
    fn spawn_proxy_thread(
        validator_interface_address: Option<SocketAddr>,
        interceptor: AuthenticationInjector,
        verified_packet_sender: Sender<Vec<PacketBatch>>,
        bundle_sender: Sender<Vec<Bundle>>,
        heartbeat_timeout_ms: u64,
        heartbeat_sender: Sender<HeartbeatEvent>,
    ) -> JoinHandle<()> {
        thread::Builder::new()
            .name("proxy_thread".into())
            .spawn(move || {
                if validator_interface_address.is_none() {
                    info!("no mev proxy address provided, exiting mev loop");
                    return;
                }

                let addr = format!("http://{}", validator_interface_address.unwrap());
                let mut backoff = BackoffStrategy::new();

                loop {
                    match Self::run_event_loops(
                        &addr,
                        &interceptor,
                        &heartbeat_sender,
                        &verified_packet_sender,
                        &bundle_sender,
                        heartbeat_timeout_ms,
                    ) {
                        Ok(_) => {
                            backoff.reset();
                        }
                        Err(e) => {
                            error!("error yo {}", e);
                            sleep(Duration::from_millis(backoff.next_wait()));
                        }
                    }
                }
            })
            .unwrap()
    }

    pub fn new(
        keypair: RwLockReadGuard<Arc<Keypair>>,
        validator_interface_address: Option<SocketAddr>,
        verified_packet_sender: Sender<Vec<PacketBatch>>,
        bundle_sender: Sender<Vec<Bundle>>,
        heartbeat_timeout_ms: u64,
    ) -> Self {
        let msg = b"Let's get this money!".to_vec();
        let sig: Signature = keypair.sign_message(msg.as_slice());
        let pubkey = keypair.pubkey();
        let interceptor = AuthenticationInjector::new(msg, sig, pubkey);

        let (heartbeat_sender, heartbeat_receiver) = unbounded();

        let proxy_thread = Self::spawn_proxy_thread(
            validator_interface_address,
            interceptor,
            verified_packet_sender,
            bundle_sender,
            heartbeat_timeout_ms,
            heartbeat_sender,
        );

        info!("[MEV] Started recv verify stage");

        Self { proxy_thread }
    }

    fn handle_bundle(
        msg: std::result::Result<
            std::result::Result<Option<SubscribeBundlesResponse>, Status>,
            RecvError,
        >,
        bundle_sender: &Sender<Vec<Bundle>>,
    ) -> Result<()> {
        match msg {
            Ok(msg) => {
                let response = msg?.ok_or(MevStageError::GrpcStreamDisconnected)?;
                let bundles = response
                    .bundles
                    .into_iter()
                    .map(|b| {
                        let batch = PacketBatch::new(
                            b.packets.into_iter().map(proto_packet_to_packet).collect(),
                        );
                        Bundle { batch }
                    })
                    .collect();
                bundle_sender
                    .send(bundles)
                    .map_err(|_| MevStageError::ChannelError)?;
            }
            Err(_) => return Err(MevStageError::ChannelError),
        }
        Ok(())
    }

    fn handle_packet(
        msg: std::result::Result<
            std::result::Result<Option<SubscribePacketsResponse>, Status>,
            RecvError,
        >,
        packet_sender: &Sender<Vec<PacketBatch>>,
        heartbeat_sender: &Sender<HeartbeatEvent>,
        tpu: &SocketAddr,
        tpu_fwd: &SocketAddr,
    ) -> Result<bool> {
        let mut is_heartbeat = false;
        match msg {
            Ok(msg) => {
                let msg = msg?
                    .ok_or(MevStageError::GrpcStreamDisconnected)?
                    .msg
                    .ok_or(MevStageError::BadMessage)?;
                match msg {
                    Msg::BatchList(batch_wrapper) => {
                        let packet_batches = batch_wrapper
                            .batch_list
                            .into_iter()
                            .map(|batch| {
                                PacketBatch::new(
                                    batch
                                        .packets
                                        .into_iter()
                                        .map(proto_packet_to_packet)
                                        .collect(),
                                )
                            })
                            .collect();
                        packet_sender
                            .send(packet_batches)
                            .map_err(|_| MevStageError::ChannelError)?;
                    }
                    Msg::Heartbeat(true) => {
                        info!("heartbeat");
                        // always sends because tpu_proxy has its own fail-safe and can't assume
                        // state
                        heartbeat_sender
                            .send(HeartbeatEvent::Tick {
                                tpu: tpu.clone(),
                                tpu_fwd: tpu_fwd.clone(),
                            })
                            .map_err(|_| MevStageError::ChannelError)?;
                        is_heartbeat = true;
                    }
                    Msg::Heartbeat(false) => {
                        info!("heartbeat false");
                        return Err(MevStageError::HeartbeatError);
                    }
                }
            }
            Err(_) => return Err(MevStageError::ChannelError),
        }
        Ok(is_heartbeat)
    }

    fn streamer_loop(
        mut client: BlockingProxyClient,
        heartbeat_sender: &Sender<HeartbeatEvent>,
        tpu: SocketAddr,
        tpu_fwd: SocketAddr,
        verified_packet_sender: &Sender<Vec<PacketBatch>>,
        bundle_sender: &Sender<Vec<Bundle>>,
        heartbeat_timeout_ms: u64,
    ) -> Result<()> {
        let mut heartbeat_sent = false;

        let (_, packet_receiver) = client.subscribe_packets()?;
        let (_, bundle_receiver) = client.subscribe_bundles()?;

        let mut first_heartbeat = true;
        let heartbeat_receiver = tick(Duration::from_millis(heartbeat_timeout_ms));

        loop {
            select! {
                recv(heartbeat_receiver) -> msg => {
                    info!("heartbeat tick");
                    if !heartbeat_sent && !first_heartbeat {
                        warn!("heartbeat late, disconnecting");
                        return Err(MevStageError::HeartbeatError);
                    }
                    first_heartbeat = false;
                    heartbeat_sent = false;
                }
                recv(bundle_receiver) -> msg => {
                    Self::handle_bundle(msg, &bundle_sender)?;
                }
                recv(packet_receiver) -> msg => {
                    heartbeat_sent |= Self::handle_packet(msg, &verified_packet_sender, &heartbeat_sender, &tpu, &tpu_fwd)?;
                }
            }
        }
    }

    fn run_event_loops(
        validator_interface_address: &str,
        auth_interceptor: &AuthenticationInjector,
        heartbeat_sender: &Sender<HeartbeatEvent>,
        verified_packet_sender: &Sender<Vec<PacketBatch>>,
        bundle_sender: &Sender<Vec<Bundle>>,
        heartbeat_timeout_ms: u64,
    ) -> Result<()> {
        let mut client = BlockingProxyClient::new(validator_interface_address, auth_interceptor)?;
        let (tpu, tpu_fwd) = client.fetch_tpu_config()?;

        Self::streamer_loop(
            client,
            heartbeat_sender,
            tpu,
            tpu_fwd,
            verified_packet_sender,
            bundle_sender,
            heartbeat_timeout_ms,
        )
    }

    async fn fetch_tpu_config(
        client: &mut ValidatorInterfaceClientType,
    ) -> Result<(SocketAddr, SocketAddr)> {
        let tpu_configs = client
            .get_tpu_configs(GetTpuConfigsRequest {})
            .await?
            .into_inner();

        let tpu_addr = tpu_configs
            .tpu
            .ok_or(MevStageError::MissingTpuSocket("tpu".into()))?;
        let tpu_forward_addr = tpu_configs
            .tpu_forward
            .ok_or(MevStageError::MissingTpuSocket("tpu_fwd".into()))?;

        let tpu_ip = IpAddr::from(tpu_addr.ip.parse::<Ipv4Addr>()?);
        let tpu_forward_ip = IpAddr::from(tpu_forward_addr.ip.parse::<Ipv4Addr>()?);

        let tpu_socket = SocketAddr::new(tpu_ip, tpu_addr.port as u16);
        let tpu_forward_socket = SocketAddr::new(tpu_forward_ip, tpu_forward_addr.port as u16);

        Ok((tpu_socket, tpu_forward_socket))
    }

    pub fn join(self) -> thread::Result<()> {
        self.proxy_thread.join()?;
        Ok(())
    }
}

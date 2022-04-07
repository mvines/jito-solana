//! The `mev_stage` maintains a connection with the validator
//! interface and streams packets from TPU proxy to the banking stage.

use {
    crate::{
        backoff::BackoffStrategy,
        blocking_proxy_client::{AuthenticationInjector, BlockingProxyClient, ProxyError},
        bundle::Bundle,
        proto::validator_interface::{
            subscribe_packets_response::Msg, SubscribeBundlesResponse, SubscribePacketsResponse,
        },
        proto_packet_to_packet,
    },
    crossbeam_channel::{select, tick, unbounded, Receiver, RecvError, RecvTimeoutError, Sender},
    log::*,
    solana_gossip::cluster_info::ClusterInfo,
    solana_perf::packet::PacketBatch,
    solana_sdk::{
        signature::{Keypair, Signature},
        signer::Signer,
    },
    std::{
        net::SocketAddr,
        sync::{Arc, RwLockReadGuard},
        thread::{self, sleep, JoinHandle},
        time::Duration,
    },
    thiserror::Error,
    tonic::Status,
};

#[derive(Error, Debug)]
pub enum MevStageError {
    #[error("proxy error: {0}")]
    ProxyError(#[from] ProxyError),
    #[error("grpc error: {0}")]
    GrpcError(#[from] Status),
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

enum HeartbeatEvent {
    Tick {
        tpu: SocketAddr,
        tpu_fwd: SocketAddr,
    },
}

pub struct MevStage {
    proxy_thread: JoinHandle<()>,
    heartbeat_thread: JoinHandle<()>,
}

impl MevStage {
    pub fn new(
        keypair: RwLockReadGuard<Arc<Keypair>>,
        validator_interface_address: Option<SocketAddr>,
        verified_packet_sender: Sender<Vec<PacketBatch>>,
        bundle_sender: Sender<Vec<Bundle>>,
        heartbeat_timeout_ms: u64,
        cluster_info: &Arc<ClusterInfo>,
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

        let heartbeat_thread =
            Self::spawn_heartbeat_thread(heartbeat_receiver, heartbeat_timeout_ms, cluster_info);

        info!("[MEV] Started recv verify stage");

        Self {
            proxy_thread,
            heartbeat_thread,
        }
    }

    fn spawn_heartbeat_thread(
        heartbeat_receiver: Receiver<HeartbeatEvent>,
        heartbeat_timeout_ms: u64,
        cluster_info: &Arc<ClusterInfo>,
    ) -> JoinHandle<()> {
        let cluster_info = cluster_info.clone();
        thread::Builder::new()
            .name("proxy_thread".into())
            .spawn(move || {
                let saved_contact_info = cluster_info.my_contact_info();
                let mut is_advertising_proxy = false;

                loop {
                    match heartbeat_receiver
                        .recv_timeout(Duration::from_millis(heartbeat_timeout_ms))
                    {
                        Ok(HeartbeatEvent::Tick { tpu, tpu_fwd }) => {
                            if !is_advertising_proxy {
                                info!("advertising tpu proxy");
                                Self::set_tpu_addresses(&cluster_info, tpu, tpu_fwd);
                                is_advertising_proxy = true;
                            }
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            if is_advertising_proxy {
                                warn!(
                                    "heartbeat timeout, reverting tpu and tpu_forwards addresses"
                                );
                                Self::set_tpu_addresses(
                                    &cluster_info,
                                    saved_contact_info.tpu,
                                    saved_contact_info.tpu_forwards,
                                );
                                is_advertising_proxy = false;
                            }
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            warn!("heartbeat channel disconnected, shutting down");
                            Self::set_tpu_addresses(
                                &cluster_info,
                                saved_contact_info.tpu,
                                saved_contact_info.tpu_forwards,
                            );
                            break;
                        }
                    }
                }
            })
            .unwrap()
    }

    fn set_tpu_addresses(
        cluster_info: &Arc<ClusterInfo>,
        tpu_address: SocketAddr,
        tpu_forward_address: SocketAddr,
    ) {
        let mut new_contact_info = cluster_info.my_contact_info();
        new_contact_info.tpu = tpu_address;
        new_contact_info.tpu_forwards = tpu_forward_address;
        cluster_info.set_my_contact_info(new_contact_info);
    }

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
                    match Self::connect_and_stream(
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
                            error!("spawn_proxy_thread error [error: {}]", e);
                            sleep(Duration::from_millis(1000));
                        }
                    }
                }
            })
            .unwrap()
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

    fn stream_from_proxy(
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
                recv(heartbeat_receiver) -> _ => {
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

    fn connect_and_stream(
        validator_interface_address: &str,
        auth_interceptor: &AuthenticationInjector,
        heartbeat_sender: &Sender<HeartbeatEvent>,
        verified_packet_sender: &Sender<Vec<PacketBatch>>,
        bundle_sender: &Sender<Vec<Bundle>>,
        heartbeat_timeout_ms: u64,
    ) -> Result<()> {
        let mut client = BlockingProxyClient::new(validator_interface_address, auth_interceptor)?;
        let (tpu, tpu_fwd) = client.fetch_tpu_config()?;

        Self::stream_from_proxy(
            client,
            heartbeat_sender,
            tpu,
            tpu_fwd,
            verified_packet_sender,
            bundle_sender,
            heartbeat_timeout_ms,
        )
    }

    pub fn join(self) -> thread::Result<()> {
        self.proxy_thread.join()?;
        self.heartbeat_thread.join()?;
        Ok(())
    }
}

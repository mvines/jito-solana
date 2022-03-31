//! The `mev_stage` maintains a connection with the validator
//! interface and streams packets from TPU proxy to the banking stage.
//! It notifies the tpu_proxy_advertiser on connect/disconnect.

use {
    crate::{
        backoff::BackoffStrategy,
        bundle::Bundle,
        proto::{
            packet::PacketBatch as PbPacketBatch,
            validator_interface::{
                validator_interface_client::ValidatorInterfaceClient, GetTpuConfigsRequest,
            },
        },
        proto_packet_to_packet,
    },
    crossbeam_channel::Sender,
    log::*,
    solana_perf::{cuda_runtime::PinnedVec, packet::PacketBatch},
    solana_sdk::{
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
    },
    std::{
        net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, RwLockReadGuard},
        thread::{self, JoinHandle},
        time::Duration,
    },
    thiserror::Error,
    tokio::{
        runtime::Runtime,
        sync::{
            mpsc,
            mpsc::{UnboundedReceiver, UnboundedSender},
        },
        time::sleep,
    },
    tonic::{
        codegen::{http::uri::InvalidUri, InterceptedService, Service},
        metadata::MetadataValue,
        service::Interceptor,
        transport::{Channel, Endpoint, Error},
        Status,
    },
};

pub struct MevStage {
    validator_interface_thread: JoinHandle<()>,
    packet_converter_thread: JoinHandle<()>,
    proxy_bridge_thread: JoinHandle<()>,
}

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
}

type Result<T> = std::result::Result<T, MevStageError>;
type ValidatorInterfaceClientType =
    ValidatorInterfaceClient<InterceptedService<Channel, AuthenticationInjector>>;

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

impl MevStage {
    pub fn new(
        keypair: RwLockReadGuard<Arc<Keypair>>,
        validator_interface_address: Option<SocketAddr>,
        verified_packet_sender: Sender<Vec<PacketBatch>>,
        tpu_notify_sender: UnboundedSender<(Option<SocketAddr>, Option<SocketAddr>)>,
        bundle_sender: Sender<Vec<Bundle>>,
    ) -> Self {
        let msg = b"Let's get this money!".to_vec();
        let sig: Signature = keypair.sign_message(msg.as_slice());
        let pubkey = keypair.pubkey();
        let interceptor = AuthenticationInjector::new(msg, sig, pubkey);

        let (packet_converter_sender, packet_converter_receiver) = mpsc::unbounded_channel();

        let rt = Runtime::new().unwrap();
        let validator_interface_thread = thread::spawn(move || {
            if validator_interface_address.is_none() {
                info!("no mev proxy address provided, exiting mev loop");
                return;
            }

            rt.block_on({
                Self::validator_interface_connection_loop(
                    interceptor,
                    validator_interface_address,
                    packet_converter_sender,
                    tpu_notify_sender,
                    bundle_sender,
                )
            })
        });

        let (proxy_bridge_sender, proxy_bridge_receiver) = mpsc::unbounded_channel();
        let packet_converter_thread = thread::spawn(move || {
            Self::convert_packets(packet_converter_receiver, proxy_bridge_sender)
        });

        let proxy_bridge_thread = thread::spawn(move || {
            Self::proxy_bridge(proxy_bridge_receiver, verified_packet_sender)
        });

        info!("[MEV] Started recv verify stage");

        Self {
            validator_interface_thread,
            packet_converter_thread,
            proxy_bridge_thread,
        }
    }

    // Proxy bridge continuously reads from the receiver
    // and writes to sender (banking stage).
    fn proxy_bridge(
        mut proxy_bridge_receiver: UnboundedReceiver<Vec<PacketBatch>>,
        verified_packet_sender: Sender<Vec<PacketBatch>>,
    ) {
        // In the loop below the None case is failure to read from
        // the packet converter thread, whereas send error is failure to
        // forward to banking. Both should only occur on shutdown.
        loop {
            let maybe_packet_batch = proxy_bridge_receiver.blocking_recv();
            match maybe_packet_batch {
                None => {
                    warn!("[MEV] Exiting proxy bridge, receiver dropped");
                    break;
                }
                Some(packet_batch) => {
                    if let Err(e) = verified_packet_sender.send(packet_batch) {
                        warn!("[MEV] Exiting proxy bridge, receiver dropped: {}", e);
                        break;
                    }
                }
            }
        }
    }

    // convert_packets makes a blocking read from packet_converter_receiver
    // before converting all packets across packet batches in a batch list
    // into a single packet batch with all converted packets, which it sends
    // to the proxy_bridge_sender asynchronously.
    fn convert_packets(
        mut packet_converter_receiver: UnboundedReceiver<Vec<PbPacketBatch>>,
        proxy_bridge_sender: UnboundedSender<Vec<PacketBatch>>,
    ) {
        // In the loop below the None case is failure to read from
        // the validator connection thread, whereas send error is failure to
        // send to proxy bridge. Both should only occur on shutdown.
        loop {
            let maybe_batch_list = packet_converter_receiver.blocking_recv(); // TODO: inner try_recv
            match maybe_batch_list {
                None => {
                    warn!("[MEV] Exiting convert packets, receiver dropped");
                    break;
                }
                Some(batch_list) => {
                    let mut converted_batch_list = Vec::with_capacity(batch_list.len());
                    for batch in batch_list {
                        converted_batch_list.push(PacketBatch {
                            packets: PinnedVec::from_vec(
                                batch
                                    .packets
                                    .into_iter()
                                    .map(proto_packet_to_packet)
                                    .collect(),
                            ),
                        });
                    }
                    // Async send, from unbounded docs:
                    // A send on this channel will always succeed as long as
                    // the receive half has not been closed. If the receiver
                    // falls behind, messages will be arbitrarily buffered.
                    if let Err(e) = proxy_bridge_sender.send(converted_batch_list) {
                        warn!("[MEV] Exiting convert packets, sender dropped: {}", e);
                        break;
                    }
                }
            }
        }
    }

    async fn start_packet_streamer_and_tpu_advertiser(
        client: &mut ValidatorInterfaceClientType,
        tpu_notify_sender: &UnboundedSender<(Option<SocketAddr>, Option<SocketAddr>)>,
        tpu: SocketAddr,
        tpu_fwd: SocketAddr,
    ) -> Result<()> {
        loop {}
        Ok(())
    }

    async fn start_bundle_streamer(client: &mut ValidatorInterfaceClientType) -> Result<()> {
        loop {}
        Ok(())
    }

    async fn run_event_loops(
        validator_interface_address: String,
        auth_interceptor: AuthenticationInjector,
        tpu_notify_sender: &UnboundedSender<(Option<SocketAddr>, Option<SocketAddr>)>,
    ) -> Result<()> {
        let channel = Endpoint::from_shared(validator_interface_address)?
            .connect()
            .await?;
        let mut client = ValidatorInterfaceClient::with_interceptor(channel, auth_interceptor);

        let (tpu, tpu_fwd) = Self::fetch_tpu_config(&mut client).await?;

        let mut handles = vec![];
        handles.push(tokio::spawn(
            Self::start_packet_streamer_and_tpu_advertiser(
                &mut client,
                tpu_notify_sender,
                tpu,
                tpu_fwd,
            ),
        ));
        handles.push(tokio::spawn(Self::start_bundle_streamer(&mut client)));

        futures::future::join_all(handles).await;

        Ok(())
    }

    async fn fetch_tpu_config(
        client: &mut ValidatorInterfaceClient<InterceptedService<Channel, AuthenticationInjector>>,
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

    // This function maintains a connection to the TPU
    // proxy backend. It is long lived and should
    // be called in a spawned thread. On connection
    // it spawns a thread to read from the open connection
    // and async forward to a passed unbounded channel.
    async fn validator_interface_connection_loop(
        interceptor: AuthenticationInjector,
        validator_interface_address: Option<SocketAddr>,
        _packet_converter_sender: UnboundedSender<Vec<PbPacketBatch>>,
        tpu_notify_sender: UnboundedSender<(Option<SocketAddr>, Option<SocketAddr>)>,
        _bundle_sender: Sender<Vec<Bundle>>,
    ) {
        if validator_interface_address.is_none() {
            return;
        }
        let validator_interface_address = validator_interface_address.unwrap();
        let addr = format!("http://{}", validator_interface_address);

        loop {
            let mut backoff = BackoffStrategy::new();

            match Self::run_event_loops(addr.clone(), interceptor.clone(), &tpu_notify_sender).await
            {
                Ok(_) => {
                    backoff.reset();
                }
                Err(e) => {
                    error!("error yo {}", e);
                    sleep(Duration::from_millis(backoff.next_wait())).await;
                }
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        let results = vec![
            self.proxy_bridge_thread.join(),
            self.packet_converter_thread.join(),
            self.validator_interface_thread.join(),
        ];

        if results.iter().all(|t| t.is_ok()) {
            Ok(())
        } else {
            Err(Box::new("failed to join a thread"))
        }
    }
}

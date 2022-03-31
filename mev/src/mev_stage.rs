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
                subscribe_packets_response::Msg,
                validator_interface_client::ValidatorInterfaceClient, GetTpuConfigsRequest,
                SubscribePacketsRequest,
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
    #[error("stream disconnected")]
    PacketStreamDisconnected,
    #[error("bad packet message")]
    BadMessage,
    #[error("error sending message to another part of the system")]
    ChannelError,
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
        heartbeat_sender: Sender<Option<(SocketAddr, SocketAddr)>>,
        bundle_sender: Sender<Vec<Bundle>>,
        i: u64,
    ) -> Self {
        let msg = b"Let's get this money!".to_vec();
        let sig: Signature = keypair.sign_message(msg.as_slice());
        let pubkey = keypair.pubkey();
        let interceptor = AuthenticationInjector::new(msg, sig, pubkey);

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
                    verified_packet_sender,
                    heartbeat_sender,
                    bundle_sender,
                )
            })
        });

        info!("[MEV] Started recv verify stage");

        Self {
            validator_interface_thread,
        }
    }

    async fn packet_streamer_loop(
        mut client: ValidatorInterfaceClient<InterceptedService<Channel, AuthenticationInjector>>,
        heartbeat_sender: Sender<Option<(SocketAddr, SocketAddr)>>,
        tpu: SocketAddr,
        tpu_fwd: SocketAddr,
        verified_packet_sender: Sender<Vec<PacketBatch>>,
    ) -> Result<()> {
        let mut subscription = client
            .subscribe_packets(SubscribePacketsRequest {})
            .await?
            .into_inner();
        loop {
            let response = subscription
                .message()
                .await?
                .ok_or(MevStageError::PacketStreamDisconnected)?;

            match response.msg.ok_or(MevStageError::BadMessage)? {
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
                    verified_packet_sender
                        .send(packet_batches)
                        .map_err(|_| MevStageError::ChannelError)?;
                }
                Msg::Heartbeat(true) => heartbeat_sender
                    .send(Some((tpu.clone(), tpu_fwd.clone())))
                    .map_err(|_| MevStageError::ChannelError)?,
                Msg::Heartbeat(false) => heartbeat_sender
                    .send(None)
                    .map_err(|_| MevStageError::ChannelError)?,
            }
        }
    }

    async fn bundle_streamer(client: ValidatorInterfaceClientType) -> Result<()> {
        loop {}
        Ok(())
    }

    async fn run_event_loops(
        validator_interface_address: String,
        auth_interceptor: AuthenticationInjector,
        heartbeat_sender: Sender<Option<(SocketAddr, SocketAddr)>>,
        verified_packet_sender: Sender<Vec<PacketBatch>>,
    ) -> Result<()> {
        let channel = Endpoint::from_shared(validator_interface_address)?
            .connect()
            .await?;
        let mut client = ValidatorInterfaceClient::with_interceptor(channel, auth_interceptor);

        let (tpu, tpu_fwd) = Self::fetch_tpu_config(&mut client).await?;

        let mut handles = vec![];
        handles.push(tokio::spawn(Self::packet_streamer_loop(
            client.clone(),
            heartbeat_sender,
            tpu,
            tpu_fwd,
            verified_packet_sender,
        )));
        handles.push(tokio::spawn(Self::bundle_streamer(client)));

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
        verified_packet_sender: Sender<Vec<PacketBatch>>,
        heartbeat_sender: Sender<Option<(SocketAddr, SocketAddr)>>,
        _bundle_sender: Sender<Vec<Bundle>>,
    ) {
        if validator_interface_address.is_none() {
            return;
        }
        let validator_interface_address = validator_interface_address.unwrap();
        let addr = format!("http://{}", validator_interface_address);

        loop {
            let mut backoff = BackoffStrategy::new();

            match Self::run_event_loops(
                addr.clone(),
                interceptor.clone(),
                heartbeat_sender.clone(),
                verified_packet_sender.clone(),
            )
            .await
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
        self.validator_interface_thread.join()
    }
}

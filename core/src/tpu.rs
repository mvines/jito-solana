//! The `tpu` module implements the Transaction Processing Unit, a
//! multi-stage transaction processing pipeline in software.

use {
    crate::{
        banking_stage::BankingStage,
        broadcast_stage::{BroadcastStage, BroadcastStageType, RetransmitSlotsReceiver},
        cluster_info_vote_listener::{
            ClusterInfoVoteListener, GossipDuplicateConfirmedSlotsSender,
            GossipVerifiedVoteHashSender, VerifiedVoteSender, VoteTracker,
        },
        fetch_stage::FetchStage,
        find_packet_sender_stake_stage::FindPacketSenderStakeStage,
        sigverify::TransactionSigVerifier,
        sigverify_stage::SigVerifyStage,
    },
    crossbeam_channel::{unbounded, Receiver},
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{blockstore::Blockstore, blockstore_processor::TransactionStatusSender},
    solana_mev::mev_stage::MevStage,
    solana_poh::poh_recorder::{PohRecorder, WorkingBankEntry},
    solana_rpc::{
        optimistically_confirmed_bank_tracker::BankNotificationSender,
        rpc_subscriptions::RpcSubscriptions,
    },
    solana_runtime::{
        bank_forks::BankForks,
        cost_model::CostModel,
        vote_sender_types::{ReplayVoteReceiver, ReplayVoteSender},
    },
    solana_sdk::signature::Keypair,
    std::{
        net::{SocketAddr, UdpSocket},
        sync::{atomic::AtomicBool, Arc, Mutex, RwLock},
        thread,
    },
};

pub const DEFAULT_TPU_COALESCE_MS: u64 = 5;

// allow multiple connections for NAT and any open/close overlap
pub const MAX_QUIC_CONNECTIONS_PER_IP: usize = 8;

const HEARTBEAT_TIMEOUT_MS: u64 = 1500;

pub struct TpuSockets {
    pub transactions: Vec<UdpSocket>,
    pub transaction_forwards: Vec<UdpSocket>,
    pub vote: Vec<UdpSocket>,
    pub broadcast: Vec<UdpSocket>,
    pub transactions_quic: UdpSocket,
}

pub struct Tpu {
    fetch_stage: FetchStage,
    sigverify_stage: SigVerifyStage,
    vote_sigverify_stage: SigVerifyStage,
    mev_stage: MevStage,
    banking_stage: BankingStage,
    cluster_info_vote_listener: ClusterInfoVoteListener,
    broadcast_stage: BroadcastStage,
    tpu_quic_t: thread::JoinHandle<()>,
    find_packet_sender_stake_stage: FindPacketSenderStakeStage,
    vote_find_packet_sender_stake_stage: FindPacketSenderStakeStage,
}

impl Tpu {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        entry_receiver: Receiver<WorkingBankEntry>,
        retransmit_slots_receiver: RetransmitSlotsReceiver,
        sockets: TpuSockets,
        subscriptions: &Arc<RpcSubscriptions>,
        transaction_status_sender: Option<TransactionStatusSender>,
        blockstore: &Arc<Blockstore>,
        broadcast_type: &BroadcastStageType,
        exit: &Arc<AtomicBool>,
        shred_version: u16,
        vote_tracker: Arc<VoteTracker>,
        bank_forks: Arc<RwLock<BankForks>>,
        verified_vote_sender: VerifiedVoteSender,
        gossip_verified_vote_hash_sender: GossipVerifiedVoteHashSender,
        replay_vote_receiver: ReplayVoteReceiver,
        replay_vote_sender: ReplayVoteSender,
        bank_notification_sender: Option<BankNotificationSender>,
        tpu_coalesce_ms: u64,
        cluster_confirmed_slot_sender: GossipDuplicateConfirmedSlotsSender,
        cost_model: &Arc<RwLock<CostModel>>,
        keypair: &Keypair,
        validator_interface_address: Option<SocketAddr>,
    ) -> Self {
        let TpuSockets {
            transactions: transactions_sockets,
            transaction_forwards: tpu_forwards_sockets,
            vote: tpu_vote_sockets,
            broadcast: broadcast_sockets,
            transactions_quic: transactions_quic_sockets,
        } = sockets;

        let (packet_sender, packet_receiver) = unbounded();
        let (vote_packet_sender, vote_packet_receiver) = unbounded();
        let fetch_stage = FetchStage::new_with_sender(
            transactions_sockets,
            tpu_forwards_sockets,
            tpu_vote_sockets,
            exit,
            &packet_sender,
            &vote_packet_sender,
            poh_recorder,
            tpu_coalesce_ms,
        );

        let (find_packet_sender_stake_sender, find_packet_sender_stake_receiver) = unbounded();

        let find_packet_sender_stake_stage = FindPacketSenderStakeStage::new(
            packet_receiver,
            find_packet_sender_stake_sender,
            bank_forks.clone(),
            cluster_info.clone(),
        );

        let (vote_find_packet_sender_stake_sender, vote_find_packet_sender_stake_receiver) =
            unbounded();

        let vote_find_packet_sender_stake_stage = FindPacketSenderStakeStage::new(
            vote_packet_receiver,
            vote_find_packet_sender_stake_sender,
            bank_forks.clone(),
            cluster_info.clone(),
        );

        let tpu_quic_t = solana_streamer::quic::spawn_server(
            transactions_quic_sockets,
            keypair,
            cluster_info.my_contact_info().tpu.ip(),
            packet_sender,
            exit.clone(),
            MAX_QUIC_CONNECTIONS_PER_IP,
        )
        .unwrap();

        let (verified_sender, verified_receiver) = unbounded();

        // MEV TPU proxy packet injection
        let (bundle_sender, bundle_rx) = unbounded();
        let mev_stage = MevStage::new(
            cluster_info.keypair(),
            validator_interface_address,
            verified_sender.clone(),
            bundle_sender,
            HEARTBEAT_TIMEOUT_MS,
            cluster_info,
        );

        let sigverify_stage = {
            let verifier = TransactionSigVerifier::default();
            SigVerifyStage::new(find_packet_sender_stake_receiver, verified_sender, verifier)
        };

        let (verified_tpu_vote_packets_sender, verified_tpu_vote_packets_receiver) = unbounded();

        let vote_sigverify_stage = {
            let verifier = TransactionSigVerifier::new_reject_non_vote();
            SigVerifyStage::new(
                vote_find_packet_sender_stake_receiver,
                verified_tpu_vote_packets_sender,
                verifier,
            )
        };

        let (verified_gossip_vote_packets_sender, verified_gossip_vote_packets_receiver) =
            unbounded();
        let cluster_info_vote_listener = ClusterInfoVoteListener::new(
            exit.clone(),
            cluster_info.clone(),
            verified_gossip_vote_packets_sender,
            poh_recorder.clone(),
            vote_tracker,
            bank_forks.clone(),
            subscriptions.clone(),
            verified_vote_sender,
            gossip_verified_vote_hash_sender,
            replay_vote_receiver,
            blockstore.clone(),
            bank_notification_sender,
            cluster_confirmed_slot_sender,
        );

        let banking_stage = BankingStage::new(
            cluster_info,
            poh_recorder,
            verified_receiver,
            verified_tpu_vote_packets_receiver,
            verified_gossip_vote_packets_receiver,
            transaction_status_sender,
            replay_vote_sender,
            cost_model.clone(),
            bundle_rx,
        );

        let broadcast_stage = broadcast_type.new_broadcast_stage(
            broadcast_sockets,
            cluster_info.clone(),
            entry_receiver,
            retransmit_slots_receiver,
            exit,
            blockstore,
            &bank_forks,
            shred_version,
        );

        Self {
            fetch_stage,
            sigverify_stage,
            vote_sigverify_stage,
            mev_stage,
            banking_stage,
            cluster_info_vote_listener,
            broadcast_stage,
            tpu_quic_t,
            find_packet_sender_stake_stage,
            vote_find_packet_sender_stake_stage,
        }
    }

    pub fn join(self) -> thread::Result<()> {
        let results = vec![
            self.fetch_stage.join(),
            self.sigverify_stage.join(),
            self.vote_sigverify_stage.join(),
            self.cluster_info_vote_listener.join(),
            self.banking_stage.join(),
            self.find_packet_sender_stake_stage.join(),
            self.vote_find_packet_sender_stake_stage.join(),
            self.mev_stage.join(),
        ];
        self.tpu_quic_t.join()?;
        let broadcast_result = self.broadcast_stage.join();
        for result in results {
            result?;
        }
        let _ = broadcast_result?;
        Ok(())
    }
}

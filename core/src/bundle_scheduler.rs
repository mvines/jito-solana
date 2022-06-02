use {
    crate::{unprocessed_packet_batches, unprocessed_packet_batches::ImmutableDeserializedPacket},
    crossbeam_channel::{bounded, Receiver, RecvError, Sender},
    futures_util::future::err,
    solana_mev::bundle::BundlePacketBatch,
    solana_perf::cuda_runtime::PinnedVec,
    solana_runtime::{accounts::AccountLocks, bank::Bank, contains::Contains},
    solana_sdk::{
        bpf_loader_upgradeable,
        bundle::Bundle,
        feature_set,
        packet::Packet,
        pubkey::Pubkey,
        signature::Signature,
        transaction::{
            AddressLoader, SanitizedTransaction, TransactionAccountLocks, TransactionError,
        },
    },
    std::{
        collections::{
            hash_map::{Entry, RandomState},
            HashMap, HashSet,
        },
        sync::Arc,
    },
};

pub struct BundleScheduler {
    unlocked_bundle_receiver: Receiver<BundlePacketBatch>,
    // bounded channels used to pre-lock accounts
    locked_bundle_sender: Sender<BundlePacketBatch>,
    locked_bundle_receiver: Receiver<BundlePacketBatch>,

    account_locks: HashMap<Pubkey, u64>,
    tx_to_locks: HashMap<Signature, HashSet<Pubkey>>,
}

// this will need to be tuned
const NUM_BUNDLES_LOOKAHEAD: usize = 3;

impl BundleScheduler {
    pub fn new(bundle_packet_receiver: Receiver<BundlePacketBatch>) -> BundleScheduler {
        // as soon as the +1 packet is added, we'll pop it off so the queue will be at length NUM_TXS_LOOKAHEAD
        let (bundle_tx_sender, bundle_tx_receiver) = bounded(NUM_BUNDLES_LOOKAHEAD + 1);
        BundleScheduler {
            unlocked_bundle_receiver: bundle_packet_receiver,
            locked_bundle_sender: bundle_tx_sender,
            locked_bundle_receiver: bundle_tx_receiver,
            account_locks: HashMap::new(),
            tx_to_locks: HashMap::new(),
        }
    }

    /// Receives a bundle and blocks on ensuring
    pub fn recv(&mut self, bank: &Arc<Bank>, tip_program_id: &Pubkey) -> Result<Bundle, RecvError> {
        self.refill_locked_bundles(bank, tip_program_id);

        // pop bundle packets, serialize to transactions, then run validation checks
        // TODO (LB): if there's no packets, this will block forever bc nothing will refill it
        let packet_batch = self.locked_bundle_receiver.recv()?;
        let txs = Self::get_bundle_txs(&packet_batch, bank, tip_program_id);

        // probably want to block until it's unlocked in AccountLocks?

        Ok(Bundle::default())
    }

    pub fn unlock(&mut self, batch: &BundlePacketBatch) {}

    fn prelock_bundle_accounts(
        &mut self,
        batch: &BundlePacketBatch,
        bank: &Arc<Bank>,
        tip_program_id: &Pubkey,
    ) -> Result<(), TransactionError> {
        let transactions = Self::get_bundle_txs(&batch, bank, tip_program_id);
        if transactions.is_empty() || batch.batch.packets.len() != transactions.len() {
            warn!(
                "error deserializing packets, throwing bundle away e: {:?}",
                batch
            );
            return Err(TransactionError::TooManyAccountLocks); // todo need better error message
        }

        // ensure we haven't already locked these and don't end up double-counting something
        let signatures: HashSet<&Signature, RandomState> =
            HashSet::from_iter(transactions.iter().map(|tx| tx.signature()));
        for sig in signatures {
            if self.tx_to_locks.contains(sig) {
                warn!("already locked tx",);
                return Err(TransactionError::AlreadyProcessed);
            }
        }

        let transactions_locks: Vec<HashSet<Pubkey>> = transactions
            .iter()
            .filter_map(|tx| {
                let locks = tx.get_account_locks(&bank.feature_set).ok()?;
                let mut pubkeys = HashSet::new();
                for a in locks.writable {
                    pubkeys.insert(*a);
                }
                for a in locks.readonly {
                    pubkeys.insert(*a);
                }
                Some(pubkeys)
            })
            .collect();
        if transactions_locks.len() != transactions.len() {
            warn!("error locking transactions, throwing bundle away");
            return Err(TransactionError::TooManyAccountLocks); // todo need better error message
        }

        transactions_locks.iter().for_each(|locks| {
            locks.iter().for_each(|a| {
                self.account_locks
                    .entry(*a)
                    .and_modify(|num| *num += 1)
                    .or_insert(1);
            });
        });

        for (tx, locks) in transactions.into_iter().zip(transactions_locks.into_iter()) {
            if let Some(locks) = self.tx_to_locks.insert(*tx.signature(), locks) {
                // this should never happen but we should handle it anyway
                error!("already locked accounts for tx: {}", *tx.signature());
                self.remove_locks(locks);
            }
        }
        Ok(())
    }

    fn remove_locks(&mut self, locks: HashSet<Pubkey, RandomState>) {
        for a in locks {
            match self.account_locks.entry(a) {
                Entry::Occupied(mut e) => {
                    *e.get_mut() -= 1;
                    if e.get() == 0 {
                        e.remove_entry()
                    }
                }
                Entry::Vacant(_) => {
                    panic!("wtf yooo");
                }
            }
        }
    }

    /// Moves bundles off the main queue into a locked bundle queue, which contains bundles
    /// that have all been pre-locked
    fn refill_locked_bundles(&mut self, bank: &Arc<Bank>, tip_program_id: &Pubkey) {
        while self.unlocked_bundle_receiver.len() <= NUM_BUNDLES_LOOKAHEAD {
            if let Ok(batch) = self.unlocked_bundle_receiver.try_recv() {
                match self.prelock_bundle_accounts(&batch, bank, tip_program_id) {
                    Ok(_) => {
                        if let Err(e) = self.locked_bundle_sender.send(batch) {
                            error!("error sending packet into locked bundle queue e: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("error pre-locking accounts: {}", e);
                    }
                }
            } else {
                break;
            }
        }
    }

    fn get_bundle_txs(
        bundle: &BundlePacketBatch,
        bank: &Arc<Bank>,
        tip_program_id: &Pubkey,
    ) -> Vec<SanitizedTransaction> {
        let packet_indexes = Self::generate_packet_indexes(&bundle.batch.packets);
        let deserialized_packets =
            unprocessed_packet_batches::deserialize_packets(&bundle.batch, &packet_indexes, None);

        deserialized_packets
            .filter_map(|p| {
                let immutable_packet = p.immutable_section().clone();
                Self::transaction_from_deserialized_packet(
                    &immutable_packet,
                    &bank.feature_set,
                    bank.vote_only_bank(),
                    bank.as_ref(),
                    tip_program_id,
                )
            })
            .collect()
    }

    // This function deserializes packets into transactions, computes the blake3 hash of transaction
    // messages, and verifies secp256k1 instructions. A list of sanitized transactions are returned
    // with their packet indexes.
    #[allow(clippy::needless_collect)]
    fn transaction_from_deserialized_packet(
        deserialized_packet: &ImmutableDeserializedPacket,
        feature_set: &Arc<feature_set::FeatureSet>,
        votes_only: bool,
        address_loader: impl AddressLoader,
        tip_program_id: &Pubkey,
    ) -> Option<SanitizedTransaction> {
        if votes_only && !deserialized_packet.is_simple_vote() {
            return None;
        }

        let tx = SanitizedTransaction::try_new(
            deserialized_packet.transaction().clone(),
            *deserialized_packet.message_hash(),
            deserialized_packet.is_simple_vote(),
            address_loader,
        )
        .ok()?;
        tx.verify_precompiles(feature_set).ok()?;

        // NOTE: if this is a weak assumption helpful for testing deployment,
        // before production it shall only be the tip program
        let tx_accounts = tx.message().account_keys();
        if tx_accounts.iter().any(|a| a == tip_program_id)
            && !tx_accounts
                .iter()
                .any(|a| a == &bpf_loader_upgradeable::id())
        {
            warn!("someone attempted to change the tip program!! tx: {:?}", tx);
            return None;
        }

        Some(tx)
    }

    fn generate_packet_indexes(vers: &PinnedVec<Packet>) -> Vec<usize> {
        vers.iter()
            .enumerate()
            .filter(|(_, pkt)| !pkt.meta.discard())
            .map(|(index, _)| index)
            .collect()
    }
}

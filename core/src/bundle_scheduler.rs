//! The `bundle_scheduler` module is responsible for handling locking and scheduling around bundles.
//!
//! The scheduler has two main features:
//! - prevent BankingStage from executing a transaction that contains account overlap with a
//!   transaction in an executing bundle.
//! - minimize BundleStage from being blocked from execution by AccountInUse from BankingStage. It
//!   does this by looking ahead N bundle batches and pre-locking those accounts.
//!
//! On the first point: imagine we have a bundle of three transactions which write lock the following
//! accounts:
//! tx0: {A, B}
//! tx1: {B, C}
//! tx2: {C, D}
//!
//! BundleStage will not commit results of tx0, tx1, and tx2 until they're all done executing. If
//! BundleStage is executing tx2 with accounts C & D and BankingStage concurrently
//! executes and commits a transaction that writes to account B, then the results of this bundle
//! may be invalid, causing the validator to produce an invalid block.
//!
//! The bundle scheduler prevents this by pre-locking all transactions in a bundle and BankingStage
//! will reference these pre-locks when determining what to execute.
use {
    crate::{unprocessed_packet_batches, unprocessed_packet_batches::ImmutableDeserializedPacket},
    crossbeam_channel::{bounded, Receiver, RecvError, Sender, TryRecvError},
    solana_mev::bundle::{BundlePacketBatch, SanitizedBundle},
    solana_perf::cuda_runtime::PinnedVec,
    solana_runtime::{bank::Bank, contains::Contains},
    solana_sdk::{
        bpf_loader_upgradeable,
        feature_set::FeatureSet,
        packet::Packet,
        pubkey::Pubkey,
        signature::Signature,
        transaction::{self, AddressLoader, SanitizedTransaction, TransactionError},
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
    unlocked_bundle_receiver: Receiver<Vec<BundlePacketBatch>>,
    // bounded channels used to pre-lock accounts
    locked_bundle_sender: Sender<Vec<BundlePacketBatch>>,
    locked_bundle_receiver: Receiver<Vec<BundlePacketBatch>>,

    account_locks: HashMap<Pubkey, u64>,
    tx_to_accounts: HashMap<Signature, HashSet<Pubkey>>,
}

// this will need to be tuned
const NUM_BATCHES_LOOKAHEAD: usize = 3;

impl BundleScheduler {
    pub fn new(bundle_packet_receiver: Receiver<Vec<BundlePacketBatch>>) -> BundleScheduler {
        // as soon as the +1 packet is added, we'll pop it off so the queue will be at length NUM_TXS_LOOKAHEAD
        let (bundle_tx_sender, bundle_tx_receiver) = bounded(NUM_BATCHES_LOOKAHEAD + 1);
        BundleScheduler {
            unlocked_bundle_receiver: bundle_packet_receiver,
            locked_bundle_sender: bundle_tx_sender,
            locked_bundle_receiver: bundle_tx_receiver,
            account_locks: HashMap::new(),
            tx_to_accounts: HashMap::new(),
        }
    }

    /// Receive a vector of sanitized bundles
    /// NOTE: If a bundle contains a transaction that changes the addresses in lookup table
    /// and then references that lookup table in a later transaction, the executed bundle will
    /// return an invalid block. This is because we turn Packets to SanitizedTransactions at the
    /// very beginning as opposed to iteratively converting Packets to SanitizedTransactions.
    pub fn recv(
        &mut self,
        bank: &Arc<Bank>,
        tip_program_id: &Pubkey,
    ) -> Result<Vec<SanitizedBundle>, RecvError> {
        self.refill_locked_bundles(bank, tip_program_id);

        let packet_batches = self.locked_bundle_receiver.recv()?;
        let bundles = packet_batches
            .into_iter()
            .filter_map(|packet_batch| {
                let transactions = Self::get_bundle_txs(&packet_batch, bank, tip_program_id);
                if transactions.is_empty() || packet_batch.batch.packets.len() != transactions.len()
                {
                    warn!(
                        "error deserializing packets, throwing bundle away e: {:?}",
                        packet_batch
                    );
                    // TODO (LB): what if it previously was considered a TX but isn't now?
                    for tx in transactions {
                        // ensure we drop locks if we're dropping bundles too
                        if let Some(locks) = self.tx_to_accounts.remove(tx.signature()) {
                            self.remove_locks(&locks);
                        }
                    }
                    return None;
                }

                let transactions_locks: Vec<HashSet<Pubkey>> =
                    Self::get_accounts(&transactions, &bank.feature_set);

                for (tx, new_locks) in transactions.iter().zip(transactions_locks.into_iter()) {
                    let maybe_new_locks = match self.tx_to_accounts.entry(*tx.signature()) {
                        Entry::Occupied(e) => {
                            if e.get() != &new_locks {
                                Some(new_locks)
                            } else {
                                None
                            }
                        }
                        Entry::Vacant(_) => None,
                    };
                    if let Some(new_locks) = maybe_new_locks {
                        let thing = self.tx_to_accounts.get(tx.signature()).unwrap().clone();
                        self.remove_locks(&thing);

                        self.tx_to_accounts
                            .insert(*tx.signature(), new_locks.clone());
                        self.add_locks(&[new_locks]);
                    }
                }

                Some(SanitizedBundle { transactions })
            })
            .collect();

        // TODO: block until all accounts are unlocked in bank's TransactionAccountsLock

        Ok(bundles)
    }

    /// Unlocks the pre-lock accounts in this batch. This should be called from bundle_stage.
    pub fn unlock(&mut self, bundle: &SanitizedBundle) {
        for tx in &bundle.transactions {
            let accounts = self.tx_to_accounts.get(tx.signature()).cloned();
            if let Some(accounts) = accounts {
                self.remove_locks(&accounts);
                self.tx_to_accounts.remove(tx.signature());
            }
        }
        if self.locked_bundle_receiver.is_empty() {
            // TODO: remove asserts
            assert!(self.tx_to_accounts.is_empty());
            assert!(self.account_locks.is_empty());
        }
    }

    /// Runs list of sanitized transactions by to determine if they're pre-locked
    pub fn get_lock_results(
        &self,
        transactions: &[SanitizedTransaction],
    ) -> Vec<transaction::Result<()>> {
        transactions
            .iter()
            .map(|tx| {
                for acc in tx.message.account_keys().iter() {
                    if self.account_locks.contains_key(acc) {
                        return Err(TransactionError::AccountInUse);
                    }
                }
                Ok(())
            })
            .collect()
    }

    fn prelock_bundle_accounts(
        &mut self,
        batches: &[BundlePacketBatch],
        bank: &Arc<Bank>,
        tip_program_id: &Pubkey,
    ) {
        for batch in batches {
            let transactions = Self::get_bundle_txs(&batch, bank, tip_program_id);
            if transactions.is_empty() || batch.batch.packets.len() != transactions.len() {
                warn!(
                    "error deserializing packets, throwing bundle away e: {:?}",
                    batch
                );
                continue;
            }

            // ensure we haven't already locked these and don't end up double-counting something
            let signatures: HashSet<&Signature, RandomState> =
                HashSet::from_iter(transactions.iter().map(|tx| tx.signature()));
            for sig in signatures {
                if self.tx_to_accounts.contains(sig) {
                    warn!("already locked tx",);
                    continue;
                }
            }

            let transactions_locks: Vec<HashSet<Pubkey>> =
                Self::get_accounts(&transactions, &bank.feature_set);
            if transactions_locks.len() != transactions.len() {
                warn!("error locking transactions, throwing bundle away");
                continue;
            }

            self.add_locks(&transactions_locks);
            for (tx, locks) in transactions.into_iter().zip(transactions_locks.into_iter()) {
                self.tx_to_accounts.insert(*tx.signature(), locks);
            }
        }
    }

    fn add_locks(&mut self, transactions_locks: &[HashSet<Pubkey>]) {
        transactions_locks.iter().for_each(|locks| {
            locks.iter().for_each(|a| {
                self.account_locks
                    .entry(*a)
                    .and_modify(|num| *num += 1)
                    .or_insert(1);
            });
        });
    }

    fn get_accounts(
        transactions: &[SanitizedTransaction],
        feature_set: &FeatureSet,
    ) -> Vec<HashSet<Pubkey>> {
        transactions
            .iter()
            .filter_map(|tx| {
                let locks = tx.get_account_locks(feature_set).ok()?;
                let mut pubkeys = HashSet::new();
                for a in locks.writable {
                    pubkeys.insert(*a);
                }
                for a in locks.readonly {
                    pubkeys.insert(*a);
                }
                Some(pubkeys)
            })
            .collect()
    }

    fn remove_locks(&mut self, locks: &HashSet<Pubkey, RandomState>) {
        for a in locks {
            match self.account_locks.entry(*a) {
                Entry::Occupied(mut e) => {
                    *e.get_mut() -= 1;
                    if e.get() == &0_u64 {
                        e.remove_entry();
                    }
                }
                Entry::Vacant(_) => {
                    panic!("expected lock to be present for account: {:?}", a);
                }
            }
        }
    }

    /// Moves bundles off the main queue into a locked bundle queue, which contains bundles
    /// that have all been pre-locked
    fn refill_locked_bundles(&mut self, bank: &Arc<Bank>, tip_program_id: &Pubkey) {
        while self.unlocked_bundle_receiver.len() <= NUM_BATCHES_LOOKAHEAD {
            // if there's no locked or pending bundles, want to block on unlocked bundles. Once there
            // are locked bundles, we don't need to block and can proceed.
            let batches = if self.locked_bundle_receiver.is_empty() {
                self.unlocked_bundle_receiver
                    .recv()
                    .map_err(|_| TryRecvError::Disconnected)
            } else {
                self.unlocked_bundle_receiver.try_recv()
            };

            if let Ok(batches) = batches {
                self.prelock_bundle_accounts(&batches, bank, tip_program_id);
                if let Err(e) = self.locked_bundle_sender.send(batches) {
                    error!("error sending packet into locked bundle queue e: {}", e);
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
        feature_set: &Arc<FeatureSet>,
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

#[cfg(test)]
mod test {
    use {
        crate::bundle_scheduler::BundleScheduler,
        bincode::serialize,
        crossbeam_channel::unbounded,
        solana_mev::bundle::BundlePacketBatch,
        solana_perf::packet::PacketBatch,
        solana_runtime::bank::Bank,
        solana_sdk::{
            hash::Hash,
            packet::Packet,
            signature::{Keypair, Signer},
            system_transaction::transfer,
            transaction::VersionedTransaction,
        },
        std::sync::Arc,
    };

    #[test]
    fn test_get_bundle() {
        let tip_program = Keypair::new().pubkey();
        let bank = Arc::new(Bank::default_for_tests());

        let kp1 = Keypair::new();
        let kp2 = Keypair::new();
        let tx0 = VersionedTransaction::from(transfer(&kp1, &kp1.pubkey(), 1, Hash::default()));
        let tx1 = VersionedTransaction::from(transfer(&kp2, &kp2.pubkey(), 1, Hash::default()));

        let (bundle_packet_sender, bundle_packet_receiver) = unbounded();
        let mut scheduler = BundleScheduler::new(bundle_packet_receiver);

        let batch = BundlePacketBatch {
            batch: PacketBatch::new(vec![
                Packet::from_data(None, &tx0).unwrap(),
                Packet::from_data(None, &tx1).unwrap(),
            ]),
        };

        bundle_packet_sender.send(vec![batch]).unwrap();

        let bundles = scheduler.recv(&bank, &tip_program).unwrap();
        assert_eq!(bundles.len(), 1);
        assert_eq!(*bundles[0].transactions[0].signature(), tx0.signatures[0]);
        assert_eq!(*bundles[0].transactions[1].signature(), tx1.signatures[0]);
        for bundle in bundles {
            scheduler.unlock(&bundle);
        }
    }
}

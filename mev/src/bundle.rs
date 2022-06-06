use {solana_perf::packet::PacketBatch, solana_sdk::transaction::SanitizedTransaction};

#[derive(Clone, Debug)]
pub struct BundlePacketBatch {
    pub batch: PacketBatch,
}

#[derive(Clone, Debug)]
pub struct SanitizedBundle {
    pub transactions: Vec<SanitizedTransaction>,
}

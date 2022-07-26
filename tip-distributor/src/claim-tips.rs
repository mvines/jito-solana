//! TODO COMMENT

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use log::info;
use solana_tip_distributor::GeneratedMerkleTreeCollection;
use clap::Parser;
use thiserror::Error as ThisError;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to JSON file containing the [GeneratedMerkleTreeCollection] object.
    #[clap(long, env)]
    generated_merkle_tree_path: PathBuf,
}

#[derive(ThisError, Debug)]
pub enum Error {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),
}

fn main() {
    env_logger::init();
    info!("Claiming tips...");

    let args: Args = Args::parse();
    let tree = read_from_json_file(args.generated_merkle_tree_path).expect("to read json file");
    info!("tree: {:?}", tree);
}

// TODO: ugly
fn read_from_json_file(
    file_path: PathBuf,
) -> Result<GeneratedMerkleTreeCollection, Error> {
    let file = File::open(file_path)?;
    let mut buf_reader = BufReader::new(file);
    let mut s = String::new();
    buf_reader.read_to_string(&mut s)?;
    Ok(serde_json::from_str(&*s)?)
}

//pub struct ClaimArgs {
//    pub proof: Vec<[u8; 32]>,
//    pub amount: u64,
//    pub index: u64,
//    pub bump: u8,
//}
//pub struct ClaimAccounts {
//    pub config: Pubkey,
//    pub tip_distribution_account: Pubkey,
//    pub claim_status: Pubkey,
//    pub claimant: Pubkey,
//    pub payer: Pubkey,
//    pub system_program: Pubkey,
//}
//pub fn claim_ix(program_id: Pubkey, args: ClaimArgs, accounts: ClaimAccounts) -> Instruction {

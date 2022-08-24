use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use log::info;
use tip_distribution::sdk::instruction::{set_merkle_root_upload_authority_ix, SetMerkleRootUploadAuthorityAccounts, SetMerkleRootUploadAuthorityArgs};
use clap::Parser;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
use solana_sdk::signature::read_keypair_file;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use solana_tip_distributor::merkle_root_generator_workflow::execute_transactions;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// TODO
    #[clap(long, env)]
    signer: PathBuf,

    /// TODO
    #[clap(long, env)]
    tip_distribution_account: String,

    /// TODO
    #[clap(long, env)]
    new_authority: String,

    /// TODO
    #[clap(long, env)]
    program_id: String,

    /// The RPC to fetch lamports from for the tip distribution accounts.
    #[clap(long, env)]
    rpc_url: String,
}

fn main() {
    env_logger::init();
    info!("Updating merkle root authority...");

    let args: Args = Args::parse();
    let program_id = Pubkey::from_str(&*args.program_id).unwrap();
    let new_authority = Pubkey::from_str(&*args.new_authority).unwrap();
    let tip_distribution_account = Pubkey::from_str(&*args.tip_distribution_account).unwrap();

    let signer =
        read_keypair_file(&args.signer).expect("Failed to read signer keypair file.");
    let signer_pubkey = signer.pubkey();

    let rpc_client = RpcClient::new(args.rpc_url);
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let recent_blockhash = runtime.block_on(rpc_client.get_latest_blockhash()).unwrap();

    // Create instruction.
    let ix = set_merkle_root_upload_authority_ix(
        program_id,
        SetMerkleRootUploadAuthorityArgs{
            new_merkle_root_upload_authority: new_authority,
        },
        SetMerkleRootUploadAuthorityAccounts {
            tip_distribution_account,
            signer: signer_pubkey,
        },
    );

    let tx = Transaction::new_signed_with_payer(
        &[ix],
        Some(&signer_pubkey),
        &[&signer],
        recent_blockhash,
    );
    execute_transactions(Arc::new(rpc_client), vec![tx]);
}
//! This binary is used to update the merkle root authority in case of mistaken
//! configuration or transfer of ownership.

use {
    clap::Parser,
    log::{error, info},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_core::tip_manager::{TipDistributionAccountConfig, TipManager, TipManagerConfig},
    solana_program::pubkey::Pubkey,
    solana_sdk::{
        signature::{read_keypair_file, KeypairInsecureClone, Signature},
        transaction::Transaction,
    },
    std::{path::PathBuf, str::FromStr, sync::Arc, thread::sleep, time::Duration},
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Program ID for tip payment program
    #[clap(long, env)]
    tip_payment_program_id: String,

    /// Program ID for tip distribution program
    #[clap(long, env)]
    tip_distribution_program_id: String,

    /// Keypair for tip distribution account config payer
    #[clap(long, env)]
    payer_keypair: PathBuf,

    /// Pubkey for merkle root upload authority
    #[clap(long, env)]
    merkle_root_upload_authority: String,

    /// Vote account
    #[clap(long, env)]
    vote_account: String,

    /// Commission bps
    #[clap(long, env)]
    commission_bps: u16,

    /// RPC URL
    #[clap(long, env)]
    rpc_url: String,

    /// Initialize tip payment program
    #[clap(long, env)]
    initialize_tip_payment_program: bool,

    /// Initialize tip payment program
    #[clap(long, env)]
    initialize_tip_distribution_config: bool,
}

const DELAY: Duration = Duration::from_millis(500);
const MAX_RETRIES: usize = 5;

fn main() {
    env_logger::init();

    let args: Args = Args::parse();
    if !args.initialize_tip_payment_program && !args.initialize_tip_distribution_config {
        error!("Nothing to do, pass one of --initialize-tip-payment-program or --initialize-tip-distribution-config");
        return;
    }
    info!("Initializing tip programs...");
    let payer_kp = read_keypair_file(&args.payer_keypair).unwrap();

    let tip_manager_config = TipManagerConfig {
        tip_payment_program_id: Pubkey::from_str(&args.tip_payment_program_id).unwrap(),
        tip_distribution_program_id: Pubkey::from_str(&args.tip_distribution_program_id).unwrap(),
        tip_distribution_account_config: TipDistributionAccountConfig {
            payer: Arc::new(payer_kp.clone()),
            merkle_root_upload_authority: Pubkey::from_str(&args.merkle_root_upload_authority)
                .unwrap(),
            vote_account: Pubkey::from_str(&args.vote_account).unwrap(),
            commission_bps: args.commission_bps,
        },
    };
    let tip_manager = TipManager::new(tip_manager_config);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    if args.initialize_tip_payment_program {
        let rpc_client = RpcClient::new(args.rpc_url.clone());
        let recent_blockhash = runtime.block_on(rpc_client.get_latest_blockhash()).unwrap();
        let tip_payment_tx =
            tip_manager.initialize_tip_payment_program_unsanitized_tx(recent_blockhash, &payer_kp);
        info!("sending tip payment initialization tx");
        runtime.spawn(async move {
            if let Err(e) =
                send_transaction_with_retry(rpc_client, &tip_payment_tx, DELAY, MAX_RETRIES).await
            {
                error!(
                    "error sending transaction [signature={}, error={}]",
                    tip_payment_tx.signatures[0], e
                );
            } else {
                info!(
                    "successfully sent transaction: [signature={}]",
                    tip_payment_tx.signatures[0]
                );
            }
        });
    }

    if args.initialize_tip_distribution_config {
        let rpc_client = RpcClient::new(args.rpc_url);
        let recent_blockhash = runtime.block_on(rpc_client.get_latest_blockhash()).unwrap();
        let tip_distribution_tx = tip_manager
            .initialize_tip_distribution_config_unsanitized_tx(recent_blockhash, &payer_kp);
        info!("sending tip distribution config tx");
        runtime.spawn(async move {
            if let Err(e) =
                send_transaction_with_retry(rpc_client, &tip_distribution_tx, DELAY, MAX_RETRIES)
                    .await
            {
                error!(
                    "error sending transaction [signature={}, error={}]",
                    tip_distribution_tx.signatures[0], e
                );
            } else {
                info!(
                    "successfully sent transaction: [signature={}]",
                    tip_distribution_tx.signatures[0]
                );
            }
        });
    }
}

async fn send_transaction_with_retry(
    rpc_client: RpcClient,
    tx: &Transaction,
    delay: Duration,
    max_retries: usize,
) -> solana_client::client_error::Result<Signature> {
    let mut retry_count: usize = 0;
    loop {
        match rpc_client.send_and_confirm_transaction(tx).await {
            Ok(sig) => {
                return Ok(sig);
            }
            Err(e) => {
                retry_count = retry_count.checked_add(1).unwrap();
                if retry_count == max_retries {
                    return Err(e);
                }
                sleep(delay);
            }
        }
    }
}

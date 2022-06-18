use {
    log::{error, info},
    solana_sdk::clock::Slot,
    solana_transaction_status::ConfirmedBlock,
    std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        thread::{self, sleep},
        time::{Duration, Instant},
    },
    tokio::task::JoinHandle,
};

fn main() {
    env_logger::init();

    let num_tasks = 128;
    let lowest_slot: Slot = 50_000_000;
    let highest_slot: Slot = 135_000_000;
    let task_unit = (highest_slot - lowest_slot) / num_tasks;
    let log_duration = Duration::from_secs(1);

    let test_duration = Duration::from_secs(600);

    info!("Benchmarking performance of stream_confirmed_blocks_with_data",);

    let total_blocks_read = Arc::new(AtomicUsize::new(0));

    let thread = {
        let total_blocks_read = total_blocks_read.clone();
        thread::spawn(move || {
            let test_start = Instant::now();

            let mut last_update_time = Instant::now();
            let mut last_update_count = 0;

            while test_start.elapsed() < test_duration {
                let elapsed = last_update_time.elapsed();
                if elapsed > log_duration {
                    let total_blocks_read = total_blocks_read.load(Ordering::Relaxed);
                    let blocks_received = total_blocks_read - last_update_count;
                    info!(
                            "[tasks={}, blocks_received={}, elapsed={:.2}, blocks/s={:?}, total_blocks_read={}]",
                            num_tasks,
                            blocks_received,
                            elapsed.as_secs_f32(),
                            blocks_received as f64 / elapsed.as_secs_f64(),
                            total_blocks_read
                        );

                    last_update_time = Instant::now();
                    last_update_count = total_blocks_read;
                }

                sleep(Duration::from_millis(100));
            }
        })
    };

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let tasks: Vec<JoinHandle<()>> = (0..num_tasks)
            .map(|i| {
                let total_blocks_read = total_blocks_read.clone();
                runtime.spawn(async move {
                    let bigtable = solana_storage_bigtable::LedgerStorage::new(true, None, None)
                        .await
                        .expect("connected to bigtable");

                    let start = Instant::now();
                    let starting_slot = (task_unit * i) + lowest_slot;
                    let ending_slot = starting_slot + task_unit;

                    // NOTE: this can't be too big!
                    let slots: Vec<_> = (starting_slot..starting_slot + 10_000).collect();
                    let mut receiver = bigtable
                        .stream_confirmed_blocks_with_data(starting_slot, task_unit as usize)
                        .await
                        .expect("start stream");

                    while start.elapsed() < test_duration {
                        if let Some(Ok((slot, block))) = receiver.recv().await {
                            total_blocks_read.fetch_add(1, Ordering::Relaxed);
                        } else {
                            error!("got error!");
                            break;
                        }
                    }
                })
            })
            .collect();
        let mut results = Vec::new();
        for t in tasks {
            let r = t.await.expect("results fetched");
            results.push(r);
        }
    });

    thread.join().unwrap();
}

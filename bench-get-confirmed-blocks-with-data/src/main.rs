use {
    log::info,
    solana_sdk::clock::Slot,
    solana_transaction_status::ConfirmedBlock,
    std::{
        sync::{Arc, Mutex},
        thread::{self, sleep},
        time::{Duration, Instant},
    },
    tokio::task::JoinHandle,
};

fn main() {
    env_logger::init();

    let num_blocks_to_fetch: Vec<u64> = vec![10, 25, 50, 100, 200, 300, 400, 500];
    let num_tasks = 128;
    let lowest_slot: Slot = 100_000_000;
    let highest_slot: Slot = 137_000_000;
    let task_unit = (highest_slot - lowest_slot) / num_tasks;
    let log_duration = Duration::from_secs(1);

    let test_duration = Duration::from_secs(30);

    for chunk_size in num_blocks_to_fetch {
        info!(
            "Benchmarking performance of get_confirmed_blocks_with_data for {:?} blocks",
            chunk_size
        );

        let total_blocks_read = Arc::new(Mutex::new(0));

        let thread = {
            let total_blocks_read = total_blocks_read.clone();
            thread::spawn(move || {
                let test_start = Instant::now();

                let mut last_update_time = Instant::now();
                let mut last_update_count = 0;

                while test_start.elapsed() < test_duration {
                    let elapsed = last_update_time.elapsed();
                    if elapsed > log_duration {
                        let total_blocks_read = *total_blocks_read.lock().unwrap();
                        let blocks_received = total_blocks_read - last_update_count;
                        info!(
                            "[tasks={}, chunk_size={}, blocks_received={}, elapsed={:.2}, blocks/s={:?}]",
                            num_tasks,
                            chunk_size,
                            blocks_received,
                            elapsed.as_secs_f32(),
                            blocks_received as f64 / elapsed.as_secs_f64()
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
                        let bigtable =
                            solana_storage_bigtable::LedgerStorage::new(true, None, None)
                                .await
                                .expect("connected to bigtable");

                        let start = Instant::now();
                        let mut starting_slot = (task_unit * i) + lowest_slot;

                        while start.elapsed() < test_duration {
                            let slot_requests: Vec<_> = (starting_slot
                                ..starting_slot.checked_add(chunk_size).unwrap_or(u64::MAX))
                                .collect();
                            let slots_blocks: Vec<(Slot, ConfirmedBlock)> = bigtable
                                .get_confirmed_blocks_with_data(slot_requests.as_slice())
                                .await
                                .expect("got blocks")
                                .collect();
                            starting_slot = slots_blocks.last().unwrap().0;
                            *total_blocks_read.lock().unwrap() += slots_blocks.len();
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
}

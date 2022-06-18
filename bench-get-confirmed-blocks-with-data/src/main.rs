use {
    solana_sdk::clock::Slot,
    std::time::{Duration, Instant},
    tokio::task::JoinHandle,
};

fn main() {
    let num_blocks_to_fetch: Vec<u64> = vec![100, 250, 500];
    let num_tasks = 128;

    for limit in num_blocks_to_fetch {
        println!(
            "Benchmarking performance of get_confirmed_blocks_with_data for {:?} blocks",
            limit
        );
        let highest_slot: Slot = 137_000_000; // recent slots are more uniform; genesis slots are tiny
        let lowest_slot: Slot = 100_000_000;
        let task_unit = (highest_slot - lowest_slot) / num_tasks;

        let test_duration = Duration::from_secs(30);

        let start = Instant::now();
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let results: Vec<usize> = runtime.block_on(async {
            let tasks: Vec<JoinHandle<usize>> = (0..num_tasks)
                .map(|i| {
                    runtime.spawn(async move {
                        let bigtable =
                            solana_storage_bigtable::LedgerStorage::new(true, None, None)
                                .await
                                .expect("connected to bigtable");

                        let start = Instant::now();
                        let mut num_blocks_fetched = 0;
                        let mut starting_slot = (task_unit * i) + lowest_slot;

                        while start.elapsed() < test_duration {
                            let slot_requests: Vec<_> = (starting_slot
                                ..starting_slot.checked_add(limit).unwrap_or(u64::MAX))
                                .collect();
                            // println!(
                            //     "task {} fetching slots ({}, {})",
                            //     i,
                            //     slot_requests.first().unwrap(),
                            //     slot_requests.last().unwrap()
                            // );
                            num_blocks_fetched += bigtable
                                .get_confirmed_blocks_with_data(slot_requests.as_slice())
                                .await
                                .expect("got blocks")
                                .count();
                            starting_slot += limit;
                        }
                        num_blocks_fetched
                    })
                })
                .collect();
            let mut results = Vec::new();
            for t in tasks {
                let r = t.await.expect("results fetched");
                results.push(r);
            }
            results
        });
        let elapsed = start.elapsed();
        let num_blocks = results.iter().sum::<usize>();
        println!(
            "results [tasks={}, chunk={}, returned={}, elapsed={:.2}, blocks/s={:?}]",
            num_tasks,
            limit,
            num_blocks,
            elapsed.as_secs_f32(),
            num_blocks as f64 / elapsed.as_secs_f64()
        );
    }
}

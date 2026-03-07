use journal::{Journal, JournalRecord, JournalRecordKind};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use storage::lsm_tree::LsmTree;

const NUM_WRITERS: usize = 10;
const ENTRIES_PER_WRITER: usize = 1500;
const NUM_READERS: usize = 4;
const READER_PROBES: usize = 2000;

/// Demonstrates WAL + LSM tree working together with concurrent reads and writes.
///
/// The write path:
///   1. Encode the operation as a JournalRecord
///   2. Append to WAL (fsync) — this is the durability guarantee
///   3. Insert into the LSM tree memtable — fast, in-memory
///
/// On crash recovery (not shown here), you'd replay the WAL to rebuild the memtable
/// for any entries that weren't flushed to SSTables yet.
///
/// The read path hits (in order):
///   1. Active memtable (in-memory BTreeMap)
///   2. Frozen memtable (if a flush is in progress)
///   3. Level-0 SSTables (most recent first)
///   4. Level-1 SSTables (compacted)
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp_dir = std::env::temp_dir().join(format!("norns-db-example-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir)?;

    let data_dir = tmp_dir.join("lsm_data");
    let wal_path = tmp_dir.join("wal.log");

    println!("Data directory: {}", tmp_dir.display());

    // -- Initialize the LSM tree and WAL --
    let lsm = Arc::new(LsmTree::<String, String>::new(
        data_dir.to_string_lossy().to_string(),
        100,  // memtable_size: flush to SSTable every 100 entries
        4,    // level_0_size: compact after 4 L0 SSTables
        4096, // ss_table_block_size
    )?);

    let journal: Arc<Journal<String, String>> = Arc::new(Journal::new(&wal_path)?);

    println!(
        "Starting {NUM_WRITERS} writers x {ENTRIES_PER_WRITER} entries, \
         {NUM_READERS} concurrent readers x {READER_PROBES} probes\n"
    );

    let writes_done = Arc::new(AtomicUsize::new(0));

    // -- Spawn concurrent writers --
    // Each writer owns a range of keys and follows the WAL-first protocol.
    let mut writer_handles = vec![];
    for writer_id in 0..NUM_WRITERS {
        let lsm = lsm.clone();
        let journal = journal.clone();
        let writes_done = writes_done.clone();

        let handle = tokio::spawn(async move {
            for i in 0..ENTRIES_PER_WRITER {
                let key = format!("key_{writer_id}_{i}");
                let value = format!("value_{writer_id}_{i}");

                // Step 1: Append to WAL (durable write, survives crashes)
                let record = JournalRecord::new(JournalRecordKind::Upsert {
                    key: key.clone(),
                    value: value.clone(),
                });
                journal.append(&record).await.expect("WAL append failed");

                // Step 2: Insert into LSM tree memtable (fast, in-memory)
                // This may trigger a flush to SSTable if the memtable is full.
                lsm.insert(key, value).expect("LSM insert failed");

                writes_done.fetch_add(1, Ordering::Relaxed);
            }
            println!("[writer-{writer_id}] done ({ENTRIES_PER_WRITER} entries written)");
        });
        writer_handles.push(handle);
    }

    // -- Spawn concurrent readers --
    // Readers probe random keys while writers are still active.
    // Some probes will miss (key not yet written) — that's expected.
    let mut reader_handles = vec![];
    for reader_id in 0..NUM_READERS {
        let lsm = lsm.clone();
        let writes_done = writes_done.clone();

        let handle = tokio::task::spawn_blocking(move || {
            let mut found = 0;
            let mut not_found = 0;

            // Wait until at least some writes have landed so we observe actual concurrency
            while writes_done.load(Ordering::Relaxed) == 0 {
                std::thread::yield_now();
            }

            for probe in 0..READER_PROBES {
                let writer_id = probe % NUM_WRITERS;
                let key_id = probe % ENTRIES_PER_WRITER;
                let key = format!("key_{writer_id}_{key_id}");

                match lsm.get(&key) {
                    Ok(Some(_)) => found += 1,
                    Ok(None) => not_found += 1,
                    Err(e) => eprintln!("[reader-{reader_id}] error: {e}"),
                }
            }

            let total_writes = writes_done.load(Ordering::Relaxed);
            println!(
                "[reader-{reader_id}] done — found={found}, not_found={not_found} \
                 (writes at time of finish: {total_writes}/{})",
                NUM_WRITERS * ENTRIES_PER_WRITER,
            );
        });
        reader_handles.push(handle);
    }

    // -- Wait for everything --
    for h in writer_handles {
        h.await?;
    }
    for h in reader_handles {
        h.await?;
    }

    // -- Verify all data is present --
    println!("\nAll tasks finished. Verifying data integrity...");

    let mut verified = 0;
    for writer_id in 0..NUM_WRITERS {
        for i in 0..ENTRIES_PER_WRITER {
            let key = format!("key_{writer_id}_{i}");
            let expected = format!("value_{writer_id}_{i}");
            let actual = lsm
                .get(&key)?
                .unwrap_or_else(|| panic!("missing key: {key}"));
            assert_eq!(actual, expected, "mismatch for {key}");
            verified += 1;
        }
    }
    println!("All {verified} entries verified OK.");

    // -- Demonstrate delete through WAL --
    println!("\nDeleting key_0_0 via WAL...");
    let del_record = JournalRecord::new(JournalRecordKind::Delete::<String, String> {
        key: "key_0_0".to_string(),
    });
    journal.append(&del_record).await?;
    lsm.delete("key_0_0".to_string())?;
    assert!(lsm.get(&"key_0_0".to_string())?.is_none());
    println!("key_0_0 deleted and confirmed absent.");

    // -- Final flush --
    println!("\nFlushing remaining memtable to SSTables...");
    lsm.flush()?;
    println!("Done. WAL file: {}", wal_path.display());

    // Cleanup
    std::fs::remove_dir_all(&tmp_dir)?;

    Ok(())
}

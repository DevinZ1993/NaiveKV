pub mod catalog;
pub mod logger;
mod memtable;
pub mod protos;
mod sstable;
pub mod thread_pool;
pub mod types;
pub mod utils;

use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use crate::catalog::{Catalog, CatalogViewer};
use crate::memtable::Memtable;
use crate::sstable::SSTable;
use crate::types::Result;

/// The facade of the storage engine.
pub struct NaiveKV {
    /// The catalog of the data files.
    catalog: Arc<RwLock<Catalog>>,

    /// The compaction daemon.
    daemon: Option<thread::JoinHandle<Result<()>>>,

    /// The shared flag for telling daemon to stop.
    stop_flag: Arc<Mutex<bool>>,
}

impl NaiveKV {
    pub fn open(
        folder_path: impl Into<PathBuf>,
        memtable_compaction_threshold: usize,
        generation_geometric_ratio: usize,
        compaction_daemon_cycle_s: u64,
    ) -> Result<Self> {
        let catalog = Arc::new(RwLock::new(Catalog::open(folder_path.into())?));
        let catalog_copy = catalog.clone();

        let stop_flag = Arc::new(Mutex::new(false));
        let stop_flag_copy = stop_flag.clone();

        let daemon = Some(thread::spawn(move || {
            let mut epoch_no = 0;
            while !*stop_flag_copy.lock()? {
                thread::sleep(Duration::from_secs(compaction_daemon_cycle_s));
                Self::compact(
                    &*catalog_copy,
                    &mut epoch_no,
                    memtable_compaction_threshold,
                    generation_geometric_ratio,
                )?;
            }
            Ok(())
        }));
        Ok(Self {
            catalog,
            daemon,
            stop_flag,
        })
    }

    pub fn catalog_viewer(&self) -> Result<CatalogViewer> {
        CatalogViewer::new(self.catalog.clone())
    }

    fn compact(
        catalog: &RwLock<Catalog>,
        epoch_no: &mut u64,
        memtable_compaction_threshold: usize,
        generation_geometric_ratio: usize,
    ) -> Result<()> {
        let ro_memtable;
        let sstable_path;
        let mut sstables = Vec::new();
        let mut gen_no = 0; // The generation number of the new SSTable.
        {
            // Lock the catalog for a short duration.
            let mut catalog = catalog.write()?;
            {
                let mut memtable = catalog.memtable.write()?;
                if memtable.data_size() < memtable_compaction_threshold {
                    return Ok(());
                }
                *epoch_no += 1;

                // Create a new Memtable to replace the current read-write Memtable.
                let mut rw_memtable =
                    Memtable::open(Catalog::gen_memtable_path(&catalog.folder_path))?;
                std::mem::swap(&mut rw_memtable, &mut *memtable);
                ro_memtable = Arc::new(rw_memtable);
            }
            // Move the old read-write Memtable into the read-only stage.
            catalog.ro_memtable = Some(ro_memtable.clone());

            // Copy pointers to the SSTables that should be merged.
            let mut size = ro_memtable.data_size();
            let mut size_threshold = memtable_compaction_threshold * generation_geometric_ratio;
            for sstable in &catalog.sstables {
                sstables.push(sstable.clone());
                size += sstable.file_size();
                if size < size_threshold {
                    break;
                }
                gen_no += 1;
                size_threshold *= generation_geometric_ratio;
            }
            sstable_path = Catalog::gen_sstable_path(&catalog.folder_path, sstables.len());
        }

        // Do the merge without locking the catalog.
        let sstable = SSTable::create(sstable_path, &ro_memtable, &sstables, gen_no, *epoch_no)?;

        {
            // Lock the catalog again for a short duration.
            let mut catalog = catalog.write()?;

            // Remove the read-only Memtable.
            catalog.ro_memtable.as_ref().unwrap().deprecate()?;
            catalog.ro_memtable = None;

            // Place the merge-to SSTable.
            if gen_no == catalog.sstables.len() {
                catalog.sstables.push(Arc::new(sstable));
            } else {
                catalog.sstables[gen_no].deprecate()?;
                catalog.sstables[gen_no] = Arc::new(sstable);
            }
            // Replace the merge-from SSTables with empty ones.
            for i in 0..gen_no {
                catalog.sstables[i].deprecate()?;
                let sstable_path = Catalog::gen_sstable_path(&catalog.folder_path, i);
                catalog.sstables[i] = Arc::new(SSTable::create_empty(sstable_path, i, *epoch_no)?);
            }
        }
        Ok(())
    }
}

impl Drop for NaiveKV {
    fn drop(&mut self) {
        *self.stop_flag.lock().unwrap() = true;
        if let Some(daemon) = self.daemon.take() {
            let _ = daemon
                .join()
                .expect("Failed to join the compaction daemon.");
        }
    }
}

#[cfg(test)]
#[allow(unused_assignments)]
mod tests {
    use super::NaiveKV;
    use crate::logger;
    use crate::thread_pool::ThreadPool;

    #[test]
    fn test_naive_kv() {
        const FOLDER_PATH: &str = "/tmp/naive_kv/test/";
        const NUM_THREADS: usize = 3;
        const MAX_NUMBER: usize = 1 << 16; // Multiple generations.
        const MEMTABLE_COMPACTION_THRESHOLD: usize = 1024; // 1 KB
        const GENERATION_GEOMETRIC_RATIO: usize = 8;
        const COMPACTION_DAEMON_CYCLE_S: u64 = 1;

        logger::init().unwrap();

        let _ = std::fs::remove_dir_all(FOLDER_PATH);

        let mut naive_kv = Some(
            NaiveKV::open(
                FOLDER_PATH,
                MEMTABLE_COMPACTION_THRESHOLD,
                GENERATION_GEOMETRIC_RATIO,
                COMPACTION_DAEMON_CYCLE_S,
            )
            .expect("Failed to create the NaiveKV instance."),
        );

        // Write initial values.
        {
            let servers = ThreadPool::new(NUM_THREADS);
            for i in 0..NUM_THREADS {
                let mut catalog_viewer = naive_kv.as_ref().unwrap().catalog_viewer().unwrap();
                let mut num = i;
                servers
                    .add_task(move || {
                        while num < MAX_NUMBER {
                            let num_str = num.to_string();
                            catalog_viewer
                                .set(num_str.clone(), num_str.clone())
                                .unwrap();

                            let val = catalog_viewer.get(&num_str).unwrap();
                            assert_eq!(val, Some(num_str));
                            num += NUM_THREADS;
                        }
                    })
                    .expect("Failed to add a task to the thread pool.");
            }
        }

        // Overwrite values.
        {
            let servers = ThreadPool::new(NUM_THREADS);
            for i in 0..NUM_THREADS {
                let mut catalog_viewer = naive_kv.as_ref().unwrap().catalog_viewer().unwrap();
                let mut num = i;
                servers
                    .add_task(move || {
                        while num < MAX_NUMBER {
                            let num_str = num.to_string();
                            let num_plus_one_str = (num + 1).to_string();
                            catalog_viewer
                                .set(num_str.clone(), num_plus_one_str.clone())
                                .unwrap();

                            let val = catalog_viewer.get(&num_str).unwrap();
                            assert_eq!(val, Some(num_plus_one_str));
                            num += NUM_THREADS;
                        }
                    })
                    .expect("Failed to add a task to the thread pool");
            }
        }

        // Restart from disk files.
        naive_kv = None;
        naive_kv = Some(
            NaiveKV::open(
                FOLDER_PATH,
                MEMTABLE_COMPACTION_THRESHOLD,
                GENERATION_GEOMETRIC_RATIO,
                COMPACTION_DAEMON_CYCLE_S,
            )
            .expect("Failed to restart the NaiveKV instance"),
        );
        let mut catalog_viewer = naive_kv.as_ref().unwrap().catalog_viewer().unwrap();
        for num in 0..MAX_NUMBER {
            let num_str = num.to_string();
            let num_plus_one_str = (num + 1).to_string();
            let val = catalog_viewer.get(&num_str).unwrap();
            assert_eq!(val, Some(num_plus_one_str));
        }
    }
}

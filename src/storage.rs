use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use crate::types::{Record, Result};

const MEMTABLE_COMPACTION_THRESHOLD: usize = 1 << 6; // 1MB
const GENERATION_GEOMETRIC_RATIO: usize = 8;

pub struct NaiveKV {
    // The catalog of the data files.
    catalog: Arc<RwLock<Catalog>>,

    // The compaction daemon.
    daemon: Option<thread::JoinHandle<Result<()>>>,

    // The shared flag for telling daemon to stop.
    stop_flag: Arc<Mutex<bool>>,
}

impl NaiveKV {
    pub fn open(folder_path: impl Into<PathBuf>) -> Result<Self> {
        let catalog = Arc::new(RwLock::new(Catalog::open(folder_path.into())?));
        let catalog_copy = catalog.clone();

        let stop_flag = Arc::new(Mutex::new(false));
        let stop_flag_copy = stop_flag.clone();

        let daemon = Some(thread::spawn(move || {
            loop {
                if *stop_flag_copy.lock()? {
                    break;
                }
                thread::sleep(Duration::from_micros(100));
                compact(&*catalog_copy)?;
            }
            Ok(())
        }));
        Ok(Self {
            catalog,
            daemon,
            stop_flag,
        })
    }

    pub fn get(&self, key: &mut str) -> Result<Option<String>> {
        do_get(&*self.catalog, key)
    }

    pub fn set(&self, key: String, value: String) -> Result<()> {
        do_set(&*self.catalog, key, Record::Value(value))
    }

    pub fn remove(&self, key: String) -> Result<()> {
        do_set(&*self.catalog, key, Record::Deleted)
    }
}

impl Drop for NaiveKV {
    fn drop(&mut self) {
        *self.stop_flag.lock().unwrap() = true;
        if let Some(daemon) = self.daemon.take() {
            daemon
                .join()
                .expect("Unable to join the compaction daemon.");
        }
    }
}

struct Catalog {
    // The absolute path of the entire folder.
    folder_path: PathBuf,

    // An in-memory KV store with write-ahead logging.
    memtable: Arc<RwLock<Memtable>>,

    // The backup of the Memtable during compaction.
    ro_memtable: Option<Arc<Memtable>>,

    // SSTables that are stored as segment files.
    sstables: Vec<Arc<SSTable>>,
}

impl Catalog {
    fn open(folder_path: PathBuf) -> Result<Self> {
        let memtable = Arc::new(RwLock::new(Memtable::new()));
        let ro_memtable = None;
        let sstables = Vec::new();
        Ok(Self {
            folder_path,
            memtable,
            ro_memtable,
            sstables,
        })
    }
}

struct Memtable {
    // The in-memory key-value store.
    data: BTreeMap<String, Record>,

    // The path of the write-ahead log.
    log_path: PathBuf,

    // The accumulated size of the key-value store.
    data_size: usize,
}

impl Memtable {
    fn new() -> Self {
        let data = BTreeMap::new();
        let log_path = PathBuf::new();
        let data_size = 0;
        Memtable {
            data,
            log_path,
            data_size,
        }
    }

    fn get(&self, key: &str) -> Result<Option<Record>> {
        Ok(None)
    }

    fn set(&mut self, key: String, record: Record) -> Result<()> {
        Ok(())
    }
}

struct SSTable {
    // The generation number.
    gen_no: usize,

    // The mapping from pivot keys to offsets in the segment file.
    index: BTreeMap<String, u64>,

    // The absolute path of the segment file.
    segment_path: String,

    // The size of the segment file in bytes.
    segment_size: usize,
}

impl SSTable {
    fn new() -> Self {
        let gen_no = 0;
        let index = BTreeMap::new();
        let segment_path = String::new();
        let segment_size = 0;
        SSTable {
            gen_no,
            index,
            segment_path,
            segment_size,
        }
    }

    fn get(&self, key: &str) -> Result<Option<Record>> {
        Ok(None)
    }
}

fn do_get(catalog: &RwLock<Catalog>, key: &str) -> Result<Option<String>> {
    // Step 1. Try to read the read-write Memtable.
    let memtable = catalog.read()?.memtable.clone(); // Locking the catalog.
    if let Some(record) = memtable.read()?.get(&key)? {
        return record.into();
    }

    // Step 2. Try to read the read-only Memtable if it exists.
    let ro_memtable = catalog.read()?.ro_memtable.as_ref().map(|rc| rc.clone()); // Locking the catalog.
    if let Some(memtable) = ro_memtable {
        if let Some(record) = memtable.get(&key)? {
            return record.into();
        }
    }

    // Step 3. Try to read the SSTables in the order of generation.
    let mut sstable;
    let mut gen = 0;
    loop {
        {
            // Locking the catalog, copy the Arc to the SSTable.
            let catalog = catalog.read()?;
            if catalog.sstables.len() == gen {
                return Ok(None);
            } else {
                sstable = Some(catalog.sstables[gen].clone());
            }
        }
        if let Some(record) = sstable.unwrap().get(&key)? {
            return record.into();
        }

        gen += 1;
    }
}

fn do_set(catalog: &RwLock<Catalog>, key: String, record: Record) -> Result<()> {
    let memtable = catalog.read()?.memtable.clone(); // Locking the catalog.
    let result = memtable.write()?.set(key, record);
    result
}

fn compact(catalog: &RwLock<Catalog>) -> Result<()> {
    let mut ro_memtable;
    let mut sstables = Vec::new();
    {
        // Lock the catalog.
        let mut catalog = catalog.write()?;
        {
            let mut memtable = catalog.memtable.write()?;
            if memtable.data_size < MEMTABLE_COMPACTION_THRESHOLD {
                return Ok(());
            }
            // Replace the read-write Memtable with an empty one.
            let mut rw_memtable = Memtable::new();
            std::mem::swap(&mut rw_memtable, &mut *memtable);
            ro_memtable = Arc::new(rw_memtable);
        }
        // Move the read-write Memtable into the read-only stage.
        catalog.ro_memtable = Some(ro_memtable.clone());

        // Copy pointers to the SSTables that should be merged.
        let mut size = ro_memtable.data_size;
        let mut size_threshold = MEMTABLE_COMPACTION_THRESHOLD * GENERATION_GEOMETRIC_RATIO;
        for sstable in &catalog.sstables {
            sstables.push(sstable.clone());
            size += sstable.segment_size;
            if size < size_threshold {
                break;
            }
            size_threshold *= GENERATION_GEOMETRIC_RATIO;
        }
    }

    let sstable = merge_memtable_with_sstables(&ro_memtable, &sstables);

    {
        // Lock the catalog again.
        let mut catalog = catalog.write()?;
        // Decommission the read-only Memtable.
        catalog.ro_memtable = None;

        // Place the new SSTable.
        let index = sstable.gen_no;
        if index == catalog.sstables.len() {
            catalog.sstables.push(Arc::new(sstable));
        } else {
            catalog.sstables[index] = Arc::new(sstable);
        }
        // Clear the old SSTables.
        for i in 0..index {
            catalog.sstables[index] = Arc::new(SSTable::new());
        }
    }
    Ok(())
}

fn merge_memtable_with_sstables(memtable: &Memtable, sstable: &Vec<Arc<SSTable>>) -> SSTable {
    let mut result = SSTable::new();
    result
}

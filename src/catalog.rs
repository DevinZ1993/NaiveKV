use rand::{thread_rng, Rng};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::memtable::Memtable;
use crate::sstable::{SSTable, SSTableView};
use crate::types::{NaiveError, Result};

pub struct Catalog {
    /// The absolute path of the data folder.
    pub folder_path: PathBuf,

    /// The in-memory active data for both read and write.
    pub memtable: Arc<RwLock<Memtable>>,

    /// The read-only backup of the Memtable during compaction.
    pub ro_memtable: Option<Arc<Memtable>>,

    /// Read-only on-disk data in increasing generations.
    pub sstables: Vec<Arc<SSTable>>,
}

impl Catalog {
    pub fn open(folder_path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(folder_path.as_path())?;

        let ro_memtable = None;
        let mut sstables = Vec::new();

        let mut memtable_paths = Vec::new();
        for dir_entry in std::fs::read_dir(folder_path.as_path())? {
            let file_path = dir_entry?.path();
            if !file_path.as_path().is_file() {
                continue;
            }
            let file_name = file_path
                .as_path()
                .file_name()
                .unwrap_or(std::ffi::OsStr::new(""))
                .to_str()
                .unwrap_or("");
            if file_name.ends_with(".sst") {
                sstables.push(Arc::new(SSTable::open(file_path)?));
            } else if file_name.starts_with("memtable_") && file_name.ends_with(".log") {
                memtable_paths.push(file_path);
            }
        }
        log::info!("Successfully generated SSTables.");

        if memtable_paths.len() > 1 {
            log::error!("Found multiple Memtable logs:");
            for memtable_path in memtable_paths {
                log::error!("  {}", memtable_path.display());
            }
            return Err(NaiveError::InvalidData);
        }

        sstables.sort_by(|a, b| a.gen_no().partial_cmp(&b.gen_no()).unwrap());
        for gen_no in 0..sstables.len() {
            let sstable = &sstables[gen_no];
            if gen_no != sstable.gen_no() {
                log::error!(
                    "Expect generation {}, found {} which is generation {}.",
                    gen_no,
                    sstable.file_path().display(),
                    sstable.gen_no()
                );
                return Err(NaiveError::InvalidData);
            }
        }

        // If no Memtable log is found, create a new one.
        let memtable = Arc::new(RwLock::new(Memtable::open(
            memtable_paths
                .pop()
                .unwrap_or(Self::gen_memtable_path(&folder_path)),
        )?));
        log::info!("Successfully generated an Memtable.");

        Ok(Self {
            folder_path,
            memtable,
            ro_memtable,
            sstables,
        })
    }

    pub fn gen_memtable_path(folder_path: &PathBuf) -> PathBuf {
        let mut path_buf = folder_path.clone();
        let mut rng = thread_rng();
        path_buf.push(format!("memtable_{}.log", rng.gen::<u64>()));
        path_buf
    }

    pub fn gen_sstable_path(folder_path: &PathBuf, gen_no: usize) -> PathBuf {
        let mut path_buf = folder_path.clone();
        let mut rng = thread_rng();
        path_buf.push(format!("gen_{}_{}.sst", gen_no, rng.gen::<u64>()));
        path_buf
    }
}

pub struct CatalogViewer {
    /// The underlying Catalog.
    catalog: Arc<RwLock<Catalog>>,

    /// The SSTable views of the last synced epoch.
    sstable_views: Vec<SSTableView>,
}

impl CatalogViewer {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Result<CatalogViewer> {
        let mut sstable_views = Vec::new();
        {
            let catalog = catalog.read()?;
            sstable_views.reserve(catalog.sstables.len());
            for sstable in &catalog.sstables {
                sstable_views.push(SSTableView::new(sstable.clone())?);
            }
        }
        Ok(Self {
            catalog,
            sstable_views,
        })
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        let catalog = self.catalog.read()?;

        // Step 1. Try to read the read-write Memtable.
        if let Some(record) = catalog.memtable.read()?.get(key)? {
            return record.into();
        }

        // Step 2. Try to read the read-only Memtable if it exists.
        if let Some(memtable) = catalog.ro_memtable.as_ref() {
            if let Some(record) = memtable.get(key)? {
                return record.into();
            }
        }

        // Step 3. Try to read the SSTableView's in sequence.
        for (gen_no, sstable) in catalog.sstables.iter().enumerate() {
            // SSTableView's are updated on demand.
            if self.sstable_views.len() == gen_no {
                self.sstable_views.push(SSTableView::new(sstable.clone())?);
            } else if self.sstable_views[gen_no].epoch_no() != sstable.epoch_no() {
                self.sstable_views[gen_no] = SSTableView::new(sstable.clone())?;
            }
            if let Some(record) = self.sstable_views[gen_no].get(key)? {
                return record.into();
            }
        }
        Ok(None)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let catalog = self.catalog.read()?;
        let result = catalog.memtable.write()?.set(key, value);
        result
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let catalog = self.catalog.read()?;
        let result = catalog.memtable.write()?.remove(key);
        result
    }
}

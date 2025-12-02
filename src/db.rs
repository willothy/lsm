use std::{
    collections::VecDeque,
    sync::{atomic::AtomicUsize, Arc},
};

use anyhow::Context;
use arc_swap::ArcSwap;

use crate::{
    config::Config,
    key::{Key, SeqNo},
    memtable::{state, MemTable},
    sstable::{manager::SSTableManager, Level},
    value::Value,
    wal::{Wal, WalRecord},
};

pub struct FrozenTables {
    /// Frozen, immutable memtables waiting to be turned into SSTables.
    pub(crate) tables: ArcSwap<VecDeque<MemTable<state::Frozen>>>,

    flushed: AtomicUsize,
}

impl FrozenTables {
    pub fn new(tables: VecDeque<MemTable<state::Frozen>>) -> Self {
        Self {
            tables: ArcSwap::from_pointee(tables),
            flushed: AtomicUsize::new(0),
        }
    }

    pub fn compact(&self) {
        self.tables.rcu(|list| {
            let mut new_list = list.as_ref().clone();
            let flushed = self.flushed.load(std::sync::atomic::Ordering::Relaxed);
            for _ in 0..flushed {
                new_list.pop_front();
            }

            // This will fail if list has changed since we loaded it, so we don't
            // need to RCU to protect the flushed count.
            self.flushed.store(0, std::sync::atomic::Ordering::Relaxed);

            new_list
        });
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = MemTable<state::Frozen>> {
        self.tables
            .load()
            .as_ref()
            .clone()
            .into_iter()
            .skip(self.flushed.load(std::sync::atomic::Ordering::Relaxed))
    }

    pub fn push(&self, table: MemTable<state::Frozen>) {
        self.tables.rcu(|list| {
            let mut new_list = list.as_ref().clone();
            new_list.push_back(table.clone());
            new_list
        });
    }

    pub fn pop_front(&self) -> Option<MemTable<state::Frozen>> {
        let initial_flushed = self.flushed.load(std::sync::atomic::Ordering::Relaxed);

        self.tables.load().get(initial_flushed).cloned()
    }

    pub fn mark_flushed(&self) {
        self.flushed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

pub struct Database {
    config: Arc<Config>,

    /// The active MemTable
    table: MemTable<state::Active>,

    /// Frozen, immutable memtables waiting to be turned into SSTables.
    imm_tables: Arc<FrozenTables>,

    wal: Wal,

    seqno: SeqNo,

    sstables: Arc<SSTableManager>,

    flush_thread: std::thread::JoinHandle<()>,
}

impl Database {
    pub fn open(config: Config) -> anyhow::Result<Self> {
        let config = Arc::new(config);

        let manifests_dir = config.data_dir.join("manifests");
        let sstables_dir = config.data_dir.join("sstables");

        std::fs::create_dir_all(&config.data_dir).context("Failed to create data directory")?;
        std::fs::create_dir_all(&sstables_dir).context("Failed to create sstables directory")?;
        std::fs::create_dir_all(&manifests_dir).context("Failed to create manifests directory")?;

        let mut wal = Wal::open(config.data_dir.join("wal.log"))?;

        let replay = wal.replay()?;

        let mut table = MemTable::new();
        let mut imm_tables = VecDeque::new();

        // TODO: CURRENT should point to the latest manifest file, not be a manifest itself.
        let manager = SSTableManager::open(Arc::clone(&config))?;

        let mut max_seqno = manager.last_committed_sequence_number();

        for record in replay {
            if record.key().seqno() < manager.last_committed_sequence_number() {
                continue;
            }

            match record {
                WalRecord::Put { key, val } => {
                    max_seqno = max_seqno.max(key.seqno());

                    table.put(key, val);
                }
                WalRecord::Delete { key } => {
                    max_seqno = max_seqno.max(key.seqno());

                    table.delete(key);
                }
            }

            if table.should_freeze() {
                imm_tables.push_back(table.freeze());
            }
        }

        let imm_tables = {
            Arc::new(FrozenTables {
                tables: ArcSwap::from_pointee(imm_tables),
                flushed: AtomicUsize::new(0),
            })
        };

        let sstables = Arc::new(manager);

        let flush_thread = std::thread::spawn({
            let sstables = Arc::clone(&sstables);
            let imm_tables = Arc::clone(&imm_tables);
            move || loop {
                if let Some(frozen) = imm_tables.pop_front() {
                    if let Err(e) = sstables.flush_memtable(&frozen, Arc::clone(&imm_tables)) {
                        eprintln!("Error flushing memtable to SSTable: {:?}", e);
                    }
                } else {
                    std::thread::park();
                }
            }
        });

        // TODO: truncate WAL to remove processed entries (seqno <= last_committed_sequence_number)

        Ok(Self {
            config,

            table,
            imm_tables,
            wal,
            seqno: max_seqno.max(sstables.last_committed_sequence_number()) + 1,
            sstables,

            flush_thread,
        })
    }

    pub fn should_freeze_memtable(&self) -> bool {
        self.table.should_freeze() || self.wal.should_compact()
    }

    pub fn get(&self, key: &bytes::Bytes) -> Option<bytes::Bytes> {
        if let Some(value) = self.table.get_latest(key) {
            match value {
                Value::Data(bytes) => return Some(bytes.clone()),
                Value::Tombstone => return None,
            }
        }

        for table in self.imm_tables.iter().rev() {
            if let Some(value) = table.get_latest(key) {
                match value {
                    Value::Data(bytes) => return Some(bytes.clone()),
                    Value::Tombstone => return None,
                }
            }
        }

        // // TODO: SSTables
        // for level in 0..self.sstables.max_level().0 {
        //     for table in self.sstables.iter_level(Level(level)).expect("level should exist") {
        //
        //
        //     }
        // }

        None
    }

    pub fn put(
        &mut self,
        key: impl Into<bytes::Bytes>,
        val: impl Into<bytes::Bytes>,
    ) -> anyhow::Result<()> {
        let key = key.into();
        let val = val.into();
        let key = Key::new(key, self.seqno.next());

        self.wal.append(WalRecord::Put {
            key: key.clone(),
            val: val.clone(),
        })?;

        self.table.put(key, val);

        self.maybe_rotate_memtable();

        Ok(())
    }

    pub fn delete(&mut self, key: impl Into<bytes::Bytes>) -> anyhow::Result<()> {
        let key = key.into();
        let key = Key::new(key, self.seqno.next());

        self.wal.append(WalRecord::Delete { key: key.clone() })?;

        self.table.delete(key);

        self.maybe_rotate_memtable();

        Ok(())
    }

    fn maybe_rotate_memtable(&mut self) {
        if self.should_freeze_memtable() {
            let frozen = self.table.freeze();

            self.imm_tables.push(frozen);

            // Tell the flush thread to wake up and flush the new frozen memtable.
            self.flush_thread.thread().unpark();

            // TODO: Handle WAL spanning multiple memtables so we can be crash-safe and
            // not lose the frozen memtables.
            // For now, we'll just never clear the WAL because we don't have SSTables.
            // self.wal.clear();
        }
    }

    pub fn debug_replay_wal(&mut self) -> anyhow::Result<Vec<WalRecord>> {
        self.wal.replay()
    }
}

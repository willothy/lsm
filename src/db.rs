use std::{
    collections::VecDeque,
    sync::{atomic::AtomicUsize, Arc},
};

use anyhow::Context;

use crate::{
    config::Config,
    key::{Key, SeqNo},
    memtable::{state, MemTable},
    sstable::manager::{FileNo, SSTableManager},
    value::Value,
    wal::{Wal, WalRecord},
};

pub struct Database {
    config: Arc<Config>,

    /// The active MemTable
    table: MemTable<state::Active>,

    /// Frozen, immutable memtables waiting to be turned into SSTables.
    imm_tables: glommio::sync::RwLock<VecDeque<MemTable<state::Frozen>>>,

    wal: Wal,

    seqno: SeqNo,

    sstables: SSTableManager,
}

pub async fn coordinator_loop() {
    loop {}
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
        let sstables = SSTableManager::open(Arc::clone(&config))?;

        let mut max_seqno = sstables.last_committed_sequence_number();

        for record in replay {
            if record.key().seqno() < sstables.last_committed_sequence_number() {
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

        // TODO: truncate WAL to remove processed entries (seqno <= last_committed_sequence_number)

        Ok(Self {
            config,

            table,
            imm_tables: glommio::sync::RwLock::new(imm_tables),
            wal,
            seqno: max_seqno.max(sstables.last_committed_sequence_number()) + 1,
            sstables,
        })
    }

    pub fn should_freeze_memtable(&self) -> bool {
        self.table.should_freeze() || self.wal.should_compact()
    }

    pub async fn get(&self, key: &bytes::Bytes) -> Option<bytes::Bytes> {
        if let Some(value) = self.table.get_latest(key) {
            match value {
                Value::Data(bytes) => return Some(bytes.clone()),
                Value::Tombstone => return None,
            }
        }

        for table in self
            .imm_tables
            .read()
            .await
            .expect("lock closed")
            .iter()
            .rev()
        {
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

    pub async fn put(
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

        self.maybe_rotate_memtable().await;

        Ok(())
    }

    pub async fn delete(&mut self, key: impl Into<bytes::Bytes>) -> anyhow::Result<()> {
        let key = key.into();
        let key = Key::new(key, self.seqno.next());

        self.wal.append(WalRecord::Delete { key: key.clone() })?;

        self.table.delete(key);

        self.maybe_rotate_memtable().await;

        Ok(())
    }

    async fn maybe_rotate_memtable(&mut self) {
        if self.should_freeze_memtable() {
            let frozen = self.table.freeze();

            self.imm_tables
                .write()
                .await
                .expect("lock closed")
                .push_back(frozen);

            // self.sstables.flush_memtable(frozen)

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

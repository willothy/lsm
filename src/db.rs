use std::path::PathBuf;

use crate::{
    config::Config,
    key::{Key, SeqNo},
    memtable::{state, MemTable},
    sstable::manager::SSTableManager,
    value::Value,
    wal::{Wal, WalRecord},
};

pub struct Database {
    config: Config,

    /// The active MemTable
    table: MemTable<state::Active>,
    /// Frozen, immutable memtables waiting to be turned into SSTables.
    imm_tables: Vec<MemTable<state::Frozen>>,

    pub wal: Wal,

    seqno: SeqNo,

    sstables: SSTableManager,
}

impl Database {
    pub fn new(data_dir: PathBuf) -> Self {
        let manifests_dir = data_dir.join("manifests");
        let sstables_dir = data_dir.join("sstables");

        std::fs::create_dir_all(&data_dir).expect("Failed to create data directory");
        std::fs::create_dir_all(&sstables_dir).expect("Failed to create sstables directory");
        std::fs::create_dir_all(&manifests_dir).expect("Failed to create manifests directory");

        let mut wal = Wal::new(data_dir.join("wal.log"));

        let replay = wal.replay();

        let mut table = MemTable::new();
        let mut imm_tables = Vec::new();

        // TODO: CURRENT should point to the latest manifest file, not be a manifest itself.
        let manager = SSTableManager::open(&data_dir.join("manifests"));

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
                imm_tables.push(table.freeze());
            }
        }

        // TODO: truncate WAL to remove processed entries (seqno <= last_committed_sequence_number)

        Self {
            config: Config { data_dir },

            table,
            imm_tables,
            wal,
            seqno: max_seqno.max(manager.last_committed_sequence_number()) + 1,
            sstables: manager,
        }
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

        // TODO: SSTables

        None
    }

    pub fn put(&mut self, key: impl Into<bytes::Bytes>, val: impl Into<bytes::Bytes>) {
        let key = key.into();
        let val = val.into();
        let key = Key::new(key, self.seqno.next());

        self.wal.append(WalRecord::Put {
            key: key.clone(),
            val: val.clone(),
        });

        self.table.put(key, val);

        self.maybe_rotate_memtable();
    }

    pub fn delete(&mut self, key: impl Into<bytes::Bytes>) {
        let key = key.into();
        let key = Key::new(key, self.seqno.next());

        self.wal.append(WalRecord::Delete { key: key.clone() });

        self.table.delete(key);

        self.maybe_rotate_memtable();
    }

    fn maybe_rotate_memtable(&mut self) {
        if self.should_freeze_memtable() {
            let frozen = self.table.freeze();

            self.imm_tables.push(frozen);

            // TODO: Handle WAL spanning multiple memtables so we can be crash-safe and
            // not lose the frozen memtables.
            // For now, we'll just never clear the WAL because we don't have SSTables.
            // self.wal.clear();
        }
    }
}

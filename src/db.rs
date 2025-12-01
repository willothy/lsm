use std::path::PathBuf;

use crate::{
    key::{Key, SeqNo},
    memtable::{state, MemTable},
    value::Value,
    wal::{Wal, WalRecord},
};

pub struct Database {
    /// The active MemTable
    table: MemTable<state::Active>,
    /// Frozen, immutable memtables waiting to be turned into SSTables.
    imm_tables: Vec<MemTable<state::Frozen>>,

    pub wal: Wal,

    seqno: SeqNo,
}

impl Database {
    pub fn new(wal_path: PathBuf) -> Self {
        let wal = Wal::new(wal_path);

        let replay = wal.replay();

        let mut table = MemTable::new();
        let mut imm_tables = Vec::new();

        let mut max_seqno = SeqNo::from(0u64);

        for record in replay {
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

        Self {
            table,
            imm_tables,
            wal,
            seqno: max_seqno + 1,
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

        self.table.put(key.clone(), val.clone());

        self.wal.append(WalRecord::Put { key, val });

        self.maybe_rotate_memtable();
    }

    pub fn delete(&mut self, key: impl Into<bytes::Bytes>) {
        let key = key.into();
        let key = Key::new(key, self.seqno.next());

        self.table.delete(key.clone());

        self.wal.append(WalRecord::Delete { key });

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

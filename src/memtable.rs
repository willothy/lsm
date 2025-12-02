use std::collections::BTreeMap;

use crate::{key::Key, value::Value};

pub mod state {
    pub struct Frozen;
    pub struct Active;

    pub trait MemTableState: sealed::Sealed {}
    impl<T> MemTableState for T where T: sealed::Sealed {}

    mod sealed {
        pub trait Sealed {}
        impl Sealed for super::Frozen {}
        impl Sealed for super::Active {}
    }
}

use state::MemTableState;

pub struct MemTable<State: MemTableState> {
    data: BTreeMap<Key, Value>,
    size: usize,
    phantom: std::marker::PhantomData<State>,
}

const MEMTABLE_MAX_SIZE: usize = 1024 * 64 /* 64KB */;

impl<S: MemTableState> MemTable<S> {
    pub fn get(&self, k: &Key) -> Option<Value> {
        self.data.get(k).cloned()
    }

    pub fn get_latest(&self, k: &bytes::Bytes) -> Option<&Value> {
        self.iter_by_user_key(k).next().map(|(_, v)| v)
    }

    pub fn iter_by_user_key(
        &self,
        k: &bytes::Bytes,
    ) -> std::collections::btree_map::Range<'_, Key, Value> {
        self.data.range(Key::range_by_user_key(k.clone()))
    }
}

// impl MemTable<Frozen> {}

impl MemTable<state::Active> {
    pub fn new() -> Self {
        MemTable {
            data: BTreeMap::new(),
            size: 0,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn should_freeze(&self) -> bool {
        self.size >= MEMTABLE_MAX_SIZE
    }

    pub fn freeze(&mut self) -> MemTable<state::Frozen> {
        let data = std::mem::take(&mut self.data);
        let size = std::mem::replace(&mut self.size, 0);

        MemTable {
            data,
            size,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn put(&mut self, k: Key, v: bytes::Bytes) {
        let l_new = v.len();
        let l_key = k.user_key().len();

        if let Some(old) = self.data.insert(k, Value::Data(v)) {
            match old {
                Value::Data(old_bytes) => {
                    let l_old = old_bytes.len();
                    if l_old > l_new {
                        self.size -= l_old - l_new;
                    } else {
                        self.size += l_new - l_old;
                    }
                }
                Value::Tombstone => {
                    self.size += l_new;
                }
            }
        } else {
            self.size += l_new + l_key;
        }
    }

    pub fn delete(&mut self, k: Key) {
        let l_key = k.user_key().len();

        if let Some(old) = self.data.insert(k, Value::Tombstone) {
            match old {
                Value::Data(old) => {
                    self.size -= old.len();
                }
                Value::Tombstone => {}
            }
        } else {
            self.size += l_key;
        }
    }
}

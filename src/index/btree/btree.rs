use super::super::indexer::Indexer;
use crate::data::LogRecord;
use bytes::Bytes;
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};
use super::super::index_iterator::IndexIterator;
use super::super::super::options::iterator_options::IteratorOptions;

/// BTree索引, 主要是封装BTreeMap数据结构
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecord>>>,
}

impl BTree {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn put(
        &self,
        key: Vec<u8>,
        pos: crate::data::LogRecordPos,
    ) -> Option<crate::data::LogRecordPos> {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos)
    }

    fn get(&self, key: Vec<u8>) -> Option<crate::data::LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }

    fn delete(&self, key: Vec<u8>) -> Option<crate::data::LogRecordPos> {
        let mut write_guard = self.tree.write();
        write_guard.remove(&key)
    }

    fn list_key(&self) -> crate::errors::Result<Vec<bytes::Bytes>> {
        let read_guard = self.tree.read();
        let mut keys = Vec::with_capacity(read_guard.len());
        for (k, _) in read_guard.iter() {
            keys.push(Bytes::copy_from_slice(&keys));
        }
        Ok(keys)
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let read_guard = self.tree.read();
        let mut items = Vec::with_capacity(read_guard.len());
        // 将 BTree 中的数据存储到数组中
        for (key, value) in read_guard.iter() {
            items.push((key.clone(), value.clone()));
        }
        if options.reverse {
            items.reverse();
        }
        Box::new(BTreeIterator {
            items,
            curr_index: 0,
            options,
        })
    }
}

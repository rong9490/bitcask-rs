use std::sync::Arc;
use crossbeam_skiplist::SkipMap;
use crate::index::indexer::Indexer;

use super::super::super::data::LogRecordPos;

// 跳表索引结构体
pub struct SkipList {
  skl: Arc<SkipMap<Vec<u8>, LogRecordPos>>,
}

impl SkipList {
  pub fn new() -> Self {
    Self {
      skl: Arc::new(SkipMap::new())
    }
  }
}

impl Indexer for SkipList {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos> {
        todo!()
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        todo!()
    }

    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        todo!()
    }

    fn list_keys(&self) -> crate::errors::Result<Vec<bytes::Bytes>> {
        todo!()
    }

    fn iterator(&self, options: crate::options::iterator_options::IteratorOptions) -> Box<dyn crate::index::index_iterator::IndexIterator> {
        todo!()
    }
}
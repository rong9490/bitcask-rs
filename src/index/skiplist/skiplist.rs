use std::sync::Arc;
use crossbeam_skiplist::SkipMap;
use super::super::super::data::LogRecordPos;

// 跳表索引结构体
pub struct Skiplist {
  skl: Arc<SkipMap<Vec<u8>, LogRecordPos>>,
}

impl Skiplist {
  pub fn new() -> Self {
    Self {
      skl: Arc::new(SkipMap::new())
    }
  }
}

impl Indexer for Skiplist {
  
}
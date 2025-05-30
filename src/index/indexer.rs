use super::bptree::bptree::BPlusTree;
use super::btree::btree::BTree;
use super::index_iterator::IndexIterator;
use super::skiplist::skiplist::SkipList;
use crate::errors::Result;
use crate::options::index_type::IndexType;
use crate::{data::LogRecordPos, options::iterator_options::IteratorOptions};
use bytes::Bytes;
use std::path::PathBuf;

/// 抽象索引接口(Indexer Trait), 需要具体实现
pub trait Indexer: Sync + Send {
    /// 向索引中存储key对应的数据位置信息
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos>;

    /// 根据key取出索引位置信息
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// 根据key删除索引位置信息
    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// 获取索引存储的所有key
    fn list_keys(&self) -> Result<Vec<Bytes>>;

    /// 返回索引迭代器
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
}

/// 根据类型打开内存索引
pub fn new_indexer(index_type: IndexType, dir_path: PathBuf) -> Box<dyn Indexer> {
    match index_type {
        IndexType::BTree => Box::new(BTree::new()),
        IndexType::SkipList => Box::new(SkipList::new()),
        IndexType::BPlusTree => Box::new(BPlusTree::new(dir_path)),
    }
}

use super::bptree::bptree::BPlusTree;
use super::btree::btree::BTree;
use super::indexer::Indexer;
use super::skiplist::skiplist::SkipList;
use crate::options::index_type::IndexType;
use std::path::PathBuf;

/// 根据类型打开内存索引
pub fn new_indexer(index_type: IndexType, dir_path: PathBuf) -> Box<dyn Indexer> {
    match index_type {
        IndexType::BTree => Box::new(BTree::new()),
        IndexType::SkipList => Box::new(SkipList::new()),
        IndexType::BPlusTree => Box::new(BPlusTree::new(dir_path)),
    }
}

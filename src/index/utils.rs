use super::indexer::Indexer;
use crate::options::index_type::IndexType;
use std::path::PathBuf;

/// 根据类型打开内存索引
pub fn new_indexer(index_type: IndexType, dir_path: PathBuf) -> Box<dyn Indexer> {
    match index_type {
        IndexType::BTree => Box::new(btree::BTree::new()),
        IndexType::SkipList => Box::new(skiplist::SkipList::new()),
        IndexType::BPlusTree => Box::new(bptree::BPlusTree::new(dir_path)),
    }
}

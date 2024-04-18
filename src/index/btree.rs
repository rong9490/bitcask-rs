// 对 btree map的简单封装

use super::Indexer;
use crate::data::log_record::LogRecordPos;
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

pub struct BTree {
    // 并发访问
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    // "实例方法"
    pub fn new() -> Self {
        Self {
            // 三层实例化: 原子锁 --> 锁 --> BTree实体
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

// 为BTree这个结构体实现Indexer这个trait这个接口
impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard = self.tree.write(); // 拿到写锁, 具体查看BTreeMap文档
        write_guard.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read(); // 拿到读锁
        let value = read_guard.get(&key);
        value.copied() // 引用转值(copied)
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();

        // 判断删除是否有效
        let remove_res = write_guard.remove(&key);
        remove_res.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_btree_put() {
        let bt: BTree = BTree::new();
        bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 0,
            },
        );
    }
}

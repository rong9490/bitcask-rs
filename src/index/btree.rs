// 对btree进行二次封装操作

use crate::data::log_record::LogRecordPos;
use crate::index::Indexer;
use parking_lot::RwLock; // Read-Write锁, 多个读写同时存在, 但写操作只有一个
use std::{collections::BTreeMap, sync::Arc};

#[allow(dead_code)]
pub struct BTree {
    // 注意这里的key, val含义
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>, // 并发访问锁, parking_lot库提供
}

impl BTree {
    // 创建一个新的BTree实例
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

// 为BTree实现Indexer接口: Struct + Trait
impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard = self.tree.write(); // 拿到写锁
        write_guard.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos /* 需要实现Copy trait */> {
        let read_guard = self.tree.read(); // 拿到读锁
        read_guard.get(&key).copied() // 拿到key对应的值, 为什么需要copied --> 引用变为拥有所有权(传递出去)
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write(); // 拿到写锁
        let remove_result = write_guard.remove(&key);
        remove_result.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree() {
        let bt = BTree::new();
        bt.put(
            b"key1".to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 100,
            },
        );
        let pos = bt.get(b"key1".to_vec()).unwrap();
        assert_eq!(pos.file_id, 1);
        assert_eq!(pos.offset, 100);

        // 测试获取不存在的键
        assert!(bt.get(b"non_existent_key".to_vec()).is_none());

        // 测试删除存在的键
        assert!(bt.delete(b"key1".to_vec()));
        assert!(bt.get(b"key1".to_vec()).is_none());

        // 测试删除不存在的键
        assert!(!bt.delete(b"non_existent_key".to_vec()));

        // 测试删除后再次插入
        bt.put(
            b"key2".to_vec(),
            LogRecordPos {
                file_id: 2,
                offset: 200,
            },
        );
        assert!(bt.delete(b"key2".to_vec()));
        bt.put(
            b"key2".to_vec(),
            LogRecordPos {
                file_id: 3,
                offset: 300,
            },
        );
        let pos = bt.get(b"key2".to_vec()).unwrap();
        assert_eq!(pos.file_id, 3);
        assert_eq!(pos.offset, 300);
    }
}

// 对btree进行二次封装操作

use crate::data::log_record::LogRecordPos;
use crate::index::Indexer;
use parking_lot::RwLock; // Read-Write锁, 多个读写同时存在, 但写操作只有一个
use std::{collections::BTreeMap, sync::Arc};

#[allow(dead_code)]
pub struct BTree {
    // 注意这里的 key, value 语义
    // Atomic reference count
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>, // 并发访问锁, parking_lot库 提供
}

impl BTree {
    // 初始化实例
    pub fn new() -> Self {
        let org_tree = BTreeMap::new();
        Self {
            tree: Arc::new(RwLock::new(org_tree)),
        }
    }
}

// 为BTree实现Indexer接口: Struct + Trait
impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        // 拿到写锁
        let mut write_guard = self.tree.write();
        // btree的插入方法
        write_guard.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        // 拿到读锁
        let read_guard = self.tree.read();
        let record_ref: Option<&LogRecordPos> = read_guard.get(&key);
        // 为什么需要copied --> 引用变为拥有所有权(传递出去)

        // 需要实现Copy trait
        record_ref.copied()
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        // 拿到写锁
        let mut write_guard = self.tree.write();
        let remove_result: Option<LogRecordPos> = write_guard.remove(&key);
        // 如果不存在就是false
        remove_result.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree() {
        let bt = BTree::new();
        let org_record = LogRecordPos {
            file_id: 1,
            offset: 120,
        };
        let _key1: Vec<u8> = b"key11234".to_vec();
        // 插入一个记录
        bt.put(
            b"key1".to_vec(), // key 是将&str转为Vec<u8>
            org_record,
        );
        // 获取记录
        let pos: LogRecordPos = bt.get(b"key1".to_vec()).unwrap();
        assert_eq!(pos.file_id, org_record.file_id);
        assert_eq!(pos.offset, org_record.offset);

        // 获取不存在的键
        assert!(bt.get(b"non_existent_key".to_vec()).is_none());
        // 删除存在的键
        assert!(bt.delete(b"key1".to_vec()));
        // 获取不存在的键
        assert!(bt.get(b"key1".to_vec()).is_none());
        // 删除不存在的键
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

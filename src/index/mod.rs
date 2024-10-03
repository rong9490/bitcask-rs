// mode.rs文件: 声明一个crate出口
// 索引相关的操作index

pub mod btree;

use crate::data::log_record::LogRecordPos;

// 抽象接口的定义(trait), 然后再考虑实现;
pub trait Indexer {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>; // Option包装, 可能不存在
    fn delete(&self, key: Vec<u8>) -> bool; // 记录不存在就返回false
}

// 一个crate出口

pub mod btree;

use crate::data::log_record::LogRecordPos;

// 抽象接口的定义
pub trait Indexer {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>; // 这里用Option包装一下
    fn delete(&self, key: Vec<u8>) -> bool;
}

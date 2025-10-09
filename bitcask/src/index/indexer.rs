use bytes::Bytes;
use crate::errors::AppResult;
use crate::options::iterator_options::IteratorOptions;
use crate::data::log_record_mod::log_record_pos::LogRecordPos;
use super::index_iterator::IndexIterator;

/// Indexer 抽象索引接口，后续如果想要接入其他的数据结构，则直接实现这个接口即可
pub trait Indexer: Sync + Send {
    /// 向索引中存储 key 对应的数据位置信息
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos>;

    /// 根据 key 取出对应的索引位置信息
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// 根据 key 删除对应的索引位置信息
    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// 获取索引存储的所有的 key
    fn list_keys(&self) -> AppResult<Vec<Bytes>>;

    /// 返回索引迭代器
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
}
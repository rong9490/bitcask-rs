use crate::data::log_record_mod::log_record_pos::LogRecordPos;

/// 抽象索引迭代器
pub trait IndexIterator: Sync + Send {
    /// Rewind 重新回到迭代器的起点，即第一个数据
    fn rewind(&mut self);

    /// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
    fn seek(&mut self, key: Vec<u8>);

    /// Next 跳转到下一个 key，返回 None 则说明迭代完毕
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}
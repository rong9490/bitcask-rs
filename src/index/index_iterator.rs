use crate::data::LogRecordPos;

/// 抽象索引迭代器
pub trait IndexIterator: Sync + Send {
  /// Rewind 重新回到迭代器的起点, 即第一个数据
  fn rewind(&mut self);

  /// Seek 根据传入的key查找第一个大于(或小于)的目标key, 从key开始遍历
  fn seek(&mut self, key: Vec<u8>);

  /// Next跳转到下一个key, 返回None说明迭代完毕
  fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_index_iterator() {
    
  }
}
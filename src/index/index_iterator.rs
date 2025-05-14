use crate::data::LogRecordPos;

/// 抽象索引迭代器
pub trait IndexIterator {
  /// Rewind 重新回到迭代器起点, 第一个数据
  fn rewind(&mut self);

  /// Seek 根据传入的key查找第一个大于(小于)等于目标的key, 从这个key开始遍历
  fn seek(&mut self, key: Vec<u8>);

  /// Next跳转到下一个key, 返回元组, 返回None说明枚举完毕
  fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_index_iterator() {
    
  }
}
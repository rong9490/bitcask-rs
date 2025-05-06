// 派生"PartialEq"宏 --> 允许该枚举值被比较相等
// 派生"Clone"/"Copy"宏 --> 允许该枚举值被复制
// C-like枚举(没有携带任何数据的枚举)
#[derive(PartialEq, Clone, Copy, Debug)]
#[repr(u8)] // 明确指定类型为u8, 也就是占用8位(1个字节)
pub enum LogRecordType {
    // 正常 put 的数据
    NORMAL = 1,
    // 被删除的数据标识，墓碑值
    DELETED = 2,
    // 事务完成的标识
    TXNFINISHED = 3,
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_log_record_type() {
    // 用来判断类型的大小, 而LogRecordType::NORMAL是个具体的值, 不是类型, 值的内存应该用size_of_val
    // assert_eq!(std::mem::size_of::<LogRecordType>(), std::mem::size_of::<LogRecordType::NORMAL>());
    // 这种写法叫做turbofish
    assert_eq!(std::mem::size_of::<LogRecordType>(), 1);

    let normal: LogRecordType = LogRecordType::NORMAL;
    let deleted: LogRecordType = LogRecordType::DELETED;
    let tenfinished: LogRecordType = LogRecordType::TXNFINISHED;

    assert_eq!(std::mem::size_of_val(&normal), std::mem::size_of_val(&deleted));
    assert_eq!(std::mem::size_of_val(&normal), 1);
  }
}
// 日志记录类型枚举
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum LogRecordType {
    // 正常 put 的数据
    NORMAL = 1,
    // 被删除的数据标识，墓碑值
    DELETED = 2,
    // 事务完成的标识
    TXNFINISHED = 3,
}

impl LogRecordType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETED,
            3 => LogRecordType::TXNFINISHED,
            _ => panic!("unknown log record type"),
        }
    }
}

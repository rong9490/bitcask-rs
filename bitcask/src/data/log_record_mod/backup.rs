/// 从数据文件中读取的 log_record 信息，包含其 size
#[derive(Debug)]
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: usize,
}

// 暂存事务数据信息
pub struct TransactionRecord {
    pub(crate) record: LogRecord,
    pub(crate) pos: LogRecordPos,
}

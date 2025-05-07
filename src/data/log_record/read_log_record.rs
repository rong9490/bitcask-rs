use super::log_record::LogRecord;

/// 从数据文件中读取的 log_record 信息，包含其 size
#[derive(Debug)]
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: usize,
}

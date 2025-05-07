use super::log_record::LogRecord;
use super::log_record_pos::LogRecordPos;

// 暂存事务数据信息
pub struct TransactionRecord {
  pub(crate) record: LogRecord,
  pub(crate) pos: LogRecordPos,
}
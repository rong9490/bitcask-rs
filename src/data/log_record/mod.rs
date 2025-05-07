/* 进一步拆分与细化 log_record.rs文件 */

pub mod utils;
pub mod log_record_type;
pub mod log_record;
pub mod log_record_pos;
pub mod read_log_record;
pub mod transaction_record;

// (re-export) 重新导出常用类型，简化外部访问
pub use log_record_type::LogRecordType;
pub use log_record::LogRecord;
pub use log_record_pos::LogRecordPos;
pub use read_log_record::ReadLogRecord;
pub use transaction_record::TransactionRecord;
pub use utils::max_log_record_header_size;

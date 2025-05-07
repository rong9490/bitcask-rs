pub mod data_file;
pub mod log_record;

// 重新导出 log_record 模块中的类型
pub use log_record::{
    LogRecordType,
    LogRecord,
    LogRecordPos,
    ReadLogRecord,
    TransactionRecord,
    max_log_record_header_size,
};
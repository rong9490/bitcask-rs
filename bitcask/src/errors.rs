use std::result;
use thiserror::Error; // 配合 anyhow

// 类型别名(易用), 绑定该错误的Result
pub type AppResult<T> = result::Result<T, AppErrors>;

/// 应用全局需要一个兜底的, 容纳所有错误的枚举!
#[derive(Debug, PartialEq, Error)]
pub enum AppErrors {
    #[error("failed to read from data file")]
    FailedReadFromDataFile,

    #[error("failed to write to data file")]
    FailedWriteToDataFile,

    #[error("failed to sync data file")]
    FailedSyncDataFile,

    #[error("failed to open data file")]
    FailedToOpenDataFile,

    #[error("the key is empty")]
    KeyIsEmpty,

    #[error("memory index failed to update")]
    IndexUpdateFailed,

    #[error("key is not found in database")]
    KeyNotFound,

    #[error("data file is not found in database")]
    DataFileNotFound,

    #[error("database dir path can not be empty")]
    DirPathIsEmpty,

    #[error("database data file size must be greater than 0")]
    DataFileSizeTooSmall,

    #[error("failed to create the database directory")]
    FailedToCreateDatabaseDir,

    #[error("failed to read the database directory")]
    FailedToReadDatabaseDir,

    #[error("the database directory maybe corrupted")]
    DataDirectoryCorrupted,

    #[error("read data file eof")]
    ReadDataFileEOF,

    #[error("invalid crc value, log record maybe corrupted")]
    InvalidLogRecordCrc,

    #[error("exceed the max batch num")]
    ExceedMaxBatchNum,
}

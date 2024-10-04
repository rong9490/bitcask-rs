// 自定义的错误类型

use std::result;
use thiserror::Error as ThisError;

// 自定义错误, 库协助: thiserror
#[derive(Debug, ThisError, PartialEq)]
pub enum BKErrors {
    #[error("failed read from ")]
    FailedReadFromDataFile,
    #[error("failed write to data file")]
    FailedWriteToDataFile,
    #[error("failed sync data file")]
    FailedSyncToDataFile,
    #[error("failed open data file")]
    FailedOpenDataFile,
    #[error("key is empty")]
    KeyIsEmpty,
    #[error("failed update memory index")]
    FailedUpdateIndex,
    #[error("key not found")]
    KeyNotFound,
    #[error("data file not found")]
    DataFileNotFound,
    #[error("dir path is empty")]
    DirPathIsEmpty,
    #[error("invalid data file size")]
    InvalidDataFileSize,
    #[error("failed to create dir")]
    FailedToCreateDir,
    #[error("failed to read data file")]
    FailedReadDataFile,
    #[error("data directory corrupted")]
    DataDirectoryCorrupted,
    #[error("read data file EOF")]
    ReadDataFileEOF,
}

// 自定义类型
pub type BKResult<T> = result::Result<T, BKErrors>;

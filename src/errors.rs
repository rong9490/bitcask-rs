use std::result;

use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum Errors {
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

    #[error("merge is in progress, try again later")]
    MergeInProgress,

    #[error("cannot use write batch, seq file not exists")]
    UnableToUseWriteBatch,

    #[error("the database directory is used by another process")]
    DatabaseIsUsing,

    #[error("invalid merge ratio, must between 0 and 1")]
    InvalidMergeRatio,

    #[error("do not reach the merge ratio")]
    MergeRatioUnreached,

    #[error("disk space is not enough for merge")]
    MeregeNoEnoughSpace,

    #[error("failed to copy the database directory")]
    FailedToCopyDirectory,

    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongTypeOperation,
}

pub type Result<T> = result::Result<T, Errors>;

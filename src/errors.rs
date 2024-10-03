// 自定义的错误类型

use std::{result};
use thiserror::Error as ThisError;

// 自定义错误, 库协助: thiserror
#[derive(Debug, ThisError)]
pub enum BKErrors {
    #[error("failed read from ")]
    FailedReadFromDataFile,
    #[error("failed write to data file")]
    FailedWriteToDataFile,
    #[error("failed sync data file")]
    FailedSyncToDataFile,
    #[error("failed open data file")]
    FailedOpenDataFile,
}

// 自定义类型
pub type BKResult<T> = result::Result<T, BKErrors>;
// 当前活跃的数据文件

use parking_lot::RwLock;
use std::{path::PathBuf, sync::Arc};

use crate::{
    errors::{BKErrors, BKResult},
    fio::IOManager,
};

use super::log_record::{LogRecord, ReadLogRecord};

pub const DATA_FILE_SUFFIX: &str = ".data";

pub struct DataFile {
    file_id: Arc<RwLock<u32>>,      // 数据文件id
    write_off: Arc<RwLock<u64>>,    // 当前偏移量
    io_manager: Box<dyn IOManager>, // IO管理句柄
}

impl DataFile {
    // 创建数据文件
    pub fn new(dir_path: PathBuf, file_id: u32) -> BKResult<DataFile> {
        todo!()
    }

    pub fn get_write_off(&self) -> u64 {
        // 锁读出来的是引用, 需要解引用
        *self.write_off.read()
    }

    pub fn set_write_off(&self, offset: u64) {
        let mut write_guard = self.write_off.write();
        *write_guard = offset;
    }

    // 获取数据文件id
    pub fn get_file_id(&self) -> u32 {
        *self.file_id.read()
    }

    pub fn read_log_record(&self, offset: u64) -> BKResult<ReadLogRecord> {
        todo!()
    }

    // 写入数据
    pub fn write(&self, buf: &[u8]) -> BKResult<usize> {
        todo!()
    }

    // 同步数据文件(持久化)
    pub fn sync(&self) -> BKResult<()> {
        self.io_manager.sync()
    }
}

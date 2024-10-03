// 系统标准的io操作流程

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use parking_lot::RwLock;
use crate::errors::{BKErrors, BKResult};
use super::IOManager;
use log::error as log_error;

pub struct FileIO {
    fd: Arc<RwLock<File>>, // 系统文件描述符(句柄)
}

impl FileIO {
    // 实例化, 传入文件名, 判断是否合法存在
    pub fn new(file_name: &str) -> Self {
        match OpenOptions::new() {
            Ok(f) => {},
            Err(e) => {}
        }
    }
}

// 实现文件操作的Trait, IOManager
impl IOManager for  FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> BKResult<usize> {
        let read_guard = self.fd.read();
        let read_res = read_guard.read_at(buf, offset);

        // 对结果错误进行处理
        let n_bytes: usize = match read_res {
            Ok(n) => n,
            // 这里可以插入错误日志, 方便问题排查!
            Err(e) => {
                log_error!("read from data file err: {:?}", e);
                return Err(BKErrors::FailedReadFromDataFile);
            },
        };

        Ok(n_bytes)
    }

    fn write(&self, buf: &[u8]) -> BKResult<usize> {
        let mut write_guard = self.fd.write();
        let write_res = write_guard.write(buf);
        let write_res = match write_res {
            Ok(n) => n,
            Err(e) => {
                log_error!("write to data file err: {:?}", e);
                return Err(BKErrors::FailedWriteToDataFile);
            }
        };

        Ok(write_res)
    }

    fn sync(&self) -> BKResult<()> {
        let read_guard = self.fd.read();
        if let Err(e) = read_guard.sync_all() {
            log_error!("sync err: {:?}", e);
            return Err(BKErrors::FailedSyncToDataFile)
        };
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
}
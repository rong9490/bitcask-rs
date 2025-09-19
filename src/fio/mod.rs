// 文件操作 fio
// 多线程之间传递, 需要 Sync + Send

pub mod utils;

mod file_io;

use std::path::PathBuf;

use file_io::FileIO;

use crate::errors::{Errors, Result};

pub trait IOManager: Sync + Send {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn write(&self, buf: &[u8]) -> Result<usize>;
    fn sync(&self) -> Result<()>;
}

/// 根据文件名称初始化 IOManager
pub fn new_io_manager(file_name: PathBuf) -> Result<impl IOManager> {
    FileIO::new(file_name)
}
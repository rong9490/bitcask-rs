// 文件操作 fio
// 多线程之间传递, 需要 Sync + Send

mod file_io;

use crate::errors::{BKErrors, BKResult};

pub trait IOManager: Sync + Send {
    fn read(&self, buf: &mut [u8], offset: u64) -> BKResult<usize>;
    fn write(&self, buf: &[u8]) -> BKResult<usize>;
    fn sync(&self) -> BKResult<()>;
}
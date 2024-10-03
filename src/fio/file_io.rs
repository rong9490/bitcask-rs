// 系统标准的io操作流程

use super::IOManager;
use crate::errors::{BKErrors, BKResult};
use log::error as log_error;
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn file_exists(path: &PathBuf) -> bool {
    // match File::open(path) {
    //     Ok(_) => true,
    //     Err(_) => false,
    // }
    Path::new(&path).exists()
}

pub struct FileIO {
    fd: Arc<RwLock<File>>, // 系统文件描述符(句柄)
}

impl FileIO {
    // 实例化, 传入文件名, 判断是否合法存在
    pub fn new(file_name: PathBuf) -> BKResult<Self> {
        // 文件存在判断: 创建文件, 读写权限
        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(file_name)
        {
            Ok(f) => {
                let fio = FileIO {
                    fd: Arc::new(RwLock::new(f)),
                };
                Ok(fio)
            }
            Err(e) => {
                log_error!("open file err: {:?}", e);
                return Err(BKErrors::FailedOpenDataFile);
            }
        }
    }
}

// 实现文件操作的Trait, IOManager
impl IOManager for FileIO {
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
            }
        };

        Ok(n_bytes)
    }

    fn write(&self, buf: &[u8]) -> BKResult<usize> {
        let mut write_guard = self.fd.write();
        match write_guard.write(buf) {
            Ok(n) => Ok(n),
            Err(e) => {
                log_error!("write to data file err: {:?}", e);
                return Err(BKErrors::FailedWriteToDataFile);
            }
        }
    }

    fn sync(&self) -> BKResult<()> {
        let read_guard = self.fd.read();
        if let Err(e) = read_guard.sync_all() {
            log_error!("sync err: {:?}", e);
            return Err(BKErrors::FailedSyncToDataFile);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_file_io_write() {
        let path: PathBuf = PathBuf::from("./temp0.data"); // 不可以嵌套文件夹(没有处理)
        assert_eq!(false, file_exists(&path));
        // println!(
        //     "文件存在: {} {}",
        //     file_exists(&path),
        //     path.to_str().unwrap()
        // );
        let fio = FileIO::new(path.clone());
        assert!(fio.is_ok());

        // 拿到文件句柄, 开始执行操作
        let fio = fio.unwrap();

        // 写入1
        let write_res = fio.write(b"hello, world");
        assert!(write_res.is_ok());
        assert_eq!(write_res.unwrap(), 12); // 写入的字节数usize

        // 写入2
        let write_res = fio.write(b"jokeeer");
        assert!(write_res.is_ok());
        assert_eq!(write_res.unwrap(), 7); // 写入的字节数usize

        // 删除文件
        let remove_res = std::fs::remove_file(path.clone());
        assert!(remove_res.is_ok());
        assert_eq!(false, file_exists(&path));
    }

    #[test]
    fn test_file_io_read() {
        let path: PathBuf = PathBuf::from("./temp1.data"); // 不可以嵌套文件夹(没有处理)
        assert_eq!(false, file_exists(&path));
        let fio = FileIO::new(path.clone());
        // 拿到文件句柄, 开始执行操作
        let fio = fio.unwrap();

        // 写入1
        let write_res = fio.write(b"hello, world");
        // 写入2
        let write_res = fio.write(b"jokeeer");

        // 读取1
        let mut buf = [0u8; 8]; // 读取的缓冲区, 大小决定了读取的字节数
        let read_res = fio.read(&mut buf, 0);
        assert!(read_res.is_ok());
        assert_eq!(read_res.unwrap(), 8); // 写入的字节数usize
        println!("读取到的内容: {:?}", buf);

        // 读取2
        let mut buf = [0u8; 8]; // 读取的缓冲区, 大小决定了读取的字节数
        let read_res = fio.read(&mut buf, 4);
        assert!(read_res.is_ok());
        assert_eq!(read_res.unwrap(), 8); // 写入的字节数usize
        println!("读取到的内容: {:?}", buf);

        // 删除文件
        let remove_res = std::fs::remove_file(path.clone());
        assert!(remove_res.is_ok());
        assert_eq!(false, file_exists(&path));
    }

    #[test]
    fn test_file_io_sync() {
        let path: PathBuf = PathBuf::from("./temp2.data"); // 不可以嵌套文件夹(没有处理)
        assert_eq!(false, file_exists(&path));
        println!(
            "文件存在: {} {}",
            file_exists(&path),
            path.to_str().unwrap()
        );
        let fio = FileIO::new(path.clone());
        assert!(fio.is_ok());
        println!(
            "文件存在2: {} {}",
            file_exists(&path),
            path.to_str().unwrap()
        );

        // 拿到文件句柄, 开始执行操作
        let fio = fio.unwrap();

        // 写入1
        let write_res = fio.write(b"hello, world");
        assert!(write_res.is_ok());
        assert_eq!(write_res.unwrap(), 12); // 写入的字节数usize

        // 写入2
        let write_res = fio.write(b"jokeeer");
        assert!(write_res.is_ok());
        assert_eq!(write_res.unwrap(), 7); // 写入的字节数usize

        // // 同步?
        let sync_res = fio.sync();
        assert!(sync_res.is_ok());

        // 删除文件
        let remove_res = std::fs::remove_file(path.clone());
        assert!(remove_res.is_ok());
        assert_eq!(false, file_exists(&path));
    }
}

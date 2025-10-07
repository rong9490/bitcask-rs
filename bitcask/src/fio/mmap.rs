use std::{fs::OpenOptions, path::PathBuf, sync::Arc};
use log::error;
use memmap2::Mmap;
use parking_lot::Mutex;
use super::IOManager;
use crate::errors::{AppErrors, AppResult};

/// 线程安全(原子锁) -> memmap2::Mmap
pub struct MMapIO {
    map: Arc<Mutex<Mmap>>,
}

impl MMapIO {
    pub fn new(file_name: PathBuf) -> AppResult<Self> {
        // 尝试打开该路径文件
        return match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(file_name)
        {
            Ok(file) => {
                // TODO 为什么要用unsafe ?
                let map: Mmap = unsafe { Mmap::map(&file).expect("failed to map the file") };
                Ok(MMapIO {
                    map: Arc::new(Mutex::new(map)),
                })
            }
            Err(e) => {
                error!("failed to open data file: {}", e);
                Err(AppErrors::FailedToOpenDataFile)
            }
        }
    }
}

impl IOManager for MMapIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> AppResult<usize> {
        let map_arr = self.map.lock();
        let end: u64 = offset + buf.len() as u64;
        if end > map_arr.len() as u64 {
            return Err(AppErrors::ReadDataFileEOF);
        }
        let val: &[u8] = &map_arr[offset as usize..end as usize];

        // 显示/隐式 展开解引用
        (&mut *buf).copy_from_slice(val);

        Ok(val.len())
    }

    fn write(&self, _buf: &[u8]) -> AppResult<usize> {
        unimplemented!() // todo!(), unreachable!()
    }

    fn sync(&self) -> AppResult<()> {
        unimplemented!()
    }

    fn size(&self) -> u64 {
        let map_arr = self.map.lock();
        map_arr.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::fio::file_io::FileIO;

    use super::*;

    #[test]
    fn test_mmap_read() {
        let path: PathBuf = PathBuf::from("/tmp/mmap-test.data");

        // 文件为空
        let mmap_res1 = MMapIO::new(path.clone());
        assert!(mmap_res1.is_ok());
        let mmap_io1 = mmap_res1.ok().unwrap();
        let mut buf1 = [0u8; 10];
        let read_res1 = mmap_io1.read(&mut buf1, 0);
        assert_eq!(read_res1.err().unwrap(), AppErrors::ReadDataFileEOF);

        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();
        fio.write(b"aa").unwrap();
        fio.write(b"bb").unwrap();
        fio.write(b"cc").unwrap();

        // 有数据的情况
        let mmap_res2 = MMapIO::new(path.clone());
        assert!(mmap_res2.is_ok());
        let mmap_io2 = mmap_res2.ok().unwrap();

        let mut buf2 = [0u8; 2];
        let read_res2 = mmap_io2.read(&mut buf2, 2);
        assert!(read_res2.is_ok());

        let remove_res = fs::remove_file(path.clone());
        assert!(remove_res.is_ok());
    }
}

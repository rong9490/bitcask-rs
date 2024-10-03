// 存储引擎
// 第三方包, 字节数组Bytes

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use crate::data::data_file::DataFile;
use crate::data::log_record::LogRecordPos;
use crate::options::Options;

use crate::{data::log_record::{LogRecord, LogRecordType}, errors::{BKErrors, BKResult}};

pub struct Engine {
    options: Options,
    active_file: Arc<RwLock<DataFile>>, // 关联当前活跃的文件
    older_files: Vec<Arc<RwLock<HashMap<u32, DataFile>>>>, // 旧的文件树
}

impl Engine {
    // pub fn new() -> Self {
    //     Self {

    //     }
    // }
    
    // 存储K-V数据, Key不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> BKResult<()> {
        if key.is_empty() {
            return Err(BKErrors::KeyIsEmpty);
        }

        // 构造LogRecord
        let log_record: LogRecord = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: LogRecordType::NORMAL,
        };

        Ok(())
    }

    // 追加写数据到当前活跃文件中
    fn append_log_record(&self, log_record: &mut LogRecord) -> BKResult<()> {
        let dir_path: PathBuf = self.options.dir_path.clone();

        // 输入数据编码
        // let mut data: Vec<u8> = Vec::new();

        let enc_record: Vec<u8> = log_record.encode();
        let record_len = enc_record.len() as u64;

        // 获取当前活跃的文件
        let mut active_file = self.active_file.write();

        // 判断大小阈值
        if active_file.get_write_off() + record_len > self.options.data_file_size {
            // 超过阈值: 1.当前活跃文件持久化, 2.创建新的活跃文件
            active_file.sync()?;

            let curr_id = active_file.get_file_id();
            let new_id = curr_id + 1;

            // 旧文件先存
            let mut older_files = self.older_files.write();
            let old_file = DataFile::new(dir_path, curr_id)?;
            older_files.insert(new_id, old_file);
            // 打开新文件
            let new_file = DataFile::new(dir_path.clone(), new_id)?;
            self.active_file.write().sync()?;
            self.active_file = Arc::new(RwLock::new(new_file));
        }

        // 执行数据的写入
        active_file.write(&enc_record)?;

        // 根据配置项决定是否sync
        if self.options.sync_writes {
            active_file.sync()?;
        }

        // 构造数据索引信息
        let index_record = LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: active_file.get_write_off(),
        };

        Ok(index_record)
    }
}
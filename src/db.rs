// 存储引擎
// 第三方包, 字节数组Bytes

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::data::data_file::DataFile;
use crate::data::log_record::LogRecordPos;
use crate::index;
use crate::options::Options;
use bytes::Bytes;
use parking_lot::RwLock;

use crate::{
    data::log_record::{LogRecord, LogRecordType},
    errors::{BKErrors, BKResult},
};

pub struct Engine {
    options: Options,
    active_file: Arc<RwLock<DataFile>>, // 关联当前活跃的文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>, // 旧的文件树
    index: Box<dyn index::Indexer>,     // 内存索引
}

impl Engine {
    pub fn put(&self, key: Bytes, value: Bytes) -> BKResult<()> {
        if key.is_empty() {
            return Err(BKErrors::KeyIsEmpty); // key不能为空
        }

        // 构造LogRecord
        let mut record: LogRecord = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: LogRecordType::NORMAL,
        };

        // 追加到活跃文件中
        let log_record_pos = self.append_log_record(&mut record)?;
        // 更新内存索引
        let ok: bool = self.index.put(key.to_vec(), log_record_pos);

        if !ok {
            return Err(BKErrors::FailedUpdateIndex); // 更新索引失败
        }
        Ok(())
    }

    // 根据key获取数据
    pub fn get(&self, key: Bytes) -> BKResult<Option<Bytes>> {
        if key.is_empty() {
            return Err(BKErrors::KeyIsEmpty);
        }

        //内存索引拿到数据信息
        let position = self.index.get(key.to_vec());
        if position.is_none() {
            return Err(BKErrors::KeyNotFound); // 键不存在
        }
        let position = position.unwrap();

        // 获取活跃文件
        let active_file = self.active_file.read();
        // 获取旧文件
        let older_files = self.older_files.read();

        // 匹配id, 拿到log_record信息
        let log_record = match position.file_id == active_file.get_file_id() {
            true => active_file.read_log_record(position.offset)?,
            false => {
                // 从旧的集合里找到文件
                let data_file = older_files.get(&position.file_id);
                if data_file.is_none() {
                    // 也就是, 新的旧的都找不到
                    return Err(BKErrors::DataFileNotFound);
                }
                let data_file = data_file.unwrap();
                data_file.read_log_record(position.offset)?
            }
        };

        // 判断类型: 被标记删除(不能访问)
        if log_record.record_type == LogRecordType::DELETE {
            return Err(BKErrors::KeyNotFound);
        }

        // 数据有效的,返回value
        Ok(Some(Bytes::from(log_record.value)))
    }

    // 追加写数据到当前活跃文件中
    fn append_log_record(&self, log_record: &mut LogRecord) -> BKResult<LogRecordPos> {
        let dir_path: PathBuf = self.options.dir_path.clone();

        // 输入数据编码
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

            // 就是文件存在map中
            let mut older_files = self.older_files.write();
            let old_file = DataFile::new(dir_path.clone(), curr_id)?;
            older_files.insert(curr_id, old_file);
            // 打开新文件
            let new_file = DataFile::new(dir_path.clone(), new_id)?;
            *active_file = new_file;
        }

        // 执行数据追加到当前活跃文件
        let write_off = active_file.get_write_off();
        active_file.write(&enc_record)?;

        // 根据配置项决定是否sync文件
        if self.options.sync_writes {
            active_file.sync()?;
        }

        // 构造数据内存索引信息
        let index_record = LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
        };

        Ok(index_record)
    }
}

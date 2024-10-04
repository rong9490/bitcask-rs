// 存储引擎
// 第三方包, 字节数组Bytes

use std::path::PathBuf;
use std::sync::Arc;
use std::{collections::HashMap, fs};

use crate::data::data_file::{DataFile, DATA_FILE_SUFFIX};
use crate::data::log_record::LogRecordPos;
use crate::index;
use crate::options::Options;
use bytes::Bytes;
use log::warn;
use parking_lot::RwLock;

use crate::{
    data::log_record::{LogRecord, LogRecordType},
    errors::{BKErrors, BKResult},
};

const INITIAL_FILE_ID: u32 = 0;

pub struct Engine {
    options: Arc<Options>,
    active_file: Arc<RwLock<DataFile>>, // 关联当前活跃的文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>, // 旧的文件树
    index: Box<dyn index::Indexer>,     // 内存索引
    file_ids: Vec<u32>, // 数据库启动时的文件 id，只用于加载索引时使用，不能在其他的地方更新或使用
}

impl Engine {
    // 启动, 打开引擎实例
    pub fn open(options: Options) -> BKResult<Engine> {
        // 用户配置的有效性校验
        if let Some(err) = check_options(&options) {
            return Err(err);
        }

        // 理解为什么需要clone
        let options = options.clone();

        // 目录是否存在, 不存在则创建
        let dir_path = options.dir_path.clone();

        // 目录不存在, 创建目录
        if !dir_path.is_dir() {
            if let Err(err) = std::fs::create_dir_all(&dir_path) {
                warn!(
                    "failed to create dir: {}, err: {}",
                    dir_path.to_string_lossy(),
                    err
                );
                return Err(BKErrors::FailedToCreateDir);
            }
        }

        // 加载数据文件
        let mut data_files: Vec<DataFile> = load_data_files(dir_path.clone())?;

        // 设置file id信息
        let mut file_ids: Vec<u32> = Vec::new();
        for data_file in data_files.iter() {
            file_ids.push(data_file.get_file_id());
        }

        // 将旧文件存入older_files中
        let mut older_files: HashMap<u32, DataFile> = HashMap::new();
        // 这里为什么判断长度, 为什么长度减2 ?
        // 因为旧文件是活跃文件的前一个文件, 所以长度减2
        if data_files.len() > 1 {
            for _ in 0..=data_files.len() - 2 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }

        // 拿到当前活跃文件，即列表中最后一个文件
        let active_file = match data_files.pop() {
            Some(v) => v,
            None => DataFile::new(dir_path.clone(), INITIAL_FILE_ID)?,
        };

        // 构造存储引擎实例
        let engine = Self {
            options: Arc::new(options.clone()),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: Box::new(index::new_indexer(options.index_type)),
            file_ids,
        };

        // 从数据文件中加载索引
        engine.load_index_from_data_files()?;

        Ok(engine)
    }

    pub fn put(&self, key: Bytes, value: Bytes) -> BKResult<()> {
        if key.is_empty() {
            return Err(BKErrors::KeyIsEmpty); // key不能为空
        }

        // 构造LogRecord
        let mut record: LogRecord = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NORMAL,
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
        let log_record: LogRecord = match position.file_id == active_file.get_file_id() {
            true => active_file.read_log_record(position.offset)?.record,
            false => {
                // 从旧的集合里找到文件
                let data_file = older_files.get(&position.file_id);
                if data_file.is_none() {
                    // 也就是, 新的旧的都找不到
                    return Err(BKErrors::DataFileNotFound);
                }
                let data_file = data_file.unwrap();
                data_file.read_log_record(position.offset)?.record
            }
        };

        // 判断类型: 被标记删除(不能访问)
        if log_record.rec_type == LogRecordType::DELETED {
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

    /// 从数据文件中加载内存索引
    /// 遍历数据文件中的内容，并依次处理其中的记录
    fn load_index_from_data_files(&self) -> BKResult<()> {
        // 数据文件为空，直接返回
        if self.file_ids.is_empty() {
            return Ok(());
        }

        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        // 遍历每个文件 id，取出对应的数据文件，并加载其中的数据
        for (i, file_id) in self.file_ids.iter().enumerate() {
            let mut offset = 0;
            loop {
                let log_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(offset),
                    false => {
                        let data_file = older_files.get(file_id).unwrap();
                        data_file.read_log_record(offset)
                    }
                };

                let (log_record, size) = match log_record_res {
                    Ok(result) => (result.record, result.size),
                    Err(e) => {
                        if e == BKErrors::ReadDataFileEOF {
                            break;
                        }
                        return Err(e);
                    }
                };

                // 构建内存索引
                let log_record_pos = LogRecordPos {
                    file_id: *file_id,
                    offset,
                };

                match log_record.rec_type {
                    LogRecordType::NORMAL => {
                        self.index.put(log_record.key.to_vec(), log_record_pos)
                    }
                    LogRecordType::DELETED => self.index.delete(log_record.key.to_vec()),
                };

                // 递增 offset，下一次读取的时候从新的位置开始
                offset += size;
            }

            // 设置活跃文件的 offset
            if i == self.file_ids.len() - 1 {
                active_file.set_write_off(offset);
            }
        }
        Ok(())
    }
}

// 从目录中加载数据文件
fn load_data_files(dir_path: PathBuf) -> BKResult<Vec<DataFile>> {
    let dir = fs::read_dir(dir_path.clone());
    if dir.is_err() {
        return Err(BKErrors::FailedReadDataFile);
    }

    let dir = dir.unwrap();

    // 存放id
    let mut file_ids: Vec<u32> = Vec::new();
    // 存放文件
    let mut data_files: Vec<DataFile> = Vec::new();

    // 遍历目录
    for file in dir {
        if let Ok(entry) = file {
            // 从文件拿到文件名
            let file_os_str = entry.file_name();
            let file_name: &str = file_os_str.to_str().unwrap();

            // 判断后缀.data
            if file_name.ends_with(DATA_FILE_SUFFIX) {
                // 解析拿到数字id
                let split_names: Vec<&str> = file_name.split(".").collect::<Vec<&str>>();
                let file_id: u32 = match split_names[0].parse::<u32>() {
                    Ok(id) => id,
                    Err(err) => {
                        warn!("parse file id failed, err: {}", err);
                        return Err(BKErrors::DataDirectoryCorrupted);
                    }
                };
                // 找到了文件id
                file_ids.push(file_id);
            }
        }
    }

    // 如果没有数据文件，则直接返回
    if file_ids.is_empty() {
        return Ok(data_files);
    }

    // 对文件 id 进行排序，从小到大进行加载
    file_ids.sort();
    // 遍历所有的文件id，依次打开对应的数据文件
    for file_id in file_ids.iter() {
        let data_file = DataFile::new(dir_path.clone(), *file_id)?;
        data_files.push(data_file);
    }

    Ok(data_files)
}

// 独立的验证方法
fn check_options(options: &Options) -> Option<BKErrors> {
    // let options = options.clone();
    // 解构 Options 结构体
    let Options {
        dir_path,
        data_file_size,
        sync_writes: _,
        index_type: _,
    } = options;

    let dir_path = dir_path.to_str();

    // 非法路径
    if dir_path.is_none() || dir_path.unwrap().is_empty() {
        return Some(BKErrors::DirPathIsEmpty);
    }

    // 文件大小非法
    if *data_file_size <= 0 {
        return Some(BKErrors::InvalidDataFileSize);
    }

    None
}

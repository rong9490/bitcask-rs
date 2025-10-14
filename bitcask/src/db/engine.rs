use std::collections::HashMap;
use std::fs;
use std::fs::{File, create_dir_all};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use parking_lot::{Mutex, RwLock};
use crate::index;
use crate::options::options::Options;
use crate::data::data_files_mod::data_file::DataFile;
use crate::index::indexer::Indexer;
use crate::errors::{AppResult, AppErrors};
use super::utils::{check_options, load_data_files};
use crate::merge::load_merge_files;
use crate::options::io_type::IOType;

// TODO 补充 engine 方法!

const INITIAL_FILE_ID: u32 = 0u32;
const SEQ_NO_KEY: &str = "seq.no";
pub(crate) const FILE_LOCK_NAME: &str = "flock";

/// bitcask 存储引擎实例结构体
pub struct Engine {
    pub(crate) options: Arc<Options>,
    pub(crate) active_file: Arc<RwLock<DataFile>>, // 当前活跃数据文件
    pub(crate) older_files: Arc<RwLock<HashMap<u32, DataFile>>>, // 旧的数据文件
    pub(crate) index: Box<dyn Indexer>,     // 数据内存索引
    file_ids: Vec<u32>, // 数据库启动时的文件 id，只用于加载索引时使用，不能在其他的地方更新或使用
    pub(crate) batch_commit_lock: Mutex<()>, // 事务提交保证串行化
    pub(crate) seq_no: Arc<AtomicUsize>, // 事务序列号，全局递增
    pub(crate) merging_lock: Mutex<()>, // 防止多个线程同时 merge
    pub(crate) seq_file_exists: bool, // 事务序列号文件是否存在
    pub(crate) is_initial: bool, // 是否是第一次初始化该目录
    lock_file: File,    // 文件锁，保证只能在数据目录上打开一个实例
    bytes_write: Arc<AtomicUsize>, // 累计写入了多少字节
    pub(crate) reclaim_size: Arc<AtomicUsize>, // 累计有多少空间可以 merge
}

impl Engine {
    // 打开 bitcask 存储引擎实例
    pub fn open(opts: Options) -> AppResult<Self> {
        // 校验用户传递过来的配置项
        if let Some(e) = check_options(&opts) {
            return Err(e);
        }

        let mut is_initial = false;
        let options = opts.clone();
        // 判断数据目录是否存在，如果不存在的话则创建这个目录
        let dir_path = options.dir_path.clone();
        if !dir_path.is_dir() {
            is_initial = true;
            if let Err(e) = create_dir_all(dir_path.as_path()) {
                warn!("create database directory err: {}", e);
                return Err(AppErrors::FailedToCreateDatabaseDir);
            }
        }

        // 判断数据目录是否已经被使用了
        let lock_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir_path.join(FILE_LOCK_NAME))
            .unwrap();
        if let Err(_) = lock_file.try_lock_exclusive() {
            return Err(AppErrors::DatabaseIsUsing);
        }

        let entries = fs::read_dir(dir_path.clone()).unwrap();
        if entries.count() == 0 {
            is_initial = true;
        }

        // 加载 merge 数据目录
        load_merge_files(dir_path.clone())?;

        // 加载数据文件
        let mut data_files = load_data_files(dir_path.clone(), options.mmap_at_startup)?;

        // 设置 file id 信息
        let mut file_ids = Vec::new();
        for v in data_files.iter() {
            file_ids.push(v.get_file_id());
        }

        // 将旧的数据文件放到后面，新的数据文件在第一个位置
        data_files.reverse();
        // 将旧的数据文件保存到 older_files 中
        let mut older_files = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..=data_files.len() - 2 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }

        // 拿到当前活跃文件，即列表中最后一个文件
        let active_file = match data_files.pop() {
            Some(v) => v,
            None => DataFile::new(dir_path.clone(), INITIAL_FILE_ID, IOType::StandardFIO)?,
        };

        // 构造存储引擎实例
        let mut engine = Self {
            options: Arc::new(opts),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: new_indexer(options.index_type, options.dir_path),
            file_ids,
            batch_commit_lock: Mutex::new(()),
            seq_no: Arc::new(AtomicUsize::new(1)),
            merging_lock: Mutex::new(()),
            seq_file_exists: false,
            is_initial,
            lock_file,
            bytes_write: Arc::new(AtomicUsize::new(0)),
            reclaim_size: Arc::new(AtomicUsize::new(0)),
        };

        // B+ 树则不需要从数据文件中加载索引
        if engine.options.index_type != IndexType::BPlusTree {
            // 从 hint 文件中加载索引
            engine.load_index_from_hint_file()?;

            // 从数据文件中加载索引
            let current_seq_no = engine.load_index_from_data_files()?;

            // 更新当前事务序列号
            if current_seq_no > 0 {
                engine.seq_no.store(current_seq_no + 1, Ordering::SeqCst);
            }

            // 重置 IO 类型
            if engine.options.mmap_at_startup {
                engine.reset_io_type();
            }
        }

        if engine.options.index_type == IndexType::BPlusTree {
            // 加载事务序列号
            let (exists, seq_no) = engine.load_seq_no();
            if exists {
                engine.seq_no.store(seq_no, Ordering::SeqCst);
                engine.seq_file_exists = exists;
            }

            // 设置当前活跃文件的偏移
            let active_file = engine.active_file.write();
            active_file.set_write_off(active_file.file_size());
        }

        Ok(engine)
    }
}
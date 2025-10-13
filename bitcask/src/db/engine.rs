use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use parking_lot::{Mutex, RwLock};
use crate::index;
use crate::options::options::Options;
use crate::data::data_files_mod::data_file::DataFile;
use crate::index::indexer::Indexer;

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
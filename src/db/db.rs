/* 数据库引擎模型 */

use std::{collections::HashMap, sync::{atomic::AtomicUsize, Arc}};
use parking_lot::{RwLock, Mutex};
use crate::{data::data_file::DataFile, options::options::Options};
use super::super::index::indexer::Indexer;
use std::fs::{self, File};
use crate::errors::Result;
use super::utils::check_options;

const INITIAL_FILE_ID: u32 = 0;
const SEQ_NO_KEY: &str = "seq.no";
pub(crate) const FILE_LOCK_NAME: &str = "flock";

/// bitcask 存储引擎实例结构体
pub struct Engine {
  pub(crate) options: Arc<Options>,
  pub(crate) active_file: Arc<RwLock<DataFile>>, // 当前活跃的数据文件
  pub(crate) older_files: Arc<RwLock<HashMap<u32, DataFile>>>, // 旧数据文件
  pub(crate) index: Box<dyn Indexer>, // 数据内存索引
  file_ids: Vec<u32>, // 启动时文件id, 用于加载索引(不可其他更新使用)
  pub(crate) batch_commit_lock: Mutex<()>, // 失误提交保证串行化
  pub(crate) seq_no: Arc<AtomicUsize>, // 事务序列号, 全局递增
  pub(crate) merging_lock: Mutex<()>, // 防止多线程同时merge
  pub(crate) seq_file_exists: bool, // 事务序列号文件是否存在
  pub(crate) is_initial: bool, // 是否是第一次初始化目录
  lock_file: File, // 文件单例锁, 保证仅一个实例
  bytes_write: Arc<AtomicUsize>, // 累计写入字节数
  pub(crate) reclaim_size: Arc<AtomicUsize>, // 累计多少空间可merge
}

/// 引擎统计信息
#[derive(Debug)]
pub struct Stat {
  // key总数量
  pub key_num: usize,
  // 数据文件数量
  pub data_file_num: usize,
  // 可回收的数据量
  pub reclaim_size: usize,
  // 数据目录占磁盘空间大小
  pub disk_size: u64,
}

/// 实现引擎方法
impl Engine {
  // 打开引擎实例
  pub fn open(opts: Options) -> Result<Self> {
    // 校验用户配置项
    if let Some(e) = check_options(&opts) {
      return Err(e)
    }
    todo!()
  }
}


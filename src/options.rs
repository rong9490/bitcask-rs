use std::path::PathBuf;
use std::default::Default;

// HACK 什么是 Default trait ?

#[derive(Clone)]
pub struct Options {
    // 数据库目录
    pub dir_path: PathBuf,

    // 数据文件大小
    pub data_file_size: u64,

    // 是否每次写都持久化
    pub sync_writes: bool,

    // 累计写到多少字节后进行持久化
    pub bytes_per_sync: usize,

    // 索引类型
    pub index_type: IndexType,

    // 是否用 mmap 打开数据库
    pub mmap_at_startup: bool,

    // 执行数据文件 merge 的阈值
    pub data_file_merge_ratio: f32,
}

#[test]
fn test_options_memory() {
    let opt: Options = Default::default();
    assert_eq!(std::mem::size_of_val(&opt), 48);

    assert_eq!(std::mem::size_of_val(&opt.dir_path), 24); // PathBuf(胖指针)
    assert_eq!(std::mem::size_of::<PathBuf>(), 24);

    assert_eq!(std::mem::size_of_val(&opt.data_file_size), 8); // u64 8个字节
    assert_eq!(std::mem::size_of::<u64>(), 8);

    assert_eq!(std::mem::size_of_val(&opt.sync_writes), 1); // bool 1个字节
    assert_eq!(std::mem::size_of::<bool>(), 1);

    assert_eq!(std::mem::size_of_val(&opt.bytes_per_sync), 8); // usize 8个字节
    assert_eq!(std::mem::size_of::<usize>(), 8);

    assert_eq!(std::mem::size_of_val(&opt.index_type), 1); // IndexType 1个字节(因为是简单枚举)
    assert_eq!(std::mem::size_of::<IndexType>(), 1);

    assert_eq!(std::mem::size_of_val(&opt.mmap_at_startup), 1); // bool 1个字节
}

#[test]
fn test_options_default() {
    let opt: Options = Default::default();
    assert_eq!(opt.dir_path, std::env::temp_dir().join("bitcask-rs"));
}

#[derive(Clone, PartialEq)]
pub enum IndexType {
    /// BTree 索引
    BTree,

    /// 跳表索引
    SkipList,

    /// B+树索引，将索引存储到磁盘上
    BPlusTree,
}

// 默认值 结构体实现
impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("bitcask-rs"),
            data_file_size: 256 * 1024 * 1024, // 256MB,
            sync_writes: false,
            bytes_per_sync: 0,
            index_type: IndexType::BTree,
            mmap_at_startup: true,
            data_file_merge_ratio: 0.5,
        }
    }
}

/// 索引迭代器配置项
pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: false,
        }
    }
}

/// 批量写数据配置项
pub struct WriteBatchOptions {
    // 一个批次当中的最大数据量
    pub max_batch_num: usize,

    // 提交时候是否进行 sync 持久化
    pub sync_writes: bool,
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_num: 10000,
            sync_writes: true,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum IOType {
    // 标准文件 IO
    StandardFIO,

    // 内存文件映射
    MemoryMap,
}

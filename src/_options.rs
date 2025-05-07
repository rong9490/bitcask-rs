use std::path::PathBuf;

#[derive(Clone)]
pub struct Options {
    // 数据库目录
    pub dir_path: PathBuf,

    // 数据文件大小
    pub data_file_size: u64,

    // 是否每次写都持久化
    pub sync_writes: bool,

    // 索引类型
    pub index_type: IndexType,
}

#[derive(Clone)]
pub enum IndexType {
    /// BTree 索引
    BTree,

    /// 跳表索引
    SkipList,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("bitcask-rs"),
            data_file_size: 256 * 1024 * 1024, // 256MB,
            sync_writes: false,
            index_type: IndexType::BTree,
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

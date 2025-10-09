use std::path::PathBuf;
use std::default::Default;
use super::index_type::IndexType;

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

/// 默认配置(Default::default())
impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("bitcask-rs"),
            data_file_size: 256 * 1024 * 1024u64, // 256MB,
            sync_writes: false,
            bytes_per_sync: 0usize,
            index_type: IndexType::BTree,
            mmap_at_startup: true,
            data_file_merge_ratio: 0.5f32,
        }
    }
}

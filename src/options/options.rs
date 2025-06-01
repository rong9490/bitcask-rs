use super::index_type::IndexType;
use std::path::PathBuf;

#[derive(Clone)]
pub struct Options {
    /// 数据库目录
    pub dir_path: PathBuf,

    // 数据文件大小
    pub data_file_size: u64,

    // 是否每次写都持久化
    pub sync_writes: bool,

    // 累计多少字节后触发持久化
    pub bytes_per_sync: usize,

    // 索引类型
    pub index_type: IndexType,

    // 是否用mmap打开数据库
    pub mmap_at_startup: bool,

    // 执行数据文件merge的阈值
    pub data_file_merge_ratio: f32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("bitcask-rs"),
            data_file_size: 256 * 1024 * 1024, // 256MB
            sync_writes: false,
            bytes_per_sync: 0,
            index_type: IndexType::BTree,
            mmap_at_startup: true,
            data_file_merge_ratio: 0.5
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_struct_options() {
        assert_eq!(std::mem::size_of::<Options>(), 40); // 需要理解为什么是40
        let op: Options = Options {
            dir_path: PathBuf::default(),
            data_file_size: 8,
            sync_writes: true,
            index_type: IndexType::BPlusTree,
        };
        assert_eq!(std::mem::size_of_val(&op), std::mem::size_of::<Options>());
        assert_eq!(std::mem::size_of_val(&op), 40);
        assert_eq!(std::mem::size_of_val(&op.data_file_size), 8); // u64占8字节内存
        assert_eq!(std::mem::size_of_val(&op.dir_path), 24); // 路径PathBuf胖指针 固定占24字节
        assert_eq!(std::mem::size_of_val(&op.sync_writes), 1);
        assert_eq!(std::mem::size_of_val(&op.index_type), 1); // 加起来: 32字节 --> 但是每8个字节对齐, 取整40字节!
    }
}
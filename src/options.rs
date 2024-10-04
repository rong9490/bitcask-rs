use std::path::PathBuf;

#[derive(Clone)]
pub struct Options {
    // 数据库存放目录
    pub dir_path: PathBuf,
    // 数据文件大小
    pub data_file_size: u64,

    // 是否在每次写入后sync
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

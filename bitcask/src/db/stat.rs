/// 存储引擎相关统计信息
#[derive(Debug)]
pub struct Stat {
    // key 的总数量
    pub key_num: usize,
    // 数据文件的数量
    pub data_file_num: usize,
    // 可以回收的数据量
    pub reclaim_size: usize,
    // 数据目录占据的磁盘空间大小
    pub disk_size: u64,
}
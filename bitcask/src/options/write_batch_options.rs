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
            max_batch_num: 10000usize,
            sync_writes: true,
        }
    }
}

// 保证批处理操作原子性, 安全性

mod write_batch;
mod utils;

use crate::db::engine::Engine;
use std::sync::Arc;
use std::sync::Mutex;
use std::collections::HashMap;
use crate::errors::{AppErrors, AppResult};
use crate::options::index_type::IndexType;
use self::write_batch::WriteBatch;
use crate::options::write_batch_options::WriteBatchOptions;

// 给Engine附加额外方法: batch系列
impl Engine {
    /// 初始化 WriteBatch
    pub fn new_write_batch(&self, options: WriteBatchOptions) -> AppResult<WriteBatch> {
        // 如果是 B+树 类型, 需要额外判断
        if self.options.index_type == IndexType::BPlusTree && !self.seq_file_exists && !self.is_initial {
            return Err(AppErrors::UnableToUseWriteBatch);
        }
        Ok(WriteBatch {
            pending_writes: Arc::new(Mutex::new(HashMap::new())),
            engine: self,
            options,
        })
    }
}
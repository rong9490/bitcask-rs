use std::{
    collections::HashMap,
};
use bytes::{BufMut, Bytes, BytesMut};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use parking_lot::Mutex;
use crate::data::log_record_mod::log_record::LogRecord;
use crate::db::engine::Engine;
use crate::options::write_batch_options::WriteBatchOptions;
use crate::errors::{AppResult, AppErrors};
// use crate::index::bptree::BPlusTree; // B+树索引
use crate::options::index_type::IndexType;
use crate::data::log_record_mod::log_record_type::LogRecordType;
use super::utils::{log_record_key_with_seq, parse_log_record_key};

const TXN_FIN_KEY: &[u8] = "txn-fin".as_bytes();
pub(crate) const NON_TRANSACTION_SEQ_NO: usize = 0usize;

/// 批量写操作，保证原子性
pub struct WriteBatch<'a> {
    pub pending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>, // 暂存用户写入的数据
    pub engine: &'a Engine, // 涉及引用, 需要生命周期'a
    pub options: WriteBatchOptions,
}

impl WriteBatch<'_> {
    /// 批量操作写数据
    pub fn put(&self, key: Bytes, value: Bytes) -> AppResult<()> {
        if key.is_empty() {
            return Err(AppErrors::KeyIsEmpty);
        }

        // 暂存数据
        let record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NORMAL,
        };

        let mut pending_writes = self.pending_writes.lock();
        pending_writes.insert(key.to_vec(), record);
        Ok(())
    }

    /// 批量操作删除数据
    pub fn delete(&self, key: Bytes) -> AppResult<()> {
        if key.is_empty() {
            return Err(AppErrors::KeyIsEmpty);
        }

        let mut pending_writes = self.pending_writes.lock();
        // 如果数据不存在则直接返回
        let index_pos = self.engine.index.get(key.to_vec());
        if index_pos.is_none() {
            if pending_writes.contains_key(&key.to_vec()) {
                pending_writes.remove(&key.to_vec());
            }
            return Ok(());
        }

        // 暂存数据
        let record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::DELETED,
        };
        pending_writes.insert(key.to_vec(), record);
        Ok(())
    }

    /// 提交数据，将数据写到文件当中，并更新内存索引
    pub fn commit(&self) -> AppResult<()> {
        let mut pending_writes = self.pending_writes.lock();
        if pending_writes.len() == 0 {
            return Ok(());
        }
        if pending_writes.len() > self.options.max_batch_num {
            return Err(AppErrors::ExceedMaxBatchNum);
        }

        // 加锁保证事务提交串行化
        let _lock = self.engine.batch_commit_lock.lock();

        // 获取全局事务序列号
        let seq_no = self.engine.seq_no.fetch_add(1, Ordering::SeqCst);

        let mut positions = HashMap::new();
        // 开始写数据到数据文件当中
        for (_, item) in pending_writes.iter() {
            let mut record = LogRecord {
                key: log_record_key_with_seq(item.key.clone(), seq_no),
                value: item.value.clone(),
                rec_type: item.rec_type,
            };

            let pos = self.engine.append_log_record(&mut record)?;
            positions.insert(item.key.clone(), pos);
        }

        // 写最后一条标识事务完成的数据
        let mut finish_record = LogRecord {
            key: log_record_key_with_seq(TXN_FIN_KEY.to_vec(), seq_no),
            value: Default::default(),
            rec_type: LogRecordType::TXNFINISHED,
        };
        self.engine.append_log_record(&mut finish_record)?;

        // 如果配置了持久化，则 sync
        if self.options.sync_writes {
            self.engine.sync()?;
        }

        // 数据全部写完之后更新内存索引
        for (_, item) in pending_writes.iter() {
            if item.rec_type == LogRecordType::NORMAL {
                let record_pos = positions.get(&item.key).unwrap();
                if let Some(old_pos) = self.engine.index.put(item.key.clone(), *record_pos) {
                    self.engine
                        .reclaim_size
                        .fetch_add(old_pos.size as usize, Ordering::SeqCst);
                }
            }
            if item.rec_type == LogRecordType::DELETED {
                if let Some(old_pos) = self.engine.index.delete(item.key.clone()) {
                    self.engine
                        .reclaim_size
                        .fetch_add(old_pos.size as usize, Ordering::SeqCst);
                }
            }
        }

        // 清空暂存数据
        pending_writes.clear();

        Ok(())
    }
}
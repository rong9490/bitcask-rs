// 第一种索引: bptree

use std::sync::Arc;
use std::path::PathBuf;
use bytes::Bytes;
use jammdb::DB;
use crate::errors::AppResult;
use super::indexer::Indexer;
use super::index_iterator::IndexIterator;
use super::bptree_iterator::BPTreeIterator;
use crate::options::iterator_options::IteratorOptions;
use crate::data::log_record_mod::log_record_pos::LogRecordPos;
use crate::data::log_record_mod::decode_log_record_pos;

const BPTREE_INDEX_FILE_NAME: &str = "bptree-index";
const BPTREE_BUCKET_NAME: &str = "bitcask-index";

// B+树索引
pub struct BPlusTree {
    tree: Arc<DB>,
}

impl BPlusTree {
    // 实例化方法
    pub fn new(dir_path: PathBuf) -> Self {
        // 打开 B+ 树实例，并创建对应的 bucket
        let bptree: DB =
            DB::open(dir_path.join(BPTREE_INDEX_FILE_NAME)).expect("failed to open bptree");
        let tree: Arc<DB> = Arc::new(bptree);
        let tx = tree.tx(true).expect("failed to begin tx");
        tx.get_or_create_bucket(BPTREE_BUCKET_NAME).unwrap();
        tx.commit().unwrap();

        Self { tree: tree.clone() }
    }
}

impl Indexer for BPlusTree {
    fn put(
        &self,
        key: Vec<u8>,
        pos: LogRecordPos,
    ) -> Option<LogRecordPos> {
        let mut result: Option<LogRecordPos> = None;
        let tx = self.tree.tx(true).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();

        // 先获取到旧的值
        if let Some(kv) = bucket.get_kv(&key) {
            let pos = decode_log_record_pos(kv.value().to_vec());
            result = Some(pos);
        }

        // put 新值
        bucket
            .put(key, pos.encode())
            .expect("failed to put value in bptree");
        tx.commit().unwrap();

        result
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let tx = self.tree.tx(false).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        if let Some(kv) = bucket.get_kv(key) {
            return Some(decode_log_record_pos(kv.value().to_vec()));
        }
        None
    }

    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let mut result = None;
        let tx = self.tree.tx(true).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        if let Ok(kv) = bucket.delete(key) {
            let pos = decode_log_record_pos(kv.value().to_vec());
            result = Some(pos);
        }
        tx.commit().unwrap();
        result
    }

    fn list_keys(&self) -> AppResult<Vec<Bytes>> {
        let tx = self.tree.tx(false).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        let mut keys = Vec::new();

        for data in bucket.cursor() {
            keys.push(Bytes::copy_from_slice(data.key()));
        }
        Ok(keys)
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let mut items: Vec<(Vec<u8>, LogRecordPos)> = Vec::new();
        let tx = self.tree.tx(false).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();

        for data in bucket.cursor() {
            let key = data.key().to_vec();
            let pos = decode_log_record_pos(data.kv().value().to_vec());
            items.push((key, pos));
        }
        if options.reverse {
            items.reverse();
        }

        Box::new(BPTreeIterator {
            items,
            curr_index: 0usize,
            options,
        })
    }
}
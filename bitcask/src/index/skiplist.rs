use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use super::index_iterator::IndexIterator;
use super::indexer::Indexer;
use super::skip_iterator::SkipListIterator;
use crate::data::log_record_mod::log_record_pos::LogRecordPos;
use crate::options::iterator_options::IteratorOptions;
use crate::errors::AppResult;

// 跳表索引
pub struct SkipList {
    skl: Arc<SkipMap<Vec<u8>, LogRecordPos>>,
}

impl SkipList {
    pub fn new() -> Self {
        Self {
            skl: Arc::new(SkipMap::new()),
        }
    }
}

impl Indexer for SkipList {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos> {
        let mut result = None;
        if let Some(entry) = self.skl.get(&key) {
            result = Some(*entry.value());
        }
        self.skl.insert(key, pos);
        result
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        if let Some(entry) = self.skl.get(&key) {
            return Some(*entry.value());
        }
        None
    }

    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        if let Some(entry) = self.skl.remove(&key) {
            return Some(*entry.value());
        }
        None
    }

    fn list_keys(&self) -> AppResult<Vec<Bytes>> {
        let mut keys = Vec::with_capacity(self.skl.len());
        for e in self.skl.iter() {
            keys.push(Bytes::copy_from_slice(e.key()));
        }
        Ok(keys)
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let mut items = Vec::with_capacity(self.skl.len());
        // 将 SkipList 中的数据存储到数组中
        for entry in self.skl.iter() {
            items.push((entry.key().clone(), *entry.value()));
        }
        if options.reverse {
            items.reverse();
        }
        Box::new(SkipListIterator {
            items,
            curr_index: 0usize,
            options,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skl_put() {
        let skl = SkipList::new();
        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res1.is_none());
        let res2 = skl.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res2.is_none());
        let res3 = skl.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res3.is_none());
        let res4 = skl.put(
            "ddee".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res4.is_none());

        let res5 = skl.put(
            "ddee".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 93,
                offset: 22,
                size: 11,
            },
        );
        assert!(res5.is_some());
        let v = res5.unwrap();
        assert_eq!(v.file_id, 1123);
        assert_eq!(v.offset, 1232);
    }

    #[test]
    fn test_skl_get() {
        let skl = SkipList::new();

        let v1 = skl.get(b"not exists".to_vec());
        assert!(v1.is_none());

        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res1.is_none());
        let v2 = skl.get(b"aacd".to_vec());
        assert!(v2.is_some());

        let res2 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 990,
                size: 11,
            },
        );
        assert!(res2.is_some());
        let v3 = skl.get(b"aacd".to_vec());
        assert!(v3.is_some());
    }

    #[test]
    fn test_skl_delete() {
        let skl = SkipList::new();

        let r1 = skl.delete(b"not exists".to_vec());
        assert!(r1.is_none());

        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res1.is_none());

        let r2 = skl.delete(b"aacd".to_vec());
        assert!(r2.is_some());
        let v = r2.unwrap();
        assert_eq!(v.file_id, 1123);
        assert_eq!(v.offset, 1232);

        let v2 = skl.get(b"aacd".to_vec());
        assert!(v2.is_none());
    }

    #[test]
    fn test_skl_list_keys() {
        let skl = SkipList::new();

        let keys1 = skl.list_keys();
        assert_eq!(keys1.ok().unwrap().len(), 0);

        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res1.is_none());
        let res2 = skl.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res2.is_none());
        let res3 = skl.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res3.is_none());
        let res4 = skl.put(
            "ddee".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res4.is_none());

        let keys2 = skl.list_keys();
        assert_eq!(keys2.ok().unwrap().len(), 4);
    }

    #[test]
    fn test_skl_iterator() {
        let skl = SkipList::new();

        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res1.is_none());
        let res2 = skl.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res2.is_none());
        let res3 = skl.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res3.is_none());
        let res4 = skl.put(
            "ddee".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
                size: 11,
            },
        );
        assert!(res4.is_none());

        let mut opts = IteratorOptions::default();
        opts.reverse = true;
        let mut iter1 = skl.iterator(opts);

        while let Some((key, _)) = iter1.next() {
            assert!(!key.is_empty());
        }
    }
}


use crate::data::log_record_mod::log_record_pos::LogRecordPos;
use crate::options::iterator_options::IteratorOptions;
use super::index_iterator::IndexIterator;

/// BTree 索引迭代器
pub struct BTreeIterator {
    pub items: Vec<(Vec<u8>, LogRecordPos)>, // 存储 key+索引
    pub curr_index: usize,                   // 当前遍历的位置下标
    pub options: IteratorOptions,            // 配置项
}

impl IndexIterator for BTreeIterator {
    fn rewind(&mut self) {
        self.curr_index = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        self.curr_index = self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }).unwrap_or_else(|insert_val| insert_val);
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.curr_index >= self.items.len() {
            return None;
        }

        while let Some(item) = self.items.get(self.curr_index) {
            self.curr_index += 1;
            let prefix = &self.options.prefix;
            if prefix.is_empty() || item.0.starts_with(&prefix) {
                return Some((&item.0, &item.1));
            }
        }
        None
    }
}

use crate::{
    data::LogRecordPos, index::index_iterator::IndexIterator,
    options::iterator_options::IteratorOptions,
};

/// BTree 索引迭代器
pub struct BTreeIterator {
    pub(crate) items: Vec<(Vec<u8>, LogRecordPos)>, // key+索引
    pub(crate) curr_index: usize,                   // 当前游标
    pub(crate) options: IteratorOptions,            // 配置项
}

impl IndexIterator for BTreeIterator {
    fn rewind(&mut self) -> () {
        self.curr_index = 0;
    }

    fn seek(&mut self, key: Vec<u8>) -> () {
        // HACK 重点理解这个函数
        self.curr_index = match self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }) {
            Ok(equal_val) => equal_val,
            Err(insert_val) => insert_val,
        }
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.curr_index >= self.items.len() {
					return None; // 非法下标
				}

				while let Some(item) = self.items.get(self.curr_index) {
					self.curr_index += 1;
					let prefix: &Vec<u8> = &self.options.prefix;
					if prefix.is_empty() || item.0.starts_with(&prefix) {
						// 查找元组
						return Some((&item.0, &item.1));
					}
				}
				None
    }
}

// HINT Default trait 哪里来的 ? 什么作用 ?
// 提供默认值!

use std::default::Default;

/// 索引迭代器配置项
pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool, // 是否反转
}

impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: false,
        }
    }
}

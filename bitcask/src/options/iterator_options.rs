use std::default::Default;

/// 索引迭代器配置项
pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

/// Default::default()
impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: false,
        }
    }
}

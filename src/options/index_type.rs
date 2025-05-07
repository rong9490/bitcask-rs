#[derive(Clone, PartialEq)]
pub enum IndexType {
    /// BTree 索引
    BTree,

    /// 跳表索引
    SkipList,

    /// B+树索引，将索引存储到磁盘上
    BPlusTree,
}

#[cfg(test)]
mod tests {}
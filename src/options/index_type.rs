#[derive(Clone, PartialEq)]
pub enum IndexType {
    /// BTree 索引
    BTree,
    /// 跳表索引
    SkipList,
    /// B+树索引，将索引存储到磁盘上
    BPlusTree,
}

// #[derive(Clone)]
// pub enum IndexType {
//     /// BTree 索引
//     BTree,
//     /// 跳表索引
//     SkipList,
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indextype() {
        // 枚举及其值的内存大小
        assert_eq!(std::mem::size_of::<IndexType>(), 1);
        assert_eq!(std::mem::size_of_val(&IndexType::BTree), 1);
        assert_eq!(std::mem::size_of_val(&IndexType::SkipList), 1);
        assert_eq!(std::mem::size_of_val(&IndexType::BPlusTree), 1);
    }
}
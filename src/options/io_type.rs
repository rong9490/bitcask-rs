#[derive(Clone, Copy, PartialEq)]
pub enum IOType {
    // 标准文件 IO
    StandardFIO,

    // 内存文件映射
    MemoryMap,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iotype() {
        assert_eq!(std::mem::size_of::<IOType>(), 1);
        assert_eq!(std::mem::size_of_val(&IOType::StandardFIO), 1);
        assert_eq!(std::mem::size_of_val(&IOType::MemoryMap), 1);
    }
}
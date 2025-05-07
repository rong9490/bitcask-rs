#[derive(Clone, Copy, PartialEq)]
pub enum IOType {
    // 标准文件 IO
    StandardFIO,

    // 内存文件映射
    MemoryMap,
}
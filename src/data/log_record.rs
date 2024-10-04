// 数据位置索引信息, 描述数据储存位置

// 日志记录类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogRecordType {
    NORMAL = 1,
    DELETE = 2,
}

pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: LogRecordType,
}

// 日志记录位置: 描述数据存储到哪个位置
#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64, // 偏移
}

// 数据日志结构体, 写到数据文件的数据:

impl LogRecord {
    // 编码LogRecord
    pub fn encode(&mut self) -> Vec<u8> {
        todo!()
    }
}

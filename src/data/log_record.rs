// 数据位置索引信息, 描述数据储存位置

// 日志记录类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogRecordType {
    // 正常 put 的数据
    NORMAL = 1,

    // 被删除的数据标识，墓碑值
    DELETED = 2,
}

pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

// 日志记录位置: 描述数据存储到哪个位置
#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64, // 偏移
}

/// 从数据文件中读取的 log_record 信息，包含其 size
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: u64,
}

// 数据日志结构体, 写到数据文件的数据:

impl LogRecord {
    // 编码LogRecord
    pub fn encode(&mut self) -> Vec<u8> {
        todo!()
    }
}

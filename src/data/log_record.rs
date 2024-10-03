// 数据位置索引信息, 描述数据储存位置

use bytes::Bytes;

// 日志记录位置: 描述数据存储到哪个位置
#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    // pub(crate) 项目内可访问
    pub(crate) file_id: u32,
    pub(crate) offset: u64, // 偏移
}

// 日志记录类型
pub enum LogRecordType {
    NORMAL = 1,
    DELETE = 2,
}

// 数据日志结构体, 写到数据文件的数据:
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: LogRecordType,
}

impl LogRecord {
    // 编码LogRecord
    pub fn encode(&mut self) -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();
        data.extend_from_slice(&self.key);
        data.extend_from_slice(&self.value);
        data.extend_from_slice(&self.record_type.to_bytes());
        data
        // Ok(())
    }
}
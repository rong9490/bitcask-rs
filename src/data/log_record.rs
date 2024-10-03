// 数据位置索引信息, 描述数据储存位置

// 日志记录位置: 描述数据存储到哪个位置
#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    // pub(crate) 项目内可访问
    pub(crate) file_id: u32,
    pub(crate) offset: u64, // 偏移
}

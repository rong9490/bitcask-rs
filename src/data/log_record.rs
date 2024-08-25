// 数据位置索引信息, 描述数据储存位置

// 日志记录位置: 都需要pub
#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

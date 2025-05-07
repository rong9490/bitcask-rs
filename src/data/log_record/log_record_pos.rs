/// 数据位置索引信息，描述数据存储到了哪个位置
#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32, // 文件 id，表示将数据存储到了哪个文件当中
    pub(crate) offset: u64,  // 偏移，表示将数据存储到了数据文件中的哪个位置
}
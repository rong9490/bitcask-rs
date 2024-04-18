// 数据位置索引信息, 描述数据储存位置

#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
  pub file_id: u32,
  pub offset: u64,
}
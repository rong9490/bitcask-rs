use bytes::BytesMut;
use prost::encoding::encode_varint;

/// 数据位置索引信息，描述数据存储到了哪个位置
#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32, // 文件 id，表示将数据存储到了哪个文件当中
    pub(crate) offset: u64,  // 偏移，表示将数据存储到了数据文件中的哪个位置
    pub(crate) size: u32,    // 数据在磁盘上的占据的空间大小
}

impl LogRecordPos {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf: BytesMut = BytesMut::new();
        encode_varint(self.file_id as u64, &mut buf);
        encode_varint(self.offset, &mut buf);
        encode_varint(self.size as u64, &mut buf);
        buf.to_vec()
    }
}

pub mod log_record_type;
pub mod log_record_pos;
pub mod log_record;

use bytes::{BufMut, BytesMut};
use prost::{length_delimiter_len, encoding::{decode_varint}};
use self::log_record_pos::LogRecordPos;

/// 获取 LogRecord header 部分的最大长度
pub fn max_log_record_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}

/// 解码 LogRecordPos
pub fn decode_log_record_pos(pos: Vec<u8>) -> LogRecordPos {
    let mut buf = BytesMut::new();
    buf.put_slice(&pos);

    let fid = match decode_varint(&mut buf) {
        Ok(fid) => fid,
        Err(e) => panic!("decode log record pos err: {}", e),
    };
    let offset = match decode_varint(&mut buf) {
        Ok(offset) => offset,
        Err(e) => panic!("decode log record pos err: {}", e),
    };
    let size = match decode_varint(&mut buf) {
        Ok(size) => size,
        Err(e) => panic!("decode log record pos err: {}", e),
    };
    LogRecordPos {
        file_id: fid as u32,
        offset,
        size: size as u32,
    }
}

use super::LogRecordPos;
use bytes::{BufMut, BytesMut};
use prost::{
    encode_length_delimiter,
    encoding::{decode_varint, encode_varint},
    length_delimiter_len,
};

/// 获取 LogRecord header 部分的最大长度
pub fn max_log_record_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}

/// 解码 LogRecordPos
pub fn decode_log_record_pos(pos: Vec<u8>) -> LogRecordPos {
    let mut buf: BytesMut = BytesMut::new();
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
        Err(e) => panic!("deode log record pos err: {}", e),
    };

    LogRecordPos {
        file_id: fid as u32,
        offset,
        size: size as u32,
    }
}

#[cfg(test)]
mod tests {}
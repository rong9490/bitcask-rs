


/// 从数据文件中读取的 log_record 信息，包含其 size
#[derive(Debug)]
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: usize,
}

// 暂存事务数据信息
pub struct TransactionRecord {
    pub(crate) record: LogRecord,
    pub(crate) pos: LogRecordPos,
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_encode_and_crc() {
        // 正常的一条 LogRecord 编码
        let rec1 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogRecordType::NORMAL,
        };
        let enc1 = rec1.encode();
        assert!(enc1.len() > 5);
        assert_eq!(1020360578, rec1.get_crc());

        // LogRecord 的 value 为空
        let rec2 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::NORMAL,
        };
        let enc2 = rec2.encode();
        assert!(enc2.len() > 5);
        assert_eq!(3756865478, rec2.get_crc());

        // 类型为 Deleted 的情况
        let rec3 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogRecordType::DELETED,
        };
        let enc3 = rec3.encode();
        assert!(enc3.len() > 5);
        assert_eq!(1867197446, rec3.get_crc());
    }
}

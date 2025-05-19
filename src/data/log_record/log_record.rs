use super::log_record_type::LogRecordType;
use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len}; // 这是什么库?

/// LogRecord 写入到数据文件的记录
/// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
#[derive(Debug)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

impl LogRecord {
    // encode 对 LogRecord 进行编码，返回字节数组及长度
    //	+-------------+--------------+-------------+--------------+-------------+-------------+
    //	|  type 类型   |    key size |   value size |      key    |      value   |  crc 校验值  |
    //	+-------------+-------------+--------------+--------------+-------------+-------------+
    //	    1字节        变长（最大5）   变长（最大5）        变长           变长           4字节
    pub fn encode(&self) -> Vec<u8> {
        let (enc_buf, _) = self.encode_and_get_crc();
        enc_buf
    }

    pub fn get_crc(&self) -> u32 {
        let (_, crc_value) = self.encode_and_get_crc();
        crc_value
    }

    fn encode_and_get_crc(&self) -> (Vec<u8>, u32) {
        // 初始化字节数组，存放编码数据
        let mut buf = BytesMut::new();
        buf.reserve(self.encoded_length());

        // 第一个字节存放 Type 类型
        buf.put_u8(self.rec_type as u8);

        // 再存储 key 和 value 的长度
        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();

        // 存储 key 和 value
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // 计算并存储 CRC 校验值
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc);

        (buf.to_vec(), crc)
    }

    // LogRecord 编码后的长度
    fn encoded_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + 4
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_memory_size  () {
        let rec = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs".as_bytes().to_vec(),
            rec_type: LogRecordType::NORMAL,
        };
        assert_eq!(rec.encoded_length(), 14);
    }

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

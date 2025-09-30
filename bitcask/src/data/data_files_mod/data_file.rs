use std::{sync::Arc, path::PathBuf};
use parking_lot::RwLock;
use crate::fio::IOManager;

// 数据文件
pub struct DataFile {
    file_id: Arc<RwLock<u32>>,           // 数据文件id
    write_off: Arc<RwLock<u64>>,         // 当前写偏移，记录该数据文件写到哪个位置了
    io_manager: Box<dyn IOManager>, // IO 管理接口
}

// impl DataFile {
//     /// 创建或打开一个新的数据文件
//     pub fn new(dir_path: PathBuf, file_id: u32, io_type: IOType) -> Result<DataFile> {
//         // 根据 path 和 id 构造出完整的文件名称
//         let file_name = get_data_file_name(dir_path, file_id);
//         // 初始化 io manager
//         let io_manager = new_io_manager(file_name, io_type);

//         Ok(DataFile {
//             file_id: Arc::new(RwLock::new(file_id)),
//             write_off: Arc::new(RwLock::new(0)),
//             io_manager,
//         })
//     }

//     /// 新建或打开 hint 索引文件
//     pub fn new_hint_file(dir_path: PathBuf) -> Result<DataFile> {
//         let file_name = dir_path.join(HINT_FILE_NAME);
//         let io_manager = new_io_manager(file_name, IOType::StandardFIO);

//         Ok(DataFile {
//             file_id: Arc::new(RwLock::new(0)),
//             write_off: Arc::new(RwLock::new(0)),
//             io_manager,
//         })
//     }

//     /// 新建或打开标识 merge 完成的文件
//     pub fn new_merge_fin_file(dir_path: PathBuf) -> Result<DataFile> {
//         let file_name = dir_path.join(MERGE_FINISHED_FILE_NAME);
//         let io_manager = new_io_manager(file_name, IOType::StandardFIO);

//         Ok(DataFile {
//             file_id: Arc::new(RwLock::new(0)),
//             write_off: Arc::new(RwLock::new(0)),
//             io_manager,
//         })
//     }

//     /// 新建或打开存储事务序列号的文件
//     pub fn new_seq_no_file(dir_path: PathBuf) -> Result<DataFile> {
//         let file_name = dir_path.join(SEQ_NO_FILE_NAME);
//         let io_manager = new_io_manager(file_name, IOType::StandardFIO);

//         Ok(DataFile {
//             file_id: Arc::new(RwLock::new(0)),
//             write_off: Arc::new(RwLock::new(0)),
//             io_manager,
//         })
//     }

//     pub fn file_size(&self) -> u64 {
//         self.io_manager.size()
//     }

//     pub fn get_write_off(&self) -> u64 {
//         let read_guard = self.write_off.read();
//         *read_guard
//     }

//     pub fn set_write_off(&self, offset: u64) {
//         let mut write_guard = self.write_off.write();
//         *write_guard = offset;
//     }

//     pub fn get_file_id(&self) -> u32 {
//         let read_guard = self.file_id.read();
//         *read_guard
//     }

//     /// 根据 offset 从数据文件中读取 LogRecord
//     pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
//         // 先读取出 header 部分的数据
//         let mut header_buf = BytesMut::zeroed(max_log_record_header_size());

//         self.io_manager.read(&mut header_buf, offset)?;

//         // 取出 type，在第一个字节
//         let rec_type = header_buf.get_u8();

//         // 取出 key 和 value 的长度
//         let key_size = decode_length_delimiter(&mut header_buf).unwrap();
//         let value_size = decode_length_delimiter(&mut header_buf).unwrap();

//         // 如果 key 和 value 均为空，则说明读取到了文件的末尾，直接返回
//         if key_size == 0 && value_size == 0 {
//             return Err(Errors::ReadDataFileEOF);
//         }

//         // 获取实际的 header 大小
//         let actual_header_size =
//             length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;

//         // 读取实际的 key 和 value，最后的 4 个字节是 crc 校验值
//         let mut kv_buf = BytesMut::zeroed(key_size + value_size + 4);
//         self.io_manager
//             .read(&mut kv_buf, offset + actual_header_size as u64)?;

//         // 构造 LogRecord
//         let log_record = LogRecord {
//             key: kv_buf.get(..key_size).unwrap().to_vec(),
//             value: kv_buf.get(key_size..kv_buf.len() - 4).unwrap().to_vec(),
//             rec_type: LogRecordType::from_u8(rec_type),
//         };

//         // 向前移动到最后的 4 个字节，就是 crc 的值
//         kv_buf.advance(key_size + value_size);

//         if kv_buf.get_u32() != log_record.get_crc() {
//             return Err(Errors::InvalidLogRecordCrc);
//         }

//         // 构造结果并返回
//         Ok(ReadLogRecord {
//             record: log_record,
//             size: actual_header_size + key_size + value_size + 4,
//         })
//     }

//     pub fn write(&self, buf: &[u8]) -> Result<usize> {
//         let n_bytes = self.io_manager.write(buf)?;
//         // 更新 write_off 字段
//         let mut write_off = self.write_off.write();
//         *write_off += n_bytes as u64;

//         Ok(n_bytes)
//     }

//     /// 写 hint 索引到文件当中
//     pub fn write_hint_record(&self, key: Vec<u8>, pos: LogRecordPos) -> Result<()> {
//         let hint_record = LogRecord {
//             key,
//             value: pos.encode(),
//             rec_type: LogRecordType::NORMAL,
//         };
//         let enc_record = hint_record.encode();
//         self.write(&enc_record)?;
//         Ok(())
//     }

//     pub fn sync(&self) -> Result<()> {
//         self.io_manager.sync()
//     }

//     pub fn set_io_manager(&mut self, dir_path: PathBuf, io_type: IOType) {
//         self.io_manager = new_io_manager(get_data_file_name(dir_path, self.get_file_id()), io_type);
//     }
// }

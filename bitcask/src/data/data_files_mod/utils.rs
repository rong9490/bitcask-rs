use std::{path::PathBuf};

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";

/// 获取文件名称
pub fn get_data_file_name(dir_path: PathBuf, file_id: u32) -> PathBuf {
    let name: String = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
    dir_path.join(name)
}

pub const HINT_FILE_NAME: &str = "hint-index";
pub const MERGE_FINISHED_FILE_NAME: &str = "merge-finished";
pub const SEQ_NO_FILE_NAME: &str = "seq-no";

// TODO 补充单测
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_new_data_file() {
//         let dir_path = std::env::temp_dir();
//         let data_file_res1 = DataFile::new(dir_path.clone(), 0, IOType::StandardFIO);
//         assert!(data_file_res1.is_ok());
//         let data_file1 = data_file_res1.unwrap();
//         assert_eq!(data_file1.get_file_id(), 0);
//
//         let data_file_res2 = DataFile::new(dir_path.clone(), 0, IOType::StandardFIO);
//         assert!(data_file_res2.is_ok());
//         let data_file2 = data_file_res2.unwrap();
//         assert_eq!(data_file2.get_file_id(), 0);
//
//         let data_file_res3 = DataFile::new(dir_path.clone(), 660, IOType::StandardFIO);
//         assert!(data_file_res3.is_ok());
//         let data_file3 = data_file_res3.unwrap();
//         assert_eq!(data_file3.get_file_id(), 660);
//     }
//
//     #[test]
//     fn test_data_file_write() {
//         let dir_path = std::env::temp_dir();
//         let data_file_res1 = DataFile::new(dir_path.clone(), 100, IOType::StandardFIO);
//         assert!(data_file_res1.is_ok());
//         let data_file1 = data_file_res1.unwrap();
//         assert_eq!(data_file1.get_file_id(), 100);
//
//         let write_res1 = data_file1.write("aaa".as_bytes());
//         assert!(write_res1.is_ok());
//         assert_eq!(write_res1.unwrap(), 3 as usize);
//
//         let write_res2 = data_file1.write("bbb".as_bytes());
//         assert!(write_res2.is_ok());
//         assert_eq!(write_res2.unwrap(), 3 as usize);
//
//         let write_res3 = data_file1.write("ccc".as_bytes());
//         assert!(write_res3.is_ok());
//         assert_eq!(write_res3.unwrap(), 3 as usize);
//     }
//
//     #[test]
//     fn test_data_file_sync() {
//         let dir_path = std::env::temp_dir();
//         let data_file_res1 = DataFile::new(dir_path.clone(), 200, IOType::StandardFIO);
//         assert!(data_file_res1.is_ok());
//         let data_file1 = data_file_res1.unwrap();
//         assert_eq!(data_file1.get_file_id(), 200);
//
//         let sync_res = data_file1.sync();
//         assert!(sync_res.is_ok());
//     }
//
//     #[test]
//     fn test_data_file_read_log_record() {
//         let dir_path = std::env::temp_dir();
//         let data_file_res1 = DataFile::new(dir_path.clone(), 700, IOType::StandardFIO);
//         assert!(data_file_res1.is_ok());
//         let data_file1 = data_file_res1.unwrap();
//         assert_eq!(data_file1.get_file_id(), 700);
//
//         let enc1 = LogRecord {
//             key: "name".as_bytes().to_vec(),
//             value: "bitcask-rs-kv".as_bytes().to_vec(),
//             rec_type: LogRecordType::NORMAL,
//         };
//         let write_res1 = data_file1.write(&enc1.encode());
//         assert!(write_res1.is_ok());
//
//         // 从起始位置读取
//         let read_res1 = data_file1.read_log_record(0);
//         assert!(read_res1.is_ok());
//         let read_enc1 = read_res1.ok().unwrap().record;
//         assert_eq!(enc1.key, read_enc1.key);
//         assert_eq!(enc1.value, read_enc1.value);
//         assert_eq!(enc1.rec_type, read_enc1.rec_type);
//
//         // 从新的位置开启读取
//         let enc2 = LogRecord {
//             key: "name".as_bytes().to_vec(),
//             value: "new-value".as_bytes().to_vec(),
//             rec_type: LogRecordType::NORMAL,
//         };
//         let write_res2 = data_file1.write(&enc2.encode());
//         assert!(write_res2.is_ok());
//
//         let read_res2 = data_file1.read_log_record(24);
//         assert!(read_res2.is_ok());
//         let read_enc2 = read_res2.ok().unwrap().record;
//         assert_eq!(enc2.key, read_enc2.key);
//         assert_eq!(enc2.value, read_enc2.value);
//         assert_eq!(enc2.rec_type, read_enc2.rec_type);
//
//         // 类型是 Deleted
//         let enc3 = LogRecord {
//             key: "name".as_bytes().to_vec(),
//             value: Default::default(),
//             rec_type: LogRecordType::DELETED,
//         };
//         let write_res3 = data_file1.write(&enc3.encode());
//         assert!(write_res3.is_ok());
//
//         let read_res3 = data_file1.read_log_record(44);
//         assert!(read_res3.is_ok());
//         let read_enc3 = read_res3.ok().unwrap().record;
//         assert_eq!(enc3.key, read_enc3.key);
//         assert_eq!(enc3.value, read_enc3.value);
//         assert_eq!(enc3.rec_type, read_enc3.rec_type);
//     }
// }

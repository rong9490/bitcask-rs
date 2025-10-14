use bytes::{BufMut, BytesMut};
use prost::{decode_length_delimiter, encode_length_delimiter};

// 编码 seq no 和 key
pub(crate) fn log_record_key_with_seq(key: Vec<u8>, seq_no: usize) -> Vec<u8> {
    let mut enc_key: BytesMut = BytesMut::new();
    encode_length_delimiter(seq_no, &mut enc_key).unwrap();
    enc_key.extend_from_slice(&key.to_vec());
    enc_key.to_vec()
}

// 解析 LogRecord 的 key，拿到实际的 key 和 seq no
pub(crate) fn parse_log_record_key(key: Vec<u8>) -> (Vec<u8>, usize) {
    let mut buf: BytesMut = BytesMut::new();
    buf.put_slice(&key);
    let seq_no: usize = decode_length_delimiter(&mut buf).unwrap();
    (buf.to_vec(), seq_no)
}

// #[cfg(test)]
// mod tests {
//     use std::path::PathBuf;
//
//     use crate::{options::Options, util};
//
//     use super::*;
//
//     #[test]
//     fn test_write_batch_1() {
//         let mut opts = Options::default();
//         opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-1");
//         opts.data_file_size = 64 * 1024 * 1024;
//         let engine = Engine::open(opts.clone()).expect("failed to open engine");
//
//         let wb = engine
//             .new_write_batch(WriteBatchOptions::default())
//             .expect("failed to create write batch");
//         // 写数据之后未提交
//         let put_res1 = wb.put(
//             util::rand_kv::get_test_key(1),
//             util::rand_kv::get_test_value(10),
//         );
//         assert!(put_res1.is_ok());
//         let put_res2 = wb.put(
//             util::rand_kv::get_test_key(2),
//             util::rand_kv::get_test_value(10),
//         );
//         assert!(put_res2.is_ok());
//
//         let res1 = engine.get(util::rand_kv::get_test_key(1));
//         assert_eq!(Errors::KeyNotFound, res1.err().unwrap());
//
//         // 事务提交之后进行查询
//         let commit_res = wb.commit();
//         assert!(commit_res.is_ok());
//
//         let res2 = engine.get(util::rand_kv::get_test_key(1));
//         assert!(res2.is_ok());
//
//         // 验证事务序列号
//         let seq_no = wb.engine.seq_no.load(Ordering::SeqCst);
//         assert_eq!(2, seq_no);
//
//         // 删除测试的文件夹
//         std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
//     }
//
//     #[test]
//     fn test_write_batch_2() {
//         let mut opts = Options::default();
//         opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-2");
//         opts.data_file_size = 64 * 1024 * 1024;
//         let engine = Engine::open(opts.clone()).expect("failed to open engine");
//
//         let wb = engine
//             .new_write_batch(WriteBatchOptions::default())
//             .expect("failed to create write batch");
//         let put_res1 = wb.put(
//             util::rand_kv::get_test_key(1),
//             util::rand_kv::get_test_value(10),
//         );
//         assert!(put_res1.is_ok());
//         let put_res2 = wb.put(
//             util::rand_kv::get_test_key(2),
//             util::rand_kv::get_test_value(10),
//         );
//         assert!(put_res2.is_ok());
//         let commit_res1 = wb.commit();
//         assert!(commit_res1.is_ok());
//
//         let put_res3 = wb.put(
//             util::rand_kv::get_test_key(1),
//             util::rand_kv::get_test_value(10),
//         );
//         assert!(put_res3.is_ok());
//
//         let commit_res2 = wb.commit();
//         assert!(commit_res2.is_ok());
//
//         // 重启之后进行校验
//         engine.close().expect("failed to close");
//         std::mem::drop(engine);
//
//         let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
//         let keys = engine2.list_keys();
//         assert_eq!(2, keys.ok().unwrap().len());
//
//         // 验证事务序列号
//         let seq_no = engine2.seq_no.load(Ordering::SeqCst);
//         assert_eq!(3, seq_no);
//
//         // 删除测试的文件夹
//         std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
//     }
//
//     // #[test]
//     // fn test_write_batch_3() {
//     //     let mut opts = Options::default();
//     //     opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-3");
//     //     opts.data_file_size = 64 * 1024 * 1024;
//     //     let engine = Engine::open(opts.clone()).expect("failed to open engine");
//
//     //     let keys = engine.list_keys();
//     //     println!("key len {:?}", keys);
//
//     //     // let mut wb_opts = WriteBatchOptions::default();
//     //     // wb_opts.max_batch_num = 10000000;
//     //     // let wb = engine.new_write_batch(wb_opts).expect("failed to create write batch");
//
//     //     // for i in 0..=1000000 {
//     //     //     let put_res = wb.put(util::rand_kv::get_test_key(i), util::rand_kv::get_test_value(10));
//     //     //     assert!(put_res.is_ok());
//     //     // }
//
//     //     // wb.commit();
//     // }
// }
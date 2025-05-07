use std::path::PathBuf;

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";

/// 获取文件名称
pub fn get_data_file_name(dir_path: PathBuf, file_id: u32) -> PathBuf {
  let name = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
  dir_path.join(name)
}
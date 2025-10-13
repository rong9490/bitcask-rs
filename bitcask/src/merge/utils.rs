use std::path::{Path, PathBuf};
use std::ffi::OsStr;

const MERGE_DIR_NAME: &str = "merge";
const MERGE_FIN_KEY: &[u8] = "merge.finished".as_bytes();

// 获取临时的用于 merge 的数据目录
fn get_merge_path(dir_path: PathBuf) -> PathBuf {
    let file_name: &OsStr = dir_path.file_name().unwrap();
    let merge_name: String = std::format!("{}-{}", file_name.to_str().unwrap(), MERGE_DIR_NAME);
    let parent: &Path = dir_path.parent().unwrap();
    parent.to_path_buf().join(merge_name)
}
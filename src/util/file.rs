use fs2;
use fs_extra;
use std::{
    fs::{self, DirEntry},
    io,
    path::PathBuf,
};

// 获取磁盘剩余空间容量
pub fn available_disk_size() -> u64 {
    // 根目录(直接处理了异常)
    if let Ok(size) = fs2::available_space(PathBuf::from("/")) {
        return size;
    }
    0
}

// 磁盘数据目录的大小
pub fn dir_disk_size(dir_path: PathBuf) -> u64 {
    if let Ok(size) = fs_extra::dir::get_size(dir_path) {
        return size;
    }
    0
}

// 拷贝数据目录
pub fn copy_dir(src: PathBuf, dest: PathBuf, exclude: &[&str]) -> io::Result<Vec<String>> {
    let mut vec: Vec<String> = Vec::new();
 
    // 如果目标目录不存在，则创建
    if !dest.exists() {
        fs::create_dir_all(&dest)?; // -> io::Result<()>
        vec.push(dest.to_string_lossy().to_string());
    }

    // io::Result<ReadDir>
    for dir_entry in fs::read_dir(src)? {
        let entry: DirEntry = dir_entry?; // 目录项
        let entry_path: PathBuf = entry.path(); // 其路径

        // 排除列表! 迭代器(any) + 闭包函数; 字符串结尾判断
        if exclude.iter().any(|&x| entry_path.ends_with(x)) {
            continue;
        }

        let dest_path: PathBuf = dest.join(entry.file_name());
        // 如果目录项是目录，则递归拷贝
        if entry.file_type()?.is_dir() {
            copy_dir(entry_path, dest_path, exclude)?;
        } else {
            let dest_str: String = dest_path.to_string_lossy().to_string();
            vec.push(dest_str);
            // 拷贝文件
            fs::copy(entry_path, dest_path)?;
        }
    }
    Ok(vec)
}

#[cfg(test)]
mod test_file {
    use super::*;

    #[test]
    fn test_available_disk_size() {
        // 193165889536
        let size: u64 = available_disk_size();
        println!("size: {}", size);
        assert!(size > 0);
    }

    #[test]
    #[should_panic]
    fn test_dir_disk_size() {
        let size: u64 = dir_disk_size(PathBuf::from("/"));
        println!("size: {}", size);
        assert!(size > 0);
    }

    #[test]
    fn test_copy_dir() {
        let result: Result<Vec<String>, io::Error> = copy_dir(
            PathBuf::from("src/util/__tmp__"),
            PathBuf::from("src/util/tmp2"),
            &[],
        );
        assert!(result.is_ok());

        let vec: Vec<String> = result.unwrap();
        println!("vec: {:?}", vec);

        let len: usize = vec.len();
        assert_eq!(len, 3);

        let dir: &str = &vec[0];
        let file: &str = &vec[1];
        let file2: &str = &vec[2];
        assert_eq!(dir, "src/util/tmp2");
        assert_eq!(file2, "src/util/tmp2/a.txt");
        assert_eq!(file, "src/util/tmp2/b.txt");
        // 清理测试目录
        let _ = fs::remove_dir_all(PathBuf::from("src/util/tmp2"));
    }
}

// cargo test util::file -- --nocapture

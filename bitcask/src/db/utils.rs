use std::fs;
use std::path::PathBuf;
use crate::errors::{AppResult, AppErrors};
use crate::data::data_files_mod::data_file::DataFile;
use crate::data::data_files_mod::utils::DATA_FILE_NAME_SUFFIX;
use crate::options::io_type::IOType;
use crate::options::options::Options;

// 从数据目录中加载数据文件
pub fn load_data_files(dir_path: PathBuf, use_mmap: bool) -> AppResult<Vec<DataFile>> {
    // 读取数据目录
    let dir = fs::read_dir(dir_path.clone());
    if dir.is_err() {
        return Err(AppErrors::FailedToReadDatabaseDir);
    }

    let mut file_ids: Vec<u32> = Vec::new();
    let mut data_files: Vec<DataFile> = Vec::new();
    for file in dir.unwrap() {
        if let Ok(entry) = file {
            // 拿到文件名
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();

            // 判断文件名称是否是以 .data 结尾
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                let split_names: Vec<&str> = file_name.split(".").collect();
                let file_id = match split_names[0].parse::<u32>() {
                    Ok(fid) => fid,
                    Err(_) => {
                        return Err(AppErrors::DataDirectoryCorrupted);
                    }
                };
                file_ids.push(file_id);
            }
        }
    }

    // 如果没有数据文件，则直接返回
    if file_ids.is_empty() {
        return Ok(data_files);
    }

    // 对文件 id 进行排序，从小到大进行加载
    file_ids.sort();
    // 遍历所有的文件id，依次打开对应的数据文件
    for file_id in file_ids.iter() {
        let mut io_type = IOType::StandardFIO;
        if use_mmap {
            io_type = IOType::MemoryMap;
        }
        let data_file = DataFile::new(dir_path.clone(), *file_id, io_type)?;
        data_files.push(data_file);
    }

    Ok(data_files)
}

pub fn check_options(opts: &Options) -> Option<AppErrors> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().len() == 0 {
        return Some(AppErrors::DirPathIsEmpty);
    }

    if opts.data_file_size <= 0u64 {
        return Some(AppErrors::DataFileSizeTooSmall);
    }

    if opts.data_file_merge_ratio < 0f32 || opts.data_file_merge_ratio > 1f32 {
        return Some(AppErrors::InvalidMergeRatio);
    }

    None
}
use std::fs;
use std::path::PathBuf;

// 从数据目录中加载数据文件
fn load_data_files(dir_path: PathBuf, use_mmap: bool) -> Result<Vec<DataFile>> {
    // 读取数据目录
    let dir = fs::read_dir(dir_path.clone());
    if dir.is_err() {
        return Err(Errors::FailedToReadDatabaseDir);
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
                        return Err(Errors::DataDirectoryCorrupted);
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

fn check_options(opts: &Options) -> Option<Errors> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().len() == 0 {
        return Some(Errors::DirPathIsEmpty);
    }

    if opts.data_file_size <= 0 {
        return Some(Errors::DataFileSizeTooSmall);
    }

    if opts.data_file_merge_ratio < 0 as f32 || opts.data_file_merge_ratio > 1 as f32 {
        return Some(Errors::InvalidMergeRatio);
    }

    None
}
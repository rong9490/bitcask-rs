use crate::options::options::Options;
use crate::errors::Errors;

pub fn check_options(opts: & Options) -> Option<Errors> {
    let dir_path: Option<&str> = opts.dir_path.to_str();

    if dir_path.is_none() || dir_path.unwrap().len() == 0 {
      return Some(Errors::DirPathIsEmpty);
    }

    if opts.data_file_size <= 0 {
      return Some(Errors::DataFileSizeTooSmall);
    }

    if opts

  todo!()
}
use bytes::Bytes;

#[allow(dead_code)]
pub fn get_test_key(i: usize) -> Bytes {
    assert_eq!(std::mem::size_of::<usize>(), 8);
    assert_eq!(std::mem::size_of_val(&i), 8);

    // 格式化参数: 数字字少为9位宽, 不足的补零
    let key: String = std::format!("bitcask-rs-key-{:09}", i);
    assert_eq!(key.len(), 24);
    assert!(key.starts_with("bitcask-rs-key-"));
    assert_eq!(std::mem::size_of_val(&key), 24); // 固定的24字节指针
    // Bytes::from(key) Converts to this type from the input type

    // 是个结构体: size = 32 (0x20), align = 0x8
    let b: Bytes = Bytes::from(key);
    assert_eq!(b.len(), 24);
    assert_eq!(std::mem::size_of_val(&b), 32);
    b
}

#[allow(dead_code)]
pub fn get_test_value(i: usize) -> Bytes {
    Bytes::from(std::format!(
        "bitcask-rs-value-value-value-value-value-value-value-value-value-{:09}",
        i
    ))
}

// 测试空间
#[cfg(test)]
mod test_rand_kv {
    use super::*;

    #[test]
    fn _get_test_key() {
        for i in 0..=1 {
            assert!(get_test_key(i).len() > 0)
        }
    }

    #[test]
    fn _get_test_value() {
        for i in 0..=1 {
            assert!(get_test_value(i).len() > 0)
        }
    }
}

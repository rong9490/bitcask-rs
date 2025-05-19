#[derive(PartialEq, Clone, Copy, Debug)]
#[repr(u8)] // 明确指定该枚举类型值为u8, 也就是占用8位(1个字节)
pub enum LogRecordType {
    // 正常 put 的数据
    NORMAL = 1,
    // 被删除的数据标识，墓碑值
    DELETED = 2,
    // 事务完成的标识
    TXNFINISHED = 3,
}

impl LogRecordType {
    // 转换函数: 类型收窄, 转为枚举值
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETED,
            3 => LogRecordType::TXNFINISHED,
            v => panic!("非法的 log_record_type: {}", v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_type() {
        assert_eq!(std::mem::size_of::<LogRecordType>(), 1); // 这种写法叫做turbofish
                                                             // 三个枚举值的内存相等, 大小都等于枚举的大小
        let normal: LogRecordType = LogRecordType::NORMAL;
        let deleted: LogRecordType = LogRecordType::DELETED;
        let tenfinished: LogRecordType = LogRecordType::TXNFINISHED;
        assert_eq!(
            std::mem::size_of_val(&normal),
            std::mem::size_of_val(&deleted)
        ); // 参数是传入引用, 而不是所有权
        assert_eq!(
            std::mem::size_of_val(&normal),
            std::mem::size_of_val(&tenfinished)
        );
        assert_eq!(std::mem::size_of_val(&normal), 1);
    }

    #[test]
    fn test_log_record_type_from_u8() {
        let v1: u8 = 1;
        let v2: u8 = 2;
        let v3: u8 = 3;
        assert_eq!(LogRecordType::from_u8(v1), LogRecordType::NORMAL);
        assert_eq!(LogRecordType::from_u8(v2), LogRecordType::DELETED);
        assert_eq!(LogRecordType::from_u8(v3), LogRecordType::TXNFINISHED);
    }

    #[test]
    #[should_panic(expected = "illegal log_record_type!!")]
    fn test_log_record_type_from_u8_should_panic() {
        let v4: u8 = 4;
        LogRecordType::from_u8(v4);
    }
}

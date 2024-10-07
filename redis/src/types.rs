use std::time::{self, Duration, SystemTime};

use bitcask_rs::{
    db,
    errors::{Errors, Result},
    options::{Options, WriteBatchOptions},
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::meta::{
    decode_metadata, HashInternalKey, ListInternalKey, Metadata, SetInternalKey, ZSetInternalKey,
};

const INITIAL_LIST_MARK: u64 = std::u64::MAX / 2;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum RedisDataType {
    String,
    Hash,
    Set,
    List,
    ZSet,
}

impl From<u8> for RedisDataType {
    fn from(value: u8) -> Self {
        match value {
            0 => RedisDataType::String,
            1 => RedisDataType::Hash,
            2 => RedisDataType::Set,
            3 => RedisDataType::List,
            4 => RedisDataType::ZSet,
            _ => panic!("invalid redis data type"),
        }
    }
}

// Redis 数据结构服务
pub struct RedisDataStructure {
    pub(crate) eng: db::Engine,
}

impl RedisDataStructure {
    pub fn new(options: Options) -> Result<Self> {
        let engine = db::Engine::open(options)?;
        Ok(Self { eng: engine })
    }

    /// =============== String 数据结构 ===============

    pub fn set(&self, key: &str, ttl: std::time::Duration, value: &str) -> Result<()> {
        if value.len() == 0 {
            return Ok(());
        }

        // 编码 value : type + expire + payload
        let mut buf = BytesMut::new();
        buf.put_u8(RedisDataType::String as u8);
        let mut expire = 0;
        if ttl != Duration::ZERO {
            if let Some(v) = SystemTime::now().checked_add(ttl) {
                expire = v.duration_since(time::UNIX_EPOCH).unwrap().as_nanos();
            }
        }
        buf.put_u128(expire);

        buf.extend_from_slice(value.as_bytes());

        // 调用存储引擎的接口写入
        self.eng
            .put(Bytes::copy_from_slice(key.as_bytes()), buf.into())?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let mut buf = self.eng.get(Bytes::copy_from_slice(key.as_bytes()))?;
        let key_type = RedisDataType::from(buf.get_u8());
        if key_type != RedisDataType::String {
            return Err(Errors::WrongTypeOperation);
        }

        // 判断过期时间
        let expire = buf.get_u128();
        if expire > 0 {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            if expire <= now {
                return Ok(None);
            }
        }

        let value = buf.to_vec();
        Ok(Some(String::from_utf8(value).unwrap()))
    }

    /// =============== Hash 数据结构 ===============

    pub fn hset(&self, key: &str, field: &str, value: &str) -> Result<bool> {
        // 查询元数据
        let mut meta = self.find_metadata(key, RedisDataType::Hash)?;

        // 初始化数据部分的 key
        let hk = HashInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            field: field.as_bytes().to_vec(),
        };

        let mut exist = true;
        // 查询是否存在
        if let Err(e) = self.eng.get(hk.encode()) {
            if e == Errors::KeyNotFound {
                exist = false;
            }
        }

        let wb = self.eng.new_write_batch(WriteBatchOptions::default())?;
        if !exist {
            meta.size += 1;
            wb.put(Bytes::copy_from_slice(key.as_bytes()), meta.encode())?;
        }
        wb.put(hk.encode(), Bytes::copy_from_slice(value.as_bytes()))?;

        wb.commit()?;

        Ok(!exist)
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<Option<String>> {
        let meta = self.find_metadata(key, RedisDataType::Hash)?;
        if meta.size == 0 {
            return Ok(None);
        }

        // 初始化数据部分的 key
        let hk = HashInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            field: field.as_bytes().to_vec(),
        };

        let value = self.eng.get(hk.encode())?;
        Ok(Some(String::from_utf8(value.to_vec()).unwrap()))
    }

    pub fn hdel(&self, key: &str, field: &str) -> Result<bool> {
        let mut meta = self.find_metadata(key, RedisDataType::Hash)?;
        if meta.size == 0 {
            return Ok(false);
        }

        // 初始化数据部分的 key
        let hk = HashInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            field: field.as_bytes().to_vec(),
        };

        let mut exist = true;
        // 查询是否存在
        if let Err(e) = self.eng.get(hk.encode()) {
            if e == Errors::KeyNotFound {
                exist = false;
            }
        }

        if exist {
            let wb = self.eng.new_write_batch(WriteBatchOptions::default())?;
            meta.size -= 1;
            wb.put(Bytes::copy_from_slice(key.as_bytes()), meta.encode())?;
            wb.delete(hk.encode())?;
            wb.commit()?;
        }
        Ok(exist)
    }

    /// =============== Set 数据结构 ===============

    pub fn sadd(&self, key: &str, member: &str) -> Result<bool> {
        // 查找元数据
        let mut meta = self.find_metadata(key, RedisDataType::Set)?;

        // 构造数据部分的 key
        let sk = SetInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            member: member.as_bytes().to_vec(),
        };

        if let Err(e) = self.eng.get(sk.encode()) {
            // 不存在则更新
            if e == Errors::KeyNotFound {
                // 更新元数据
                let wb = self.eng.new_write_batch(WriteBatchOptions::default())?;
                meta.size += 1;
                wb.put(Bytes::copy_from_slice(key.as_bytes()), meta.encode())?;
                // 更新数据部分
                wb.put(sk.encode(), Bytes::new())?;
                wb.commit()?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn sismember(&self, key: &str, member: &str) -> Result<bool> {
        let meta = self.find_metadata(key, RedisDataType::Set)?;
        if meta.size == 0 {
            return Ok(false);
        }

        // 构造数据部分的 key
        let sk = SetInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            member: member.as_bytes().to_vec(),
        };

        match self.eng.get(sk.encode()) {
            Ok(_) => return Ok(true),
            Err(e) => {
                if e != Errors::KeyNotFound {
                    return Err(e);
                }
                return Ok(false);
            }
        }
    }

    pub fn srem(&self, key: &str, member: &str) -> Result<bool> {
        let mut meta = self.find_metadata(key, RedisDataType::Set)?;
        if meta.size == 0 {
            return Ok(false);
        }

        // 构造数据部分的 key
        let sk = SetInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            member: member.as_bytes().to_vec(),
        };

        if let Ok(_) = self.eng.get(sk.encode()) {
            // 更新元数据
            meta.size -= 1;
            let wb = self.eng.new_write_batch(WriteBatchOptions::default())?;
            wb.put(Bytes::copy_from_slice(key.as_bytes()), meta.encode())?;
            // 写数据部分
            wb.delete(sk.encode())?;
            wb.commit()?;
            return Ok(true);
        }
        Ok(false)
    }

    /// =============== List 数据结构 ===============

    pub fn lpush(&self, key: &str, element: &str) -> Result<u32> {
        self.push_inner(key, element, true)
    }

    pub fn rpush(&self, key: &str, element: &str) -> Result<u32> {
        self.push_inner(key, element, false)
    }

    pub fn lpop(&self, key: &str) -> Result<Option<String>> {
        self.pop_inner(key, true)
    }

    pub fn rpop(&self, key: &str) -> Result<Option<String>> {
        self.pop_inner(key, false)
    }

    fn push_inner(&self, key: &str, element: &str, is_left: bool) -> Result<u32> {
        // 查询元数据
        let mut meta = self.find_metadata(key, RedisDataType::List)?;

        // 构造数据部分的 key
        let lk = ListInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            index: match is_left {
                true => meta.head - 1,
                false => meta.tail,
            },
        };

        // 更新数据和元数据
        let wb = self.eng.new_write_batch(WriteBatchOptions::default())?;
        meta.size += 1;
        if is_left {
            meta.head -= 1;
        } else {
            meta.tail += 1;
        }
        wb.put(Bytes::copy_from_slice(key.as_bytes()), meta.encode())?;
        wb.put(lk.encode(), Bytes::copy_from_slice(element.as_bytes()))?;
        wb.commit()?;

        Ok(meta.size)
    }

    fn pop_inner(&self, key: &str, is_left: bool) -> Result<Option<String>> {
        // 查询元数据
        let mut meta = self.find_metadata(key, RedisDataType::List)?;
        if meta.size == 0 {
            return Ok(None);
        }

        // 构造数据部分的 key
        let lk = ListInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            index: match is_left {
                true => meta.head,
                false => meta.tail - 1,
            },
        };

        let element = self.eng.get(lk.encode())?;

        // 更新元数据
        meta.size -= 1;
        if is_left {
            meta.head += 1;
        } else {
            meta.tail -= 1;
        }
        self.eng
            .put(Bytes::copy_from_slice(key.as_bytes()), meta.encode())?;

        Ok(Some(String::from_utf8(element.to_vec()).unwrap()))
    }

    /// =============== List 数据结构 ===============

    pub fn zadd(&self, key: &str, score: f64, member: &str) -> Result<bool> {
        let mut meta = self.find_metadata(key, RedisDataType::ZSet)?;

        let mut exist = true;
        let mut old_score = 0.0;
        // 构造数据部分的 key
        let zk = ZSetInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            score,
            member: member.as_bytes().to_vec(),
        };

        match self.eng.get(zk.encode_member()) {
            Ok(val) => {
                let val = String::from_utf8(val.to_vec()).unwrap();
                old_score = val.parse().unwrap();
                if old_score == score {
                    return Ok(false);
                }
            }
            Err(e) => {
                if e != Errors::KeyNotFound {
                    return Err(e);
                }
                exist = false;
            }
        }

        let wb = self.eng.new_write_batch(WriteBatchOptions::default())?;
        // 更新元数据和数据
        if !exist {
            meta.size += 1;
            wb.put(Bytes::copy_from_slice(key.as_bytes()), meta.encode())?;
        }
        if exist {
            let old_zk = ZSetInternalKey {
                key: key.as_bytes().to_vec(),
                version: meta.version,
                score: old_score,
                member: member.as_bytes().to_vec(),
            };
            wb.delete(old_zk.encode_score())?;
        }
        wb.put(zk.encode_member(), Bytes::from(score.to_string()))?;
        wb.put(zk.encode_score(), Bytes::new())?;
        wb.commit()?;

        Ok(!exist)
    }

    pub fn zscore(&self, key: &str, member: &str) -> Result<f64> {
        let meta = self.find_metadata(key, RedisDataType::ZSet)?;
        if meta.size == 0 {
            return Ok(-1 as f64);
        }

        // 构造数据部分的 key
        let zk = ZSetInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            score: 0.0,
            member: member.as_bytes().to_vec(),
        };

        let score = self.eng.get(zk.encode_member())?;
        Ok(String::from_utf8(score.to_vec()).unwrap().parse().unwrap())
    }

    fn find_metadata(&self, key: &str, data_type: RedisDataType) -> Result<Metadata> {
        let mut exist = true;
        let mut meta = None;
        match self.eng.get(Bytes::copy_from_slice(key.as_bytes())) {
            Ok(meta_buf) => {
                // 判断类型是否匹配
                let typ = &meta_buf[0..1];
                if data_type != RedisDataType::from(typ[0]) {
                    return Err(Errors::WrongTypeOperation);
                }
                meta = Some(decode_metadata(meta_buf));
                // 判断是否过期
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                let expire = meta.as_ref().unwrap().expire;
                if expire != 0 && expire <= now {
                    exist = false;
                }
            }
            Err(e) => {
                if e != Errors::KeyNotFound {
                    return Err(e);
                }
                exist = false;
            }
        };

        if !exist {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let mut metadata = Metadata {
                data_type,
                expire: 0,
                version: now,
                size: 0,
                head: 0,
                tail: 0,
            };
            if data_type == RedisDataType::List {
                metadata.head = INITIAL_LIST_MARK;
                metadata.tail = INITIAL_LIST_MARK;
            }
            meta = Some(metadata);
        }

        Ok(meta.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use bitcask_rs::errors;

    use super::*;

    #[test]
    fn test_redis_get() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-redis-get");
        let rds = RedisDataStructure::new(opts.clone()).unwrap();

        let set_res = rds.set("key1", Duration::ZERO, "val1");
        assert!(set_res.is_ok());
        let set_res = rds.set("key2", Duration::from_secs(5), "val2");
        assert!(set_res.is_ok());

        let v1 = rds.get("key1");
        assert!(v1.is_ok());
        let v2 = rds.get("key2");
        assert!(v2.is_ok());

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_redis_del_type() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-redis-del-type");
        let rds = RedisDataStructure::new(opts.clone()).unwrap();

        let set_res = rds.set("key1", Duration::ZERO, "val1");
        assert!(set_res.is_ok());

        let type1 = rds.key_type("key1");
        assert_eq!(type1.ok().unwrap(), RedisDataType::String);

        let del_res = rds.del("key1");
        assert!(del_res.is_ok());

        let v1 = rds.get("key1");
        assert_eq!(v1.err().unwrap(), errors::Errors::KeyNotFound);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_redis_hget() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-redis-hget");
        let rds = RedisDataStructure::new(opts.clone()).unwrap();

        let res = rds.hset("myhash", "field1", "val-1");
        assert!(res.ok().unwrap());
        let res = rds.hset("myhash", "field1", "val-2");
        assert_eq!(res.ok().unwrap(), false);
        let res = rds.hset("myhash", "field2", "val-3");
        assert!(res.ok().unwrap());

        let res = rds.hget("myhash", "field1");
        assert!(res.is_ok());

        let res = rds.hget("myhash", "field2");
        assert!(res.is_ok());

        let res = rds.hget("myhash", "field-not-exist");
        assert_eq!(res.err().unwrap(), Errors::KeyNotFound);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_redis_hdel() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-redis-hdel");
        let rds = RedisDataStructure::new(opts.clone()).unwrap();

        let del = rds.hdel("myhash", "field");
        assert_eq!(del.ok().unwrap(), false);

        let res = rds.hset("myhash", "field1", "val-1");
        assert!(res.ok().unwrap());
        let res = rds.hset("myhash", "field1", "val-2");
        assert_eq!(res.ok().unwrap(), false);
        let res = rds.hset("myhash", "field2", "val-3");
        assert!(res.ok().unwrap());

        let del = rds.hdel("myhash", "field1");
        assert!(del.ok().unwrap());

        let res = rds.hget("myhash", "field1");
        assert_eq!(res.err().unwrap(), Errors::KeyNotFound);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_redis_sismember() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-redis-sismember");
        let rds = RedisDataStructure::new(opts.clone()).unwrap();

        let res = rds.sadd("myset", "val-1");
        assert!(res.ok().unwrap());
        let res = rds.sadd("myset", "val-1");
        assert_eq!(res.ok().unwrap(), false);
        let res = rds.sadd("myset", "val-2");
        assert!(res.ok().unwrap());

        let res = rds.sismember("myset-1", "val-1");
        assert_eq!(res.ok().unwrap(), false);
        let res = rds.sismember("myset", "val-1");
        assert!(res.ok().unwrap());
        let res = rds.sismember("myset", "val-2");
        assert!(res.ok().unwrap());
        let res = rds.sismember("myset", "val-not-exist");
        assert_eq!(res.ok().unwrap(), false);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_redis_srem() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-redis-srem");
        let rds = RedisDataStructure::new(opts.clone()).unwrap();

        let res = rds.sadd("myset", "val-1");
        assert!(res.ok().unwrap());
        let res = rds.sadd("myset", "val-1");
        assert_eq!(res.ok().unwrap(), false);
        let res = rds.sadd("myset", "val-2");
        assert!(res.ok().unwrap());

        let res = rds.srem("myset-1", "val-1");
        assert_eq!(res.ok().unwrap(), false);
        let res = rds.srem("myset", "val-not-exist");
        assert_eq!(res.ok().unwrap(), false);
        let res = rds.srem("myset", "val-1");
        assert!(res.ok().unwrap());

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_redis_lpop() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-redis-lpop");
        let rds = RedisDataStructure::new(opts.clone()).unwrap();

        let res = rds.lpush("mylist", "aa");
        assert_eq!(res.ok().unwrap(), 1);
        let res = rds.lpush("mylist", "bb");
        assert_eq!(res.ok().unwrap(), 2);
        let res = rds.lpush("mylist", "cc");
        assert_eq!(res.ok().unwrap(), 3);

        let res = rds.lpop("mylist");
        assert!(res.is_ok());
        let res = rds.lpop("mylist");
        assert!(res.is_ok());
        let res = rds.lpop("mylist");
        assert!(res.is_ok());
        let res = rds.lpop("mylist");
        assert!(res.is_ok());

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_redis_rpop() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-redis-rpop");
        let rds = RedisDataStructure::new(opts.clone()).unwrap();

        let res = rds.rpush("mylist", "aa");
        assert_eq!(res.ok().unwrap(), 1);
        let res = rds.rpush("mylist", "bb");
        assert_eq!(res.ok().unwrap(), 2);
        let res = rds.rpush("mylist", "cc");
        assert_eq!(res.ok().unwrap(), 3);

        let res = rds.rpop("mylist");
        assert!(res.is_ok());
        let res = rds.rpop("mylist");
        assert!(res.is_ok());
        let res = rds.rpop("mylist");
        assert!(res.is_ok());
        let res = rds.rpop("mylist");
        assert!(res.is_ok());

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_redis_zscore() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-redis-zset");
        let rds = RedisDataStructure::new(opts.clone()).unwrap();

        let res = rds.zadd("myzset", 12.11, "val-1");
        assert!(res.ok().unwrap());

        let res = rds.zadd("myzset", 33.11, "val-1");
        assert_eq!(res.ok().unwrap(), false);

        let res = rds.zadd("myzset", 33.99, "val-2");
        assert!(res.ok().unwrap());

        let res = rds.zscore("myzset", "val-1");
        assert_eq!(res.ok().unwrap(), 33.11 as f64);

        let res = rds.zscore("myzset", "val-2");
        assert_eq!(res.ok().unwrap(), 33.99 as f64);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }
}

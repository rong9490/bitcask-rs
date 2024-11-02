use bitcask_rs::errors::Result;
use bytes::{Buf, Bytes};

use crate::types::{RedisDataStructure, RedisDataType};

impl RedisDataStructure {
    pub fn del(&self, key: &str) -> Result<()> {
        self.eng.delete(Bytes::copy_from_slice(key.as_bytes()))
    }

    pub fn key_type(&self, key: &str) -> Result<RedisDataType> {
        let mut buf = self.eng.get(Bytes::copy_from_slice(key.as_bytes()))?;
        Ok(RedisDataType::from(buf.get_u8()))
    }
}

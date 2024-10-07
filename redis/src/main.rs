use std::{collections::HashMap, sync::Mutex, time::Duration};

use bitcask_rs::options::Options;
use redis::types::RedisDataStructure;

const SERVER_ADDR: &str = "127.0.0.1:6380";

type CmdHandler = dyn Fn(&mut redcon::Conn, Vec<Vec<u8>>, &Mutex<RedisDataStructure>);

fn main() {
    // 打开 Redis 数据结构服务
    let rds = Mutex::new(RedisDataStructure::new(Options::default()).unwrap());

    // 启动 Redis server 服务
    let mut bitcask_server = redcon::listen(SERVER_ADDR, rds).unwrap();
    bitcask_server.command = Some(|conn, rds, args| {
        // 存储支持的命令列表
        let mut supported_commands = HashMap::new();
        supported_commands.insert("set", Box::new(set) as Box<CmdHandler>);
        supported_commands.insert("get", Box::new(get) as Box<CmdHandler>);
        supported_commands.insert("hset", Box::new(hset) as Box<CmdHandler>);
        supported_commands.insert("sadd", Box::new(sadd) as Box<CmdHandler>);
        supported_commands.insert("lpush", Box::new(lpush) as Box<CmdHandler>);
        supported_commands.insert("zadd", Box::new(zadd) as Box<CmdHandler>);

        let name: String = String::from_utf8_lossy(&args[0]).to_lowercase();
        match supported_commands.get(name.as_str()) {
            Some(handler) => handler(conn, args, rds),
            None => conn.write_error("Err unknow command"),
        }
    });

    println!("bitcask serving at {}", bitcask_server.local_addr());
    bitcask_server.serve().unwrap();
}

fn set(conn: &mut redcon::Conn, args: Vec<Vec<u8>>, rds: &Mutex<RedisDataStructure>) {
    if args.len() != 3 {
        conn.write_error("Err wrong number of arguments");
        return;
    }

    let redis_data_structure = rds.lock().unwrap();
    let res = redis_data_structure.set(
        &String::from_utf8_lossy(&args[1]),
        Duration::ZERO,
        &String::from_utf8_lossy(&args[2]),
    );
    if res.is_err() {
        conn.write_error(&res.err().unwrap().to_string());
        return;
    }
    conn.write_string("OK");
}

fn get(conn: &mut redcon::Conn, args: Vec<Vec<u8>>, rds: &Mutex<RedisDataStructure>) {
    if args.len() != 2 {
        conn.write_error("Err wrong number of arguments");
        return;
    }

    let redis_data_structure = rds.lock().unwrap();
    match redis_data_structure.get(&String::from_utf8_lossy(&args[1])) {
        Ok(val) => conn.write_string(val.unwrap().as_str()),
        Err(e) => conn.write_error(e.to_string().as_str()),
    }
}

fn hset(conn: &mut redcon::Conn, args: Vec<Vec<u8>>, rds: &Mutex<RedisDataStructure>) {
    if args.len() != 4 {
        conn.write_error("Err wrong number of arguments");
        return;
    }

    let redis_data_structure = rds.lock().unwrap();

    let key = String::from_utf8_lossy(&args[1]);
    let field = String::from_utf8_lossy(&args[2]);
    let value = String::from_utf8_lossy(&args[3]);
    match redis_data_structure.hset(&key, &field, &value) {
        Ok(val) => conn.write_integer(val as i64),
        Err(e) => conn.write_error(e.to_string().as_str()),
    }
}

fn sadd(conn: &mut redcon::Conn, args: Vec<Vec<u8>>, rds: &Mutex<RedisDataStructure>) {
    if args.len() != 3 {
        conn.write_error("Err wrong number of arguments");
        return;
    }

    let redis_data_structure = rds.lock().unwrap();

    let key = String::from_utf8_lossy(&args[1]);
    let member = String::from_utf8_lossy(&args[2]);
    match redis_data_structure.sadd(&key, &member) {
        Ok(val) => conn.write_integer(val as i64),
        Err(e) => conn.write_error(e.to_string().as_str()),
    }
}

fn lpush(conn: &mut redcon::Conn, args: Vec<Vec<u8>>, rds: &Mutex<RedisDataStructure>) {
    if args.len() != 3 {
        conn.write_error("Err wrong number of arguments");
        return;
    }

    let redis_data_structure = rds.lock().unwrap();
    let key = String::from_utf8_lossy(&args[1]);
    let value = String::from_utf8_lossy(&args[2]);
    match redis_data_structure.lpush(&key, &value) {
        Ok(val) => conn.write_integer(val as i64),
        Err(e) => conn.write_error(e.to_string().as_str()),
    }
}

fn zadd(conn: &mut redcon::Conn, args: Vec<Vec<u8>>, rds: &Mutex<RedisDataStructure>) {
    if args.len() != 4 {
        conn.write_error("Err wrong number of arguments");
        return;
    }

    let redis_data_structure = rds.lock().unwrap();
    let key = String::from_utf8_lossy(&args[1]);
    let score = String::from_utf8_lossy(&args[2]);
    let member = String::from_utf8_lossy(&args[3]);
    match redis_data_structure.zadd(&key, score.parse().unwrap(), &member) {
        Ok(val) => conn.write_integer(val as i64),
        Err(e) => conn.write_error(e.to_string().as_str()),
    }
}

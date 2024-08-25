// Uncomment this block to pass the first stage
use chrono::Utc;
use rand::Rng;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::{collections::HashMap, error::Error, str, sync::Arc};
use std::{env, mem};
use tokio::net::TcpStream;
use tokio::sync::broadcast::{self, Sender};
use tokio::sync::RwLock;
use tokio::time::{interval, sleep, Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Debug, Clone)]
struct Command {
    parts: Vec<String>,
    bytes: usize,
}

trait ToRedis {
    fn to_redis_bulk_string(self) -> String;
    fn to_resp_array(self) -> String;
    fn to_command_list(self) -> Result<Vec<Command>, Box<dyn Error + Send + Sync>>;
}

impl<T: AsRef<str>> ToRedis for T {
    fn to_redis_bulk_string(self) -> String {
        let length = self.as_ref().len();
        format!("${}\r\n{}\r\n", length, self.as_ref())
    }
    fn to_resp_array(self) -> String {
        let vec: Vec<&str> = self.as_ref().split(" ").collect();
        let mut resp_string = format!("*{}\r\n", vec.len());
        for element in vec {
            resp_string.push_str(element.to_redis_bulk_string().as_str());
        }
        resp_string
    }
    fn to_command_list(self) -> Result<Vec<Command>, Box<dyn Error + Send + Sync>> {
        let mut lines = self.as_ref().lines().peekable();
        let mut commands = Vec::new();

        while lines.peek().is_some() {
            let mut item_num_line = lines.next().ok_or("message is empty")?;
            if !item_num_line.starts_with("*") {
                if let Some(star_index) = item_num_line.find('*') {
                    item_num_line = &item_num_line[star_index..];
                } else {
                    continue;
                }
            }
            let mut bytes = item_num_line.len() + 2;

            let item_nums: usize = item_num_line[1..].parse()?;

            let mut command = Vec::with_capacity(item_nums);
            for _ in 0..item_nums {
                let bulk_len_line = lines.next().ok_or("bulk length line is empty")?;
                bytes += bulk_len_line.len() + 2;
                if !bulk_len_line.starts_with('$') {
                    return Err("expected bulk string format".into());
                }
                let _bulk_len: usize = bulk_len_line[1..].parse()?;

                let bulk_str = lines.next().ok_or("bulk string line is empty")?;
                bytes += bulk_str.len() + 2;
                command.push(bulk_str.to_lowercase());
            }
            let com = Command {
                parts: command,
                bytes,
            };
            commands.push(com);
        }

        Ok(commands)
    }
}

type ThreadSafe<T> = Arc<RwLock<T>>;

#[derive(Debug, Clone)]
struct Config {
    dir: Option<String>,
    db_filename: Option<String>,
    port: u16,
}

unsafe impl Send for Config {}
unsafe impl Sync for Config {}

#[derive(Debug, Clone)]
struct Redis {
    kv: ThreadSafe<HashMap<String, String>>,
    expire: ThreadSafe<HashMap<String, i64>>,
    master_address: Option<String>,
    replicas: ThreadSafe<Vec<ThreadSafe<TcpStream>>>,
    tx: Sender<String>,
    offset: Arc<AtomicUsize>,
    write_pending: Arc<AtomicBool>,
    config: Config,
    in_transaction: Arc<AtomicBool>,
    commands: ThreadSafe<Vec<Command>>,
}

unsafe impl Send for Redis {}
unsafe impl Sync for Redis {}

impl Redis {
    pub fn new() -> Self {
        let args: Vec<String> = env::args().collect();

        let port = args
            .iter()
            .position(|s| s == "--port")
            .and_then(|port_index| args.get(port_index + 1))
            .map(|port_str| port_str.parse::<u16>())
            .unwrap_or_else(|| Ok(6379_u16))
            .expect("port must be a valid u16 number");

        let master_address =
            args.iter()
                .position(|s| s == "--replicaof")
                .and_then(|replica_of_index| {
                    args.get(replica_of_index + 1)
                        .map(|s| s.split_whitespace().collect::<Vec<&str>>())
                        .filter(|replica| replica.len() == 2)
                        .map(|replica| format!("{}:{}", replica[0], replica[1]))
                });

        let dir = args
            .iter()
            .position(|s| s == "--dir")
            .and_then(|dir_index| args.get(dir_index + 1).map(|s| s.to_owned()));

        let db_filename = args
            .iter()
            .position(|s| s == "--dbfilename")
            .and_then(|dir_index| args.get(dir_index + 1).map(|s| s.to_owned()));

        let replicas = Arc::new(RwLock::new(vec![]));
        let (tx, _) = broadcast::channel(100);

        Redis {
            kv: Arc::new(RwLock::new(HashMap::new())),
            expire: Arc::new(RwLock::new(HashMap::new())),
            master_address,
            replicas,
            tx,
            offset: Arc::new(AtomicUsize::new(0)),
            write_pending: Arc::new(AtomicBool::new(false)),
            config: Config {
                port,
                dir,
                db_filename,
            },
            in_transaction: Arc::new(AtomicBool::new(false)),
            commands: Arc::new(RwLock::new(vec![])),
        }
    }
    pub async fn init(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let is_slave = self.master_address.is_some();
        if is_slave {
            let self_clone = self.clone();
            tokio::spawn(self_clone.run_salve());
        }
        self.run().await?;
        Ok(())
    }

    async fn run_salve(self) {
        let _err = self.salve().await;
    }

    async fn salve(self) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let address = self.master_address.unwrap();
        let mut socket = TcpStream::connect(address).await?;
        socket.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
        let mut buf = [0; 1024];
        let n = socket.read(&mut buf).await?;
        let response = String::from_utf8_lossy(&buf[..n]);
        if response.trim() != "+PONG" {
            panic!("wanted pong got:{response}");
        }

        socket
            .write_all(
                format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n",
                    self.config.port
                )
                .as_bytes(),
            )
            .await?;
        let n = socket.read(&mut buf).await?;
        let response = String::from_utf8_lossy(&buf[..n]);
        if response.trim() != "+OK" {
            panic!("wanted OK got:{response}");
        }
        socket
            .write_all(
                "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
                    .to_string()
                    .as_bytes(),
            )
            .await?;
        let n = socket.read(&mut buf).await?;
        let response = String::from_utf8_lossy(&buf[..n]);
        if response.trim() != "+OK" {
            panic!("wanted OK got:{response}");
        }
        socket
            .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
            .await?;

        let mut offset: usize = 0;
        loop {
            let n = socket.read(&mut buf).await?;
            if n == 0 {
                break;
            } else {
                let Ok(commands) = String::from_utf8_lossy(&buf[..n]).to_command_list() else {
                    continue;
                };
                let replconf_command = commands
                    .iter()
                    .find(|c| c.parts.contains(&String::from("replconf")));
                let commands = commands
                    .iter()
                    .filter(|c| !c.parts.contains(&String::from("replconf")));
                for command in commands {
                    if command.parts.contains(&String::from("ping")) {
                        offset += command.bytes;
                    }
                    if command.parts.contains(&String::from("set")) {
                        let default = String::from("");
                        let Some(key) = command.parts.get(1) else {
                            continue;
                        };
                        let value = command.parts.get(2).unwrap_or(&default);
                        let k = key.to_string();
                        if let Some(px_index) =
                            command.parts.iter().position(|s| s.to_lowercase() == "px")
                        {
                            let ttl = command.parts.get(px_index + 1).and_then(|ttl| {
                                let Ok(_ttl) = ttl.parse::<i64>() else {
                                    return None;
                                };
                                Some(_ttl)
                            });
                            if ttl.is_none() {
                                continue;
                            }
                            let ttl = ttl.unwrap() + Utc::now().timestamp_millis();
                            self.expire.write().await.insert(k.clone(), ttl);
                        }

                        let mut map = self.kv.write().await;
                        offset += command.bytes;
                        map.insert(k, value.to_owned());
                        drop(map);
                    }
                }

                if replconf_command.is_some() {
                    let command = replconf_command.unwrap();
                    let replay = format!("REPLCONF ACK {}", offset).to_resp_array();
                    socket.write_all(replay.as_bytes()).await?;
                    print!("replconf offset: {offset}+{n}=");
                    offset += command.bytes;
                }
            }
        }
        Ok(offset)
    }

    async fn run(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.config.port)).await?;
        let self_clone = self.clone();
        tokio::spawn(async move {
            let _gc = self_clone.gc().await;
        });
        let self_clone = self.clone();
        tokio::spawn(async move {
            let _p = self_clone.propagate().await;
        });
        loop {
            let (stream, _) = listener.accept().await?;

            let mut cloned_self = self.clone();

            tokio::spawn(async move {
                let _ = cloned_self
                    .handel_stream(Arc::new(RwLock::new(stream))) // Use the cloned_tx value
                    .await;
            });
        }
    }
    async fn handel_stream(
        &mut self,
        stream: Arc<RwLock<TcpStream>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut buf = [0; 1020];
        loop {
            let read_count = stream.write().await.read(&mut buf).await?;
            if read_count == 0 {
                return Ok(());
            }
            let commands = String::from_utf8_lossy(&buf[..read_count]).to_command_list()?;
            let is_master = self.master_address.is_none();
            self.process_commands(commands, is_master, &stream).await?;
        }
    }

    async fn process_commands(
        &mut self,
        commands: Vec<Command>,
        is_master: bool,
        stream: &Arc<RwLock<TcpStream>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _ = Box::pin(async move {
            let _ = inner(self, commands, is_master, stream).await;
        })
        .await;

        async fn inner(
            _self: &mut Redis,
            commands: Vec<Command>,
            is_master: bool,
            stream: &Arc<RwLock<TcpStream>>,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            for mut command in commands {
                if command.parts.contains(&String::from("info")) {
                    let role = if is_master { "master" } else { "slave" };
                    let mut response = format!("role:{}", role).to_redis_bulk_string();
                    response =
                        format!("{response}master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
                            .to_redis_bulk_string();

                    response = format!("{response}master_repl_offset:0")
                        .as_str()
                        .to_redis_bulk_string();
                    stream.write().await.write_all(response.as_bytes()).await?;
                }
                if command.parts.contains(&String::from("ping")) {
                    if _self.in_transaction.load(Ordering::SeqCst) {
                        let mut commands = _self.commands.write().await;
                        commands.push(command);
                        stream.write().await.write_all(b"+QUEUED\r\n").await?;
                        continue;
                    }
                    stream.write().await.write_all(b"+PONG\r\n").await?;
                }
                if command.parts.contains(&String::from("echo")) {
                    let default = String::from("");
                    let message = command.parts.get(1).unwrap_or(&default);
                    if _self.in_transaction.load(Ordering::SeqCst) {
                        let mut commands = _self.commands.write().await;
                        commands.push(command);
                        stream.write().await.write_all(b"+QUEUED\r\n").await?;
                        continue;
                    }
                    stream
                        .write()
                        .await
                        .write_all(format!("+{}\r\n", message).as_bytes())
                        .await?;
                }
                if command.parts.contains(&String::from("set")) {
                    if _self.in_transaction.load(Ordering::SeqCst) {
                        let mut commands = _self.commands.write().await;
                        commands.push(command);
                        stream.write().await.write_all(b"+QUEUED\r\n").await?;
                        continue;
                    }
                    let default = String::from("");
                    let Some(key) = command.parts.get(1) else {
                        stream
                            .write()
                            .await
                            .write_all(b"-ERR no key provided\r\n")
                            .await?;
                        continue;
                    };
                    let value = command.parts.get(2).unwrap_or(&default);
                    let k = key.to_string();
                    if let Some(px_index) =
                        command.parts.iter().position(|s| s.to_lowercase() == "px")
                    {
                        let ttl = command.parts.get(px_index + 1).and_then(|ttl| {
                            let Ok(_ttl) = ttl.parse::<i64>() else {
                                return None;
                            };
                            Some(_ttl)
                        });
                        if ttl.is_none() {
                            stream
                                .write()
                                .await
                                .write_all(b"-no ttl must of time number\r\n")
                                .await?;
                            continue;
                        }

                        let ttl = ttl.unwrap() + Utc::now().timestamp_millis();
                        _self.expire.write().await.insert(k.clone(), ttl);
                    }
                    let mut map = _self.kv.write().await;
                    map.insert(k, value.to_owned());
                    drop(map);
                    let message = command.parts[..3].join(" ").to_resp_array();
                    stream.write().await.write_all(b"+OK\r\n").await?;
                    _self.offset.fetch_add(command.bytes, Ordering::SeqCst);
                    if is_master && !_self.replicas.read().await.is_empty() {
                        _self.write_pending.store(true, Ordering::SeqCst);
                        let _set = _self.tx.send(message);
                    }
                }
                if command.parts.contains(&String::from("incr")) {
                    if _self.in_transaction.load(Ordering::SeqCst) {
                        let mut commands = _self.commands.write().await;
                        commands.push(command);
                        stream.write().await.write_all(b"+QUEUED\r\n").await?;
                        continue;
                    }
                    let Some(key) = command.parts.get(1) else {
                        stream
                            .write()
                            .await
                            .write_all(b"-ERR no key provided\r\n")
                            .await?;
                        continue;
                    };
                    let k = key.to_string();
                    let mut map = _self.kv.write().await;
                    let Some(value) = map.get(&k) else {
                        map.insert(k, "1".to_string());
                        drop(map);
                        stream.write().await.write_all(b":1\r\n").await?;
                        continue;
                    };
                    let Ok(value) = value.parse::<i64>() else {
                        stream
                            .write()
                            .await
                            .write_all(b"-ERR value is not an integer or out of range\r\n")
                            .await?;
                        continue;
                    };
                    let value = value + 1;
                    map.insert(k, value.to_string());
                    drop(map);
                    stream
                        .write()
                        .await
                        .write_all(format!(":{}\r\n", value).as_bytes())
                        .await?;
                }
                if command.parts.contains(&String::from("multi")) {
                    _self.in_transaction.store(true, Ordering::SeqCst);
                    stream.write().await.write_all(b"+OK\r\n").await?;
                }
                if command.parts.contains(&String::from("exec")) {
                    if !_self.in_transaction.swap(false, Ordering::SeqCst) {
                        stream
                            .write()
                            .await
                            .write_all(b"-ERR EXEC without MULTI\r\n")
                            .await?;
                        continue;
                    }
                    let commands: Vec<Command> = mem::take(_self.commands.write().await.as_mut());
                    if commands.is_empty() {
                        stream.write().await.write_all(b"*0\r\n").await?;
                        continue;
                    }
                    _self.process_commands(commands, is_master, stream).await?;
                }
                if !command.parts.contains(&String::from("config"))
                    && command.parts.contains(&String::from("get"))
                {
                    if _self.in_transaction.load(Ordering::SeqCst) {
                        let mut commands = _self.commands.write().await;
                        commands.push(command);
                        stream.write().await.write_all(b"+QUEUED\r\n").await?;
                        continue;
                    }
                    let Some(key) = command.parts.get(1) else {
                        stream
                            .write()
                            .await
                            .write_all(b"-ERR no key provided\r\n")
                            .await?;
                        continue;
                    };
                    let map = _self.kv.read().await;
                    let Some(value) = map.get(key) else {
                        stream.write().await.write_all(b"$-1\r\n").await?;
                        continue;
                    };
                    let expire = _self.expire.read().await;
                    let ttl = expire.get(key).and_then(|&ttl| {
                        if Utc::now().timestamp_millis() >= ttl {
                            Some(())
                        } else {
                            None
                        }
                    });
                    drop(expire);
                    let value = value.clone();
                    drop(map);
                    if ttl.is_some() {
                        stream.write().await.write_all(b"$-1\r\n").await?;
                        _self.kv.write().await.remove(key);
                        continue;
                    }

                    stream
                        .write()
                        .await
                        .write_all(format!("+{}\r\n", value).as_bytes())
                        .await?;
                }
                if command.parts.contains(&String::from("replconf")) {
                    if command.parts.contains(&String::from("listening-port")) {
                        let Some(_) = command.parts.get(2) else {
                            stream
                                .write()
                                .await
                                .write_all(b"-No port provided\r\n")
                                .await?;
                            continue;
                        };
                    }
                    stream.write().await.write_all(b"+OK\r\n").await?;
                }
                if command.parts.contains(&String::from("wait")) {
                    let stream = stream.clone();
                    let Some(min_replica_count) = command.parts.get(1) else {
                        stream
                            .write()
                            .await
                            .write_all(b"-ERR no replica count\r\n")
                            .await?;
                        continue;
                    };
                    let Ok(min_replica_count) = min_replica_count.parse::<i64>() else {
                        stream
                            .write()
                            .await
                            .write_all(b"-ERR count should be u64\r\n")
                            .await?;
                        continue;
                    };
                    let Some(timeout) = command.parts.get(2) else {
                        stream
                            .write()
                            .await
                            .write_all(b"-ERR no timeout\r\n")
                            .await?;
                        continue;
                    };
                    let Ok(timeout) = timeout.parse::<i64>() else {
                        stream
                            .write()
                            .await
                            .write_all(b"-ERR timeout should be u64\r\n")
                            .await?;
                        continue;
                    };
                    let _res = _self.wait(stream, min_replica_count, timeout).await;
                }
                if command.parts.contains(&String::from("config")) {
                    if command.parts.contains(&String::from("get"))
                        && command.parts.contains(&String::from("dir"))
                    {
                        let Some(dir_replay) = _self.config.dir.clone() else {
                            stream
                                .write()
                                .await
                                .write_all(b"-ERR no dir was provided \r\n")
                                .await?;
                            continue;
                        };
                        let dir_replay = format!("dir {dir_replay}").to_resp_array();
                        stream
                            .write()
                            .await
                            .write_all(dir_replay.as_bytes())
                            .await?;
                    }

                    if command.parts.contains(&String::from("get"))
                        && command.parts.contains(&String::from("dbfilename"))
                    {
                        let Some(db_filename) = _self.config.db_filename.clone() else {
                            stream
                                .write()
                                .await
                                .write_all(b"-ERR no dir was provided \r\n")
                                .await?;
                            continue;
                        };
                        let dir_replay = format!("dbfilename {db_filename}").to_resp_array();
                        stream
                            .write()
                            .await
                            .write_all(dir_replay.as_bytes())
                            .await?;
                    }
                    continue;
                }
                if command.parts.contains(&String::from("psync")) {
                    let Some(replication_id) = command.parts.get_mut(1) else {
                        stream
                            .write()
                            .await
                            .write_all(b"-ERR no replication id provided")
                            .await?;
                        continue;
                    };
                    if replication_id == "?" {
                        replication_id.clear();
                        let id = generate_random_id(16);
                        replication_id.push_str(id.to_string().as_str());
                    }
                    stream
                        .write()
                        .await
                        .write_all(
                            format!("FULLRESYNC {} 0", replication_id)
                                .to_redis_bulk_string()
                                .as_bytes(),
                        )
                        .await?;
                    let empty_file_payload = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")?;
                    stream
                        .write()
                        .await
                        .write_all(format!("${}\r\n", empty_file_payload.len()).as_bytes())
                        .await?;
                    stream
                        .write()
                        .await
                        .write_all(empty_file_payload.as_slice())
                        .await?;
                    _self.replicas.write().await.push(stream.clone());
                    return Ok(());
                }
            }
            Ok(())
        }
        Ok(())
    }
    async fn wait(
        &self,
        stream: ThreadSafe<TcpStream>,
        min_replica_count: i64,
        timeout: i64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let write_pending = self.write_pending.load(Ordering::SeqCst);

        if !write_pending {
            stream
                .write()
                .await
                .write_all(format!(":{}\r\n", self.replicas.read().await.len()).as_bytes())
                .await?;
            return Ok(());
        }

        let deadline = Utc::now().timestamp_millis() + timeout;
        let propagation_count = Arc::new(AtomicUsize::new(0));

        for replica in self.replicas.read().await.iter() {
            let ack = "REPLCONF GETACK *".to_resp_array();
            replica.write().await.write_all(ack.as_bytes()).await?;
        }

        loop {
            let time_remaining = deadline - Utc::now().timestamp_millis();
            let pcg = propagation_count.load(Ordering::SeqCst);

            if time_remaining <= 0 || pcg == self.replicas.read().await.len() {
                stream
                    .write()
                    .await
                    .write_all(format!(":{pcg}\r\n").as_bytes())
                    .await?;
                self.write_pending.store(false, Ordering::SeqCst);
                return Ok(());
            }

            for replica in self.replicas.read().await.iter() {
                let pcg = propagation_count.load(Ordering::SeqCst);

                if pcg >= min_replica_count as usize {
                    break;
                }

                let stream = stream.clone();
                let propagation_count = propagation_count.clone();
                let self_clone = self.clone();
                let replica = replica.clone();

                tokio::spawn(async move {
                    if let Ok(rep) = replica.try_write() {
                        let mut buf = [0; 1020];
                        if let Ok(count) = rep.try_read(&mut buf) {
                            let reply = String::from_utf8_lossy(&buf[..count]).to_command_list();
                            if let Ok(reply) = reply {
                                if !reply.is_empty() {
                                    if let Ok(offset) = reply[0].parts[2].parse::<usize>() {
                                        let current_offset =
                                            self_clone.offset.load(Ordering::SeqCst);
                                        if current_offset <= offset {
                                            propagation_count.fetch_add(1, Ordering::SeqCst);
                                        }
                                    } else {
                                        stream
                                            .write()
                                            .await
                                            .write_all(b"-ERR offset not of type u64\r\n")
                                            .await
                                            .unwrap();
                                    }
                                }
                            } else {
                                stream
                                    .write()
                                    .await
                                    .write_all(b"-ERR ack failed\r\n")
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                });

                sleep(Duration::from_millis(25)).await;
            }

            let pcg = propagation_count.load(Ordering::SeqCst);
            if pcg >= min_replica_count as usize {
                stream
                    .write()
                    .await
                    .write_all(format!(":{pcg}\r\n").as_bytes())
                    .await?;
                self.write_pending.store(false, Ordering::SeqCst);
                return Ok(());
            }

            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn propagate(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut rx = self.tx.subscribe();

        while let Ok(received) = rx.recv().await {
            let replicas = self.replicas.read().await;

            for replica in replicas.iter() {
                if let Ok(mut stream) = replica.try_write() {
                    if let Err(e) = stream.write_all(received.as_bytes()).await {
                        eprintln!("Failed to write to replica: {}", e);
                    }
                } else {
                    eprintln!("Failed to acquire write lock for a replica");
                }
            }
        }

        Ok(())
    }
    async fn gc(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut interval = interval(Duration::from_secs(60));
        loop {
            interval.tick().await;

            let keys_to_remove = {
                let map = self.kv.read().await;
                let expire = self.expire.read().await;
                map.iter()
                    .filter_map(|(key, _)| {
                        expire.get(key).and_then(|&ttl| {
                            if Utc::now().timestamp_millis() >= ttl {
                                Some(key.clone())
                            } else {
                                None
                            }
                        })
                    })
                    .collect::<Vec<_>>()
            };

            if !keys_to_remove.is_empty() {
                let mut map = self.kv.write().await;
                for key in keys_to_remove {
                    map.remove(&key);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let redis = Redis::new();
    let err = redis.init().await;
    println!("main {err:?}");
    Ok(())
}

fn generate_random_id(length: usize) -> String {
    let mut rng = rand::thread_rng();
    let id: String = (0..length)
        .map(|_| {
            let idx = rng.gen_range(0..52);

            if idx < 26 {
                (b'a' + idx as u8) as char
            } else {
                (b'A' + (idx - 26) as u8) as char
            }
        })
        .collect();
    id
}

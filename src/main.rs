use chrono::Utc;
use rand::Rng;
use std::{
    collections::HashMap, env, error::Error, sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    }
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::broadcast::{self, Sender},
    sync::RwLock,
    time::{interval, sleep, Duration},
};
use futures::future::BoxFuture;
use futures::FutureExt;

/// A parsed command from a Redis-style protocol.
#[derive(Debug, Clone)]
struct Command {
    parts: Vec<String>,
    bytes: usize,
}

/// Helper trait for formatting responses and parsing incoming data.
trait ToRedis {
    fn to_redis_bulk_string(&self) -> String;
    fn to_resp_array(&self) -> String;
    fn to_command_list(&self) -> Result<Vec<Command>, Box<dyn Error + Send + Sync>>;
}

impl<T: AsRef<str>> ToRedis for T {
    fn to_redis_bulk_string(&self) -> String {
        let content = self.as_ref();
        format!("${}\r\n{}\r\n", content.len(), content)
    }

    fn to_resp_array(&self) -> String {
        let parts: Vec<&str> = self.as_ref().split_whitespace().collect();
        let mut resp = format!("*{}\r\n", parts.len());
        for part in parts {
            resp.push_str(&part.to_redis_bulk_string());
        }
        resp
    }

    fn to_command_list(&self) -> Result<Vec<Command>, Box<dyn Error + Send + Sync>> {
        let mut lines = self.as_ref().lines().peekable();
        let mut commands = Vec::new();

        while let Some(mut line) = lines.next() {
            if !line.starts_with('*') {
                if let Some(star_index) = line.find('*') {
                    line = &line[star_index..];
                } else {
                    continue;
                }
            }
            let mut total_bytes = line.len() + 2;
            let item_count: usize = line[1..].trim().parse()?;
            let mut parts = Vec::with_capacity(item_count);
            for _ in 0..item_count {
                let bulk_len_line = lines.next().ok_or("expected bulk length line")?;
                total_bytes += bulk_len_line.len() + 2;
                if !bulk_len_line.starts_with('$') {
                    return Err("expected bulk string format".into());
                }
                let _bulk_len: usize = bulk_len_line[1..].trim().parse()?;
                let bulk_str = lines.next().ok_or("expected bulk string line")?;
                total_bytes += bulk_str.len() + 2;
                parts.push(bulk_str.to_lowercase());
            }
            commands.push(Command {
                parts,
                bytes: total_bytes,
            });
        }
        Ok(commands)
    }
}

/// Configuration options provided on the command line.
#[derive(Debug, Clone)]
struct Config {
    dir: Option<String>,
    db_filename: Option<String>,
    port: u16,
}

/// The main Redis-like server state.
#[derive(Debug, Clone)]
struct Redis {
    kv: Arc<RwLock<HashMap<String, String>>>,
    expire: Arc<RwLock<HashMap<String, i64>>>,
    master_address: Option<String>,
    replicas: Arc<RwLock<Vec<Arc<RwLock<TcpStream>>>>>,
    tx: Sender<String>,
    offset: Arc<AtomicUsize>,
    write_pending: Arc<AtomicBool>,
    config: Config,
    in_transaction: Arc<AtomicBool>,
    commands: Arc<RwLock<Vec<Command>>>,
}

impl Redis {
    pub fn new() -> Self {
        let args: Vec<String> = env::args().collect();

        // Parse port argument; default to 6379.
        let port = args
            .iter()
            .position(|s| s == "--port")
            .and_then(|i| args.get(i + 1))
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(6379);

        // Parse master (replica) address.
        let master_address = args
            .iter()
            .position(|s| s == "--replicaof")
            .and_then(|i| args.get(i + 1))
            .map(|s| {
                let parts: Vec<&str> = s.split_whitespace().collect();
                if parts.len() == 2 {
                    format!("{}:{}", parts[0], parts[1])
                } else {
                    s.to_string()
                }
            });

        // Parse configuration options.
        let dir = args
            .iter()
            .position(|s| s == "--dir")
            .and_then(|i| args.get(i + 1))
            .cloned();
        let db_filename = args
            .iter()
            .position(|s| s == "--dbfilename")
            .and_then(|i| args.get(i + 1))
            .cloned();

        let replicas = Arc::new(RwLock::new(Vec::new()));
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
            commands: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Initialize the server. If a master address is provided, start as a slave.
    pub async fn init(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if self.master_address.is_some() {
            let slave = self.clone();
            tokio::spawn(async move {
                if let Err(e) = slave.run_slave().await {
                    eprintln!("Slave error: {}", e);
                }
            });
        }
        self.run().await?;
        Ok(())
    }

    /// Run as a slave. Connect to the master and start the replication protocol.
    async fn run_slave(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let address = self.master_address.clone().unwrap();
        let mut socket = TcpStream::connect(address).await?;
        socket.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
        let mut buf = [0; 1024];
        let n = socket.read(&mut buf).await?;
        let response = String::from_utf8_lossy(&buf[..n]);
        if response.trim() != "+PONG" {
            return Err(format!("Expected PONG, got: {}", response).into());
        }

        let replconf_cmd = format!(
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n",
            self.config.port
        );
        socket.write_all(replconf_cmd.as_bytes()).await?;
        let n = socket.read(&mut buf).await?;
        let response = String::from_utf8_lossy(&buf[..n]);
        if response.trim() != "+OK" {
            return Err(format!("Expected +OK, got: {}", response).into());
        }

        let replconf_capa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        socket.write_all(replconf_capa.as_bytes()).await?;
        let n = socket.read(&mut buf).await?;
        let response = String::from_utf8_lossy(&buf[..n]);
        if response.trim() != "+OK" {
            return Err(format!("Expected +OK, got: {}", response).into());
        }

        socket
            .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
            .await?;

        let mut offset: usize = 0;
        loop {
            let n = socket.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let data = String::from_utf8_lossy(&buf[..n]);
            let commands = match data.to_command_list() {
                Ok(cmds) => cmds,
                Err(_) => continue,
            };

            let replconf_command = commands
                .iter()
                .find(|c| c.parts.contains(&"replconf".to_string()));
            let non_repl_commands: Vec<_> = commands
                .iter()
                .filter(|c| !c.parts.contains(&"replconf".to_string()))
                .collect();

            for command in non_repl_commands {
                if command.parts.contains(&"ping".to_string()) {
                    offset += command.bytes;
                }
                if command.parts.contains(&"set".to_string()) {
                    if let Some(key) = command.parts.get(1) {
                        let value = command.parts.get(2).cloned().unwrap_or_default();
                        if let Some(px_index) =
                            command.parts.iter().position(|s| s == "px")
                        {
                            if let Some(ttl_str) = command.parts.get(px_index + 1) {
                                if let Ok(ttl) = ttl_str.parse::<i64>() {
                                    let expire_time = ttl + Utc::now().timestamp_millis();
                                    self.expire.write().await.insert(key.clone(), expire_time);
                                }
                            }
                        }
                        self.kv.write().await.insert(key.clone(), value);
                        offset += command.bytes;
                    }
                }
            }

            if let Some(cmd) = replconf_command {
                let ack = format!("REPLCONF ACK {}", offset).to_resp_array();
                socket.write_all(ack.as_bytes()).await?;
                offset += cmd.bytes;
            }
        }
        Ok(())
    }

    /// Run the main server loop: accept connections, spawn tasks, start GC and propagation.
    async fn run(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let listener =
            TcpListener::bind(format!("127.0.0.1:{}", self.config.port)).await?;
        let redis_clone = self.clone();
        tokio::spawn(async move {
            if let Err(e) = redis_clone.gc().await {
                eprintln!("GC error: {}", e);
            }
        });
        let redis_clone = self.clone();
        tokio::spawn(async move {
            if let Err(e) = redis_clone.propagate().await {
                eprintln!("Propagation error: {}", e);
            }
        });

        loop {
            let (stream, _) = listener.accept().await?;
            let redis = self.clone();
            let stream = Arc::new(RwLock::new(stream));
            tokio::spawn(async move {
                if let Err(e) = redis.handle_stream(stream).await {
                    eprintln!("Stream error: {}", e);
                }
            });
        }
    }

    /// Handle a client connection by reading data and processing commands.
    async fn handle_stream(
        &self,
        stream: Arc<RwLock<TcpStream>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut buf = [0; 1024];
        loop {
            let n = stream.write().await.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let data = String::from_utf8_lossy(&buf[..n]);
            let commands = data.to_command_list()?;
            let is_master = self.master_address.is_none();
            self.process_commands(commands, is_master, &stream)
                .await?;
        }
        Ok(())
    }

    /// Process each command by matching on its first argument.
    fn process_commands<'a>(
        &'a self,
        commands: Vec<Command>,
        is_master: bool,
        stream: &'a Arc<RwLock<TcpStream>>,
    ) -> BoxFuture<'a, Result<(), Box<dyn Error + Send + Sync>>> {
        async move {
            for command in commands {
                // Example handling for a few commands
                let cmd = command.parts.get(0).map(String::as_str).unwrap_or("");
                match cmd {
                    "info" => {
                        let role = if is_master { "master" } else { "slave" };
                        let mut response = role.to_redis_bulk_string();
                        response.push_str(
                            &"master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
                                .to_redis_bulk_string(),
                        );
                        response.push_str(&"master_repl_offset:0".to_redis_bulk_string());
                        stream.write().await.write_all(response.as_bytes()).await?;
                    }
                    "ping" => {
                        stream.write().await.write_all(b"+PONG\r\n").await?;
                    }
                    "echo" => {
                        let message = command.parts.get(1).cloned().unwrap_or_default();
                        stream
                            .write()
                            .await
                            .write_all(format!("+{}\r\n", message).as_bytes())
                            .await?;
                    }
                    "set" => {
                        if self.in_transaction.load(Ordering::SeqCst) {
                            self.commands.write().await.push(command);
                            stream
                                .write()
                                .await
                                .write_all(b"+QUEUED\r\n")
                                .await?;
                            continue;
                        }
                        if let Some(key) = command.parts.get(1) {
                            let value = command.parts.get(2).cloned().unwrap_or_default();
                            if let Some(px_index) =
                                command.parts.iter().position(|s| s == "px")
                            {
                                if let Some(ttl_str) = command.parts.get(px_index + 1) {
                                    if let Ok(ttl) = ttl_str.parse::<i64>() {
                                        let expire_time = ttl + Utc::now().timestamp_millis();
                                        self.expire.write().await.insert(key.clone(), expire_time);
                                    } else {
                                        stream
                                            .write()
                                            .await
                                            .write_all(b"-ERR invalid TTL\r\n")
                                            .await?;
                                        continue;
                                    }
                                } else {
                                    stream
                                        .write()
                                        .await
                                        .write_all(b"-ERR no TTL provided\r\n")
                                        .await?;
                                    continue;
                                }
                            }
                            self.kv.write().await.insert(key.clone(), value);
                            let msg = command.parts[..3].join(" ").to_resp_array();
                            stream.write().await.write_all(b"+OK\r\n").await?;
                            self.offset
                                .fetch_add(command.bytes, Ordering::SeqCst);
                            if is_master && !self.replicas.read().await.is_empty() {
                                self.write_pending.store(true, Ordering::SeqCst);
                                let _ = self.tx.send(msg);
                            }
                        } else {
                            stream
                                .write()
                                .await
                                .write_all(b"-ERR no key provided\r\n")
                                .await?;
                        }
                    }
                    "incr" => {
                        if self.in_transaction.load(Ordering::SeqCst) {
                            self.commands.write().await.push(command);
                            stream
                                .write()
                                .await
                                .write_all(b"+QUEUED\r\n")
                                .await?;
                            continue;
                        }
                        if let Some(key) = command.parts.get(1) {
                            let mut kv = self.kv.write().await;
                            let new_val = if let Some(val_str) = kv.get(key) {
                                match val_str.parse::<i64>() {
                                    Ok(num) => num + 1,
                                    Err(_) => {
                                        stream
                                            .write()
                                            .await
                                            .write_all(b"-ERR value is not an integer\r\n")
                                            .await?;
                                        continue;
                                    }
                                }
                            } else {
                                1
                            };
                            kv.insert(key.clone(), new_val.to_string());
                            stream
                                .write()
                                .await
                                .write_all(format!(":{}\r\n", new_val).as_bytes())
                                .await?;
                        } else {
                            stream
                                .write()
                                .await
                                .write_all(b"-ERR no key provided\r\n")
                                .await?;
                        }
                    }
                    "multi" => {
                        self.in_transaction.store(true, Ordering::SeqCst);
                        stream.write().await.write_all(b"+OK\r\n").await?;
                    }
                    "exec" => {
                        if !self.in_transaction.swap(false, Ordering::SeqCst) {
                            stream
                                .write()
                                .await
                                .write_all(b"-ERR EXEC without MULTI\r\n")
                                .await?;
                            continue;
                        }
                        let queued_commands = {
                            let mut cmds = self.commands.write().await;
                            std::mem::take(&mut *cmds)
                        };
                        if queued_commands.is_empty() {
                            stream.write().await.write_all(b"*0\r\n").await?;
                        } else {
                            self.process_commands(queued_commands, is_master, stream)
                                .await?;
                        }
                    }
                    "get" => {
                        if self.in_transaction.load(Ordering::SeqCst) {
                            self.commands.write().await.push(command);
                            stream
                                .write()
                                .await
                                .write_all(b"+QUEUED\r\n")
                                .await?;
                            continue;
                        }
                        if let Some(key) = command.parts.get(1) {
                            let kv = self.kv.read().await;
                            if let Some(value) = kv.get(key) {
                                if let Some(&ttl) = self.expire.read().await.get(key) {
                                    if Utc::now().timestamp_millis() >= ttl {
                                        stream
                                            .write()
                                            .await
                                            .write_all(b"$-1\r\n")
                                            .await?;
                                        drop(kv);
                                        self.kv.write().await.remove(key);
                                        continue;
                                    }
                                }
                                stream
                                    .write()
                                    .await
                                    .write_all(format!("+{}\r\n", value).as_bytes())
                                    .await?;
                            } else {
                                stream.write().await.write_all(b"$-1\r\n").await?;
                            }
                        } else {
                            stream
                                .write()
                                .await
                                .write_all(b"-ERR no key provided\r\n")
                                .await?;
                        }
                    }
                    "replconf" => {
                        // For simplicity, just reply OK.
                        stream.write().await.write_all(b"+OK\r\n").await?;
                    }
                    "wait" => {
                        if let (Some(min_str), Some(timeout_str)) =
                            (command.parts.get(1), command.parts.get(2))
                        {
                            if let (Ok(min), Ok(timeout)) =
                                (min_str.parse::<i64>(), timeout_str.parse::<i64>())
                            {
                                self.wait(stream.clone(), min, timeout).await?;
                            } else {
                                stream
                                    .write()
                                    .await
                                    .write_all(b"-ERR invalid arguments for wait\r\n")
                                    .await?;
                            }
                        } else {
                            stream
                                .write()
                                .await
                                .write_all(b"-ERR missing arguments for wait\r\n")
                                .await?;
                        }
                    }
                    "config" => {
                        if command.parts.contains(&"get".to_string())
                            && command.parts.contains(&"dir".to_string())
                        {
                            if let Some(dir) = &self.config.dir {
                                let response = format!("dir {}", dir).to_resp_array();
                                stream.write().await.write_all(response.as_bytes()).await?;
                            } else {
                                stream
                                    .write()
                                    .await
                                    .write_all(b"-ERR no dir provided\r\n")
                                    .await?;
                            }
                        } else if command.parts.contains(&"get".to_string())
                            && command.parts.contains(&"dbfilename".to_string())
                        {
                            if let Some(db) = &self.config.db_filename {
                                let response = format!("dbfilename {}", db).to_resp_array();
                                stream.write().await.write_all(response.as_bytes()).await?;
                            } else {
                                stream
                                    .write()
                                    .await
                                    .write_all(b"-ERR no dbfilename provided\r\n")
                                    .await?;
                            }
                        } else {
                            stream.write().await.write_all(b"+OK\r\n").await?;
                        }
                    }
                    "psync" => {
                        let replication_id = if let Some(id) = command.parts.get(1) {
                            if id == "?" {
                                generate_random_id(16)
                            } else {
                                id.clone()
                            }
                        } else {
                            String::new()
                        };
                        let response =
                            format!("FULLRESYNC {} 0", replication_id).to_redis_bulk_string();
                        stream.write().await.write_all(response.as_bytes()).await?;
                        // Send an empty file payload.
                        let payload = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")?;
                        let payload_resp = format!("${}\r\n", payload.len());
                        stream.write().await.write_all(payload_resp.as_bytes()).await?;
                        stream.write().await.write_all(&payload).await?;
                        self.replicas.write().await.push(stream.clone());
                        return Ok(());
                    }
                    _ => {
                        stream
                            .write()
                            .await
                            .write_all(b"-ERR unknown command\r\n")
                            .await?;
                    }
                }
            }
            Ok(())
        }
        .boxed()
    }

    /// Implements the WAIT command: block until the required number of replicas have acknowledged writes.
    async fn wait(
        &self,
        stream: Arc<RwLock<TcpStream>>,
        min_replica_count: i64,
        timeout: i64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !self.write_pending.load(Ordering::SeqCst) {
            let count = self.replicas.read().await.len();
            stream
                .write()
                .await
                .write_all(format!(":{}\r\n", count).as_bytes())
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
            let current_count = propagation_count.load(Ordering::SeqCst);
            if time_remaining <= 0 || current_count == self.replicas.read().await.len() {
                stream
                    .write()
                    .await
                    .write_all(format!(":{}\r\n", current_count).as_bytes())
                    .await?;
                self.write_pending.store(false, Ordering::SeqCst);
                return Ok(());
            }

            for replica in self.replicas.read().await.iter() {
                let propagation_count = propagation_count.clone();
                let self_clone = self.clone();
                let stream_clone = stream.clone();
                let replica = replica.clone();
                tokio::spawn(async move {
                    if let Ok(mut rep) = replica.try_write() {
                        let mut buf = [0; 1024];
                        if let Ok(n) = rep.try_read(&mut buf) {
                            let reply = String::from_utf8_lossy(&buf[..n]);
                            if let Ok(cmds) = reply.to_command_list() {
                                if !cmds.is_empty() {
                                    if let Ok(offset) = cmds[0]
                                        .parts
                                        .get(2)
                                        .unwrap_or(&"0".to_string())
                                        .parse::<usize>()
                                    {
                                        let current_offset =
                                            self_clone.offset.load(Ordering::SeqCst);
                                        if current_offset <= offset {
                                            propagation_count.fetch_add(1, Ordering::SeqCst);
                                        }
                                    } else {
                                        let _ = stream_clone
                                            .write()
                                            .await
                                            .write_all(b"-ERR invalid offset\r\n")
                                            .await;
                                    }
                                }
                            } else {
                                let _ = stream_clone
                                    .write()
                                    .await
                                    .write_all(b"-ERR ack failed\r\n")
                                    .await;
                            }
                        }
                    }
                });
            }

            sleep(Duration::from_millis(25)).await;
            if propagation_count.load(Ordering::SeqCst) >= min_replica_count as usize {
                stream
                    .write()
                    .await
                    .write_all(
                        format!(":{}\r\n", propagation_count.load(Ordering::SeqCst))
                            .as_bytes(),
                    )
                    .await?;
                self.write_pending.store(false, Ordering::SeqCst);
                return Ok(());
            }
            sleep(Duration::from_millis(25)).await;
        }
    }

    /// Propagate writes to all replicas.
    async fn propagate(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut rx = self.tx.subscribe();
        while let Ok(received) = rx.recv().await {
            for replica in self.replicas.read().await.iter() {
                if let Ok(mut rep) = replica.try_write() {
                    if let Err(e) = rep.write_all(received.as_bytes()).await {
                        eprintln!("Failed to write to replica: {}", e);
                    }
                } else {
                    eprintln!("Failed to acquire write lock for a replica");
                }
            }
        }
        Ok(())
    }

    /// Periodically check for expired keys and remove them.
    async fn gc(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut interval = interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            let keys_to_remove: Vec<String> = {
                let expire = self.expire.read().await;
                 self.kv.read().await.iter()
                    .flat_map(|(key, _)| {
                        if let Some(&ttl) = expire.get(key) {
                            if Utc::now().timestamp_millis() >= ttl {
                                return Some(key.clone());
                            }
                        }
                        None
                    })
                    .collect()
            };

            if !keys_to_remove.is_empty() {
                let mut kv = self.kv.write().await;
                for key in keys_to_remove {
                    kv.remove(&key);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Starting Redis-like server...");
    let redis = Redis::new();
    if let Err(e) = redis.init().await {
        eprintln!("Error: {}", e);
    }
    Ok(())
}

/// Generate a random ID composed of uppercase and lowercase letters.
fn generate_random_id(length: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..length)
        .map(|_| {
            let idx = rng.gen_range(0..52);
            if idx < 26 {
                (b'a' + idx as u8) as char
            } else {
                (b'A' + (idx - 26) as u8) as char
            }
        })
        .collect()
}

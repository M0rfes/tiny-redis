// Uncomment this block to pass the first stage
use chrono::Utc;
use rand::Rng;
use std::env;
use std::{collections::HashMap, error::Error, str, sync::Arc};
use tokio::net::TcpStream;
use tokio::sync::broadcast::{self, Sender};
use tokio::sync::RwLock;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    let args: Vec<String> = env::args().collect();
    let shared_map = Arc::new(RwLock::new(HashMap::new()));
    let mut config = HashMap::new();
    let mut port = "6379";
    if let Some(port_index) = args.iter().position(|s| s == "--port") {
        port = args.get(port_index + 1).expect("no port provided");
    }
    config.insert("port", port.to_owned());
    if let Some(replica_of_index) = args.iter().position(|s| s == "--replicaof") {
        let replica: Vec<&str> = args.get(replica_of_index + 1).unwrap().split(" ").collect();
        config.insert("master_host", replica[0].to_owned());
        config.insert("master_port", replica[1].to_owned());
    }
    if let Some(master_host) = config.get("master_host") {
        let master_port = config.get("master_port").unwrap();
        let port = config.get("port").unwrap();
        let address = format!("{master_host}:{master_port}");
        let map_clone = shared_map.clone();
        tokio::spawn(run_replica(address.to_owned(), port.to_owned(), map_clone));
    }
    let config = Arc::new(RwLock::new(config));
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    let (tx, _) = broadcast::channel(100);
    loop {
        let (stream, _) = listener.accept().await?;
        let map_clone = shared_map.clone();
        let config_clone = config.clone();
        let tx_clone = tx.clone();
        let stream = Arc::new(RwLock::new(stream));
        let stream = stream.clone();
        tokio::spawn(handle_stream(stream, map_clone, config_clone, tx_clone));
    }
}

enum Role {
    Master,
    Slave,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Master => write!(f, "master"),
            Role::Slave => write!(f, "slave"),
        }
    }
}

async fn handle_stream(
    stream: Arc<RwLock<TcpStream>>,
    shared_map: Arc<RwLock<HashMap<String, Value>>>,
    config: Arc<RwLock<HashMap<&str, String>>>,
    tx: Sender<String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut buf = [0; 512];
    let role = match config.read().await.get("master_host") {
        Some(_) => Role::Slave,
        None => Role::Master,
    };
    let mut replicas: Vec<Arc<RwLock<TcpStream>>> = vec![];

    loop {
        let read_count = stream.write().await.read(&mut buf).await?;
        if read_count == 0 {
            break;
        }
        let commands = match parse_resp(&buf[..read_count]) {
            Ok(c) => c,
            Err(e) => panic!("{e:?}"),
        };
        for mut command in commands {
            if command.contains(&String::from("info")) {
                let mut response = to_redis_bulk_string(format!("role:{}", role).as_str());
                response = to_redis_bulk_string(
                    format!("{response}master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
                        .as_str(),
                );
                response = to_redis_bulk_string(format!("{response}master_repl_offset:0").as_str());
                stream.write().await.write_all(response.as_bytes()).await?;
            } else if command.contains(&String::from("ping")) {
                stream.write().await.write_all(b"+PONG\r\n").await?;
            } else if command.contains(&String::from("echo")) {
                let default = String::from("");
                let message = command.get(1).unwrap_or(&default);
                stream
                    .write()
                    .await
                    .write_all(format!("+{}\r\n", message).as_bytes())
                    .await?;
            } else if command.contains(&String::from("set")) {
                let default = String::from("");
                let Some(key) = command.get(1) else {
                    stream
                        .write()
                        .await
                        .write_all(b"-ERR no key provided\r\n")
                        .await?;
                    continue;
                };
                let value = command.get(2).unwrap_or(&default);
                let k = key.to_string();
                let mut ttl = 0;
                if let Some(px_index) = command.iter().position(|s| s.to_lowercase() == "px") {
                    let _ttl = command.get(px_index + 1);
                    if _ttl.is_none() {
                        stream
                            .write()
                            .await
                            .write_all(b"-no ttl provided\r\n")
                            .await?;
                        continue;
                    }
                    let _ttl = _ttl.unwrap().parse::<i64>();
                    if _ttl.is_err() {
                        stream
                            .write()
                            .await
                            .write_all(b"-ttl not valid i64\r\n")
                            .await?;
                        continue;
                    }
                    ttl = _ttl.unwrap();
                }

                let v = Value {
                    value: value.to_owned(),
                    created_at: Utc::now().timestamp_millis(),
                    ttl,
                };
                let mut map = shared_map.write().await;
                map.insert(k, v);
                drop(map);
                let message = format_as_resp_array(command[..3].to_vec());

                stream.write().await.write_all(b"+OK\r\n").await?;
                match role {
                    Role::Master => {
                        let r = tx.send(message);
                        println!("{r:?}\n------------------------");
                    }
                    _ => (),
                };
            } else if command.contains(&String::from("get")) {
                let Some(key) = command.get(1) else {
                    stream
                        .write()
                        .await
                        .write_all(b"-ERR no key provided\r\n")
                        .await?;
                    continue;
                };
                let map = shared_map.read().await;
                let Some(value) = map.get(key) else {
                    drop(map);
                    stream.write().await.write_all(b"$-1\r\n").await?;
                    continue;
                };

                if value.ttl != 0 && Utc::now().timestamp_millis() - value.created_at >= value.ttl {
                    drop(map);
                    let mut map = shared_map.write().await;
                    map.remove(key);
                    drop(map);
                    stream.write().await.write_all(b"$-1\r\n").await?;
                    continue;
                }
                stream
                    .write()
                    .await
                    .write_all(format!("+{}\r\n", value.value).as_bytes())
                    .await?;
            } else if command.contains(&String::from("replconf")) {
                if command.contains(&String::from("listening-port")) {
                    let Some(port) = command.get(2) else {
                        stream
                            .write()
                            .await
                            .write_all(b"-No port provided\r\n")
                            .await?;
                        continue;
                    };
                    let mut config = config.write().await;
                    config.insert("slave_port", port.to_owned());
                }
                stream.write().await.write_all(b"+OK\r\n").await?;
            } else if command.contains(&String::from("psync")) {
                let Some(replication_id) = command.get_mut(1) else {
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
                        to_redis_bulk_string(format!("FULLRESYNC {} 0", replication_id).as_str())
                            .as_bytes(),
                    )
                    .await?;
                let empty_file_payload = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")?;
                stream
                    .write()
                    .await
                    .write(format!("${}\r\n", empty_file_payload.len()).as_bytes())
                    .await?;
                stream
                    .write()
                    .await
                    .write(empty_file_payload.as_slice())
                    .await?;
                // store all the streams
                replicas.push(stream.clone());
                while let Ok(received) = tx.subscribe().recv().await {
                    for replica in &replicas {
                        let mut stream = replica.write().await;
                        stream.write_all(received.as_bytes()).await?;
                    }
                }
            } else {
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

fn parse_resp(buf: &[u8]) -> Result<Vec<Vec<String>>, Box<dyn Error + Send + Sync>> {
    let message = String::from_utf8_lossy(buf);
    let mut lines = message.lines().peekable();
    let mut commands = Vec::new();

    while lines.peek().is_some() {
        let item_num_line = lines.next().ok_or("message is empty")?;
        if !item_num_line.starts_with('*') {
            return Err(format!("expected array format but got {item_num_line}").into());
        }
        let item_nums: usize = item_num_line[1..].parse()?;

        let mut command = Vec::with_capacity(item_nums);
        for _ in 0..item_nums {
            let bulk_len_line = lines.next().ok_or("bulk length line is empty")?;
            if !bulk_len_line.starts_with('$') {
                return Err("expected bulk string format".into());
            }
            let _bulk_len: usize = bulk_len_line[1..].parse()?;

            let bulk_str = lines.next().ok_or("bulk string line is empty")?;
            command.push(bulk_str.to_lowercase());
        }
        commands.push(command);
    }

    Ok(commands)
}

fn to_redis_bulk_string(input: &str) -> String {
    let length = input.len();
    format!("${}\r\n{}\r\n", length, input)
}

fn format_as_resp_array(vec: Vec<String>) -> String {
    let mut resp_string = format!("*{}\r\n", vec.len());
    for element in vec {
        resp_string.push_str(&format!("${}\r\n{}\r\n", element.len(), element));
    }
    resp_string
}

#[derive(Debug)]
struct Value {
    value: String,
    created_at: i64,
    ttl: i64,
}

fn generate_random_id(length: usize) -> String {
    let mut rng = rand::thread_rng();
    let id: String = (0..length)
        .map(|_| {
            let idx = rng.gen_range(0..52);
            let c = if idx < 26 {
                (b'a' + idx as u8) as char
            } else {
                (b'A' + (idx - 26) as u8) as char
            };
            c
        })
        .collect();
    id
}

async fn run_replica(
    address: String,
    port: String,
    shared_map: Arc<RwLock<HashMap<String, Value>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
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
                port
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
        .write_all(format!("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",).as_bytes())
        .await?;
    let n = socket.read(&mut buf).await?;
    let response = String::from_utf8_lossy(&buf[..n]);
    if response.trim() != "+OK" {
        panic!("wanted OK got:{response}");
    }
    socket
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .await?;
    let _n = socket.read(&mut buf).await?;
    let _n = socket.read(&mut buf).await?;
    let mut offset: usize = 0;
    loop {
        let n = socket.read(&mut buf).await?;
        if n == 0 {
            break;
        } else {
            let Ok(commands) = parse_resp(&buf[..n]) else {
                continue;
            };
            for command in commands {
                println!("{command:?}");
                if command.contains(&String::from("ping")) {
                    offset += buf[..n].len();
                    println!("{offset}");
                } else if command.contains(&String::from("set")) {
                    offset += buf[..n].len();
                    println!("{offset}");
                    let default = String::from("");
                    let Some(key) = command.get(1) else {
                        continue;
                    };
                    let value = command.get(2).unwrap_or(&default);
                    let k = key.to_string();
                    let mut ttl = 0;
                    if let Some(px_index) = command.iter().position(|s| s.to_lowercase() == "px") {
                        let _ttl = command.get(px_index + 1);
                        if _ttl.is_none() {
                            continue;
                        }
                        let _ttl = _ttl.unwrap().parse::<i64>();
                        if _ttl.is_err() {
                            continue;
                        }
                        ttl = _ttl.unwrap();
                    }

                    let v = Value {
                        value: value.to_owned(),
                        created_at: Utc::now().timestamp_millis(),
                        ttl,
                    };
                    let mut map = shared_map.write().await;
                    map.insert(k, v);
                    drop(map);
                } else if command.contains(&String::from("replconf")) {
                    let replay: Vec<String> = format!("REPLCONF ACK {}", offset)
                        .split(" ")
                        .map(str::to_string)
                        .collect();

                    offset += buf[..n].len();
                    println!("{offset}");
                    let replay = format_as_resp_array(replay);
                    socket.write_all(replay.as_bytes()).await?;
                }
            }
        }
    }
    Ok(())
}

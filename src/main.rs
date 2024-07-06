// Uncomment this block to pass the first stage
use chrono::Utc;
use std::env;
use std::{collections::HashMap, error::Error, str, sync::Arc};
use tokio::net::TcpStream;
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
        config.insert("master_host", args[replica_of_index + 1].to_owned());
        config.insert("master_port", args[replica_of_index + 2].to_owned());
    }
    if let Some(master_host) = config.get("master_host") {
        let master_port = config.get("master_port").unwrap();
        let address = format!("{master_host}:{master_port}");
        let mut socket = TcpStream::connect(address).await?;
        socket.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
        let mut buf = [0; 1024];
        let n = socket.read(&mut buf).await?;
        let response = String::from_utf8_lossy(&buf[..n]);
        if response.trim() != "+PONG" {
            panic!("wanted pong got:{response}");
        }
        let port = config.get("port").unwrap();

        socket
            .write_all(
                format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n",
                    port
                )
                .as_bytes(),
            )
            .await?;
        let mut buf = [0; 1024];
        let n = socket.read(&mut buf).await?;
        let response = String::from_utf8_lossy(&buf[..n]);
        if response.trim() != "+OK" {
            panic!("wanted OK got:{response}");
        }
        socket
            .write_all(
                format!("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",).as_bytes(),
            )
            .await?;
        let mut buf = [0; 1024];
        let n = socket.read(&mut buf).await?;
        let response = String::from_utf8_lossy(&buf[..n]);
        if response.trim() != "+OK" {
            panic!("wanted OK got:{response}");
        }
        socket
            .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
            .await?;
        let mut buf = [0; 1024];
        let _n = socket.read(&mut buf).await?;
    }
    let config = Arc::new(RwLock::new(config));
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let map_clone = Arc::clone(&shared_map);
        let config_clone = Arc::clone(&config);
        tokio::spawn(handle_stream(stream, map_clone, config_clone));
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
    mut stream: tokio::net::TcpStream,
    shared_map: Arc<RwLock<HashMap<String, Value>>>,
    config: Arc<RwLock<HashMap<&str, String>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut buf = [0; 512];
    let req_time = Utc::now().timestamp_millis();
    let role = match config.read().await.get("master_host") {
        Some(_) => Role::Slave,
        None => Role::Master,
    };
    loop {
        let read_count = stream.read(&mut buf).await?;
        if read_count == 0 {
            break Ok(());
        }
        let mut command = parse_resp(&buf)?;

        if command.contains(&String::from("info")) {
            let mut response = to_redis_bulk_string(format!("role:{}", role).as_str());
            // master_replid: 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
            response = to_redis_bulk_string(
                format!("{response}master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
                    .as_str(),
            );
            // master_repl_offset: 0
            response = to_redis_bulk_string(format!("{response}master_repl_offset:0").as_str());
            stream.write_all(response.as_bytes()).await?;
        } else if command.contains(&String::from("ping")) {
            stream.write_all(b"+PONG\r\n").await?;
        } else if command.contains(&String::from("echo")) {
            let default = String::from("");
            let message = command.get(1).unwrap_or(&default);
            stream
                .write_all(format!("+{}\r\n", message).as_bytes())
                .await?;
        } else if command.contains(&String::from("set")) {
            match role {
                Role::Slave => {
                    println!("salve recived value {}", command.join(" "));
                }
                _ => (),
            }
            let default = String::from("");
            let Some(key) = command.get(1) else {
                stream.write_all(b"-ERR no key provided\r\n").await?;
                continue;
            };
            let value = command.get(2).unwrap_or(&default);
            let k = key.to_string();
            let mut ttl: i64 = 0;
            if let Some(px_index) = command.iter().position(|s| s.to_lowercase() == "px") {
                let _ttl = command.get(px_index + 1);
                if _ttl.is_none() {
                    stream.write_all(b"-no ttl provided\r\n").await?;
                    continue;
                }
                let _ttl = _ttl.unwrap().parse::<i64>();
                if _ttl.is_err() {
                    stream.write_all(b"-ttl not valid u64\r\n").await?;
                    continue;
                }
                ttl = _ttl.unwrap();
            }
            let v = Value {
                value: value.to_owned(),
                created_at: req_time,
                ttl,
            };
            let mut map = shared_map.write().await;
            map.insert(k, v);
            drop(map);
            stream.write_all(b"+OK\r\n").await?;
            let config = config.read().await;
            let Some(port) = config.get("slave_port") else {
                continue;
            };
            let message = format_as_resp_array(command[..3].to_vec());

            match role {
                Role::Master => {
                    tokio::spawn(propagate(port.to_owned(), message));
                }
                _ => (),
            };
        } else if command.contains(&String::from("get")) {
            let map = shared_map.read().await;
            let Some(key) = command.get(1) else {
                stream.write_all(b"-ERR no key provided\r\n").await?;
                continue;
            };
            let Some(value) = map.get(key) else {
                drop(map);
                stream.write_all(b"$-1\r\n").await?;
                continue;
            };
            if value.ttl != 0 && Utc::now().timestamp_millis() - value.created_at >= value.ttl {
                drop(map);
                let mut map = shared_map.write().await;
                map.remove(key);
                drop(map);
                stream.write_all(b"$-1\r\n").await?;
                continue;
            }
            stream
                .write_all(format!("+{}\r\n", value.value).as_bytes())
                .await?;
        } else if command.contains(&String::from("replconf")) {
            if command.contains(&String::from("listening-port")) {
                let Some(port) = command.get(2) else {
                    stream.write_all(b"-No port provided\r\n").await?;
                    continue;
                };
                let mut config = config.write().await;
                config.insert("slave_port", port.to_owned());
            }
            stream.write_all(b"+OK\r\n").await?;
        } else if command.contains(&String::from("psync")) {
            let Some(replication_id) = command.get_mut(1) else {
                stream.write_all(b"-ERR no replication id provided").await?;
                continue;
            };
            if replication_id == "?" {
                replication_id.clear();
                replication_id.push_str("abc")
            }
            stream
                .write_all(
                    to_redis_bulk_string(format!("FULLRESYNC {} 0", replication_id).as_str())
                        .as_bytes(),
                )
                .await?;
            let empty_file_payload = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")?;
            stream
                .write(format!("${}\r\n", empty_file_payload.len()).as_bytes())
                .await?;
            stream.write(empty_file_payload.as_slice()).await?;
        } else {
            stream.write_all(b"-ERR unknown command\r\n").await?;
        }
    }
}

fn parse_resp(buf: &[u8]) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
    let message = String::from_utf8_lossy(buf);
    let mut lines = message.lines();

    let item_num_line = lines.next().ok_or("message is empty")?;
    let item_nums: usize = item_num_line[1..].parse()?;

    let mut command = Vec::with_capacity(item_nums);
    for _ in 0..item_nums {
        let bulk_len_line = lines.next().ok_or("bulk length line is empty")?;
        let _bulk_len: usize = bulk_len_line[1..].parse()?;

        let bulk_str = lines.next().ok_or("bulk string line is empty")?;
        command.push(bulk_str.to_lowercase());
    }

    Ok(command)
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

struct Value {
    value: String,
    created_at: i64,
    ttl: i64,
}

async fn propagate(port: String, message: String) -> Result<(), Box<dyn Error + Send + Sync>> {
    let address = format!("127.0.0.1:{}", port);
    println!("sending to {address}");
    let mut slave_socket = TcpStream::connect(address).await?;
    slave_socket.write_all(b"+hello").await?;
    Ok(())
}

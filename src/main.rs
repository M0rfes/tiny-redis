// Uncomment this block to pass the first stage
use std::env;
use std::{collections::HashMap, error::Error, str, sync::Arc};
use tokio::sync::RwLock;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{sleep, Duration},
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
        let replica_of = args
            .get(replica_of_index + 1)
            .expect("no master info provided")
            .split(" ")
            .collect::<Vec<&str>>();
        config.insert("replica_of_host", replica_of[0].to_owned());
        config.insert("replica_of_port", replica_of[1].to_owned());
    }
    let config = Arc::new(RwLock::new(config));
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    loop {
        let (stream, _) = listener.accept().await?;
        let map_clone = Arc::clone(&shared_map);
        let config_clone = Arc::clone(&config);
        tokio::spawn(handle_stream(stream, map_clone, config_clone));
    }
}

async fn handle_stream(
    mut stream: tokio::net::TcpStream,
    shared_map: Arc<RwLock<HashMap<String, String>>>,
    config: Arc<RwLock<HashMap<&str, String>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut buf = [0; 512];
    loop {
        let read_count = stream.read(&mut buf).await?;
        if read_count == 0 {
            break Ok(());
        }
        let command = parse_resp(&buf)?;
        if command.iter().any(|s| s == "info") {
            let role = config.read().await;
            let role = match role.get("replica_of_host") {
                Some(_) => "slave",
                None => "master",
            };
            let response = to_redis_bulk_string(format!("role:{}", role).as_str());
            stream.write_all(response.as_bytes()).await?;
        }

        if command.iter().any(|s| s == "ping") {
            stream.write_all(b"+PONG\r\n").await?;
            return Ok(());
        }

        if command.iter().any(|s| s == "echo") {
            let default = String::from("");
            let message = command.get(1).unwrap_or(&default);
            stream
                .write_all(format!("+{}\r\n", message).as_bytes())
                .await?;
            return Ok(());
        }

        if command.iter().any(|s| s == "set") {
            let default = String::from("");
            let Some(key) = command.get(1) else {
                stream.write_all(b"-ERR no key provided\r\n").await?;
                return Ok(());
            };
            let value = command.get(2).unwrap_or(&default);
            let mut map = shared_map.write().await;
            let k = key.to_string();
            map.insert(k, value.to_string());
            let Some(px_index) = command.iter().position(|s| s == "px") else {
                stream.write_all(b"+OK\r\n").await?;
                return Ok(());
            };
            let ttl: u64 = command
                .get(px_index + 1)
                .unwrap_or(&String::from("0"))
                .parse::<u64>()?;
            if ttl > 0 {
                let clone_map = shared_map.clone();
                let k_clone = key.to_string();
                tokio::spawn(async move {
                    sleep(Duration::from_millis(ttl)).await;
                    let mut map = clone_map.write().await;
                    map.remove(&k_clone);
                });
            }
            stream.write_all(b"+OK\r\n").await?;
            return Ok(());
        }
        if command.iter().any(|s| s == "get") {
            let map = shared_map.read().await;
            let Some(key) = command.get(1) else {
                stream.write_all(b"-ERR no key provided\r\n").await?;
                return Ok(());
            };
            let Some(value) = map.get(key) else {
                stream.write_all(b"$-1\r\n").await?;
                return Ok(());
            };
            stream
                .write_all(format!("+{}\r\n", value).as_bytes())
                .await?;
            return Ok(());
        }
        stream.write_all(b"-ERR unknown command\r\n").await?;
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

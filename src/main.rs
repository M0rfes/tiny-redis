// Uncomment this block to pass the first stage
use std::{collections::HashMap, error::Error, str, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::RwLock,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    // Uncomment this block to pass the first stage
    //
    let shared_map = Arc::new(RwLock::new(HashMap::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (stream, _) = listener.accept().await?;
        let map_clone = Arc::clone(&shared_map);
        tokio::spawn(handle_stream(stream, map_clone));
    }
}

async fn handle_stream(
    mut stream: tokio::net::TcpStream,
    shared_map: Arc<RwLock<HashMap<String, String>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut buf = [0; 512];
    loop {
        let read_count = stream.read(&mut buf).await?;
        if read_count == 0 {
            break Ok(());
        }
        let mut command = parse_resp(&buf)?;
        command[0] = command[0].to_lowercase();
        match command
            .iter()
            .map(String::as_str)
            .collect::<Vec<&str>>()
            .as_slice()
        {
            ["ping"] => {
                stream.write_all(b"+PONG\r\n").await?;
            }
            ["echo", message] => {
                stream
                    .write_all(format!("+{}\r\n", message).as_bytes())
                    .await?;
            }
            ["set", key, value] => {
                let mut map = shared_map.write().await;
                map.insert(key.to_string(), value.to_string());
                stream.write_all(b"+OK\r\n").await?;
            }
            ["get", key] => {
                let map = shared_map.read().await;
                let default = String::from("");
                let value = map.get(key.to_owned()).unwrap_or(&default);
                stream
                    .write_all(format!("+{}\r\n", value).as_bytes())
                    .await?;
            }
            _ => stream.write_all(b"-ERR unknown command\r\n").await?,
        };
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
        command.push(bulk_str.to_owned());
    }

    Ok(command)
}

// Uncomment this block to pass the first stage
use std::{error::Error, str};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move { handle_stream(stream).await.expect("handler failed") });
    }
}

async fn handle_stream(mut stream: tokio::net::TcpStream) -> Result<(), Box<dyn Error>> {
    let mut buf = [0; 512];
    loop {
        let read_count = stream.read(&mut buf).await?;
        if read_count == 0 {
            break Ok(());
        }
        let mut command = parse_resp(&buf)?;
        command[0] = command[0].to_lowercase();
        let response = match command
            .iter()
            .map(String::as_str)
            .collect::<Vec<&str>>()
            .as_slice()
        {
            ["ping"] => "+PONG\r\n".to_owned(),
            ["echo", message] => format!("+{}\r\n", message),
            _ => "-ERR unknown command\r\n".to_owned(),
        };
        stream.write_all(response.as_bytes()).await?;
    }
}

fn parse_resp(buf: &[u8]) -> Result<Vec<String>, Box<dyn Error>> {
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

//! Run with
//!
//! ```not_rust
//! cargo run --example split
//! ```

use embedded_io_adapters::tokio_1::FromTokio;
use rand::{
    SeedableRng,
    rngs::{StdRng, SysRng},
};
use tokio::{
    io::{ReadHalf, WriteHalf},
    net::TcpStream,
};
use websocketz::{Message, WebSocket, next, options::ConnectOptions};

fn split(
    stream: FromTokio<TcpStream>,
) -> (
    FromTokio<ReadHalf<TcpStream>>,
    FromTokio<WriteHalf<TcpStream>>,
) {
    let (read, write) = tokio::io::split(stream.into_inner());

    (FromTokio::new(read), FromTokio::new(write))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect("127.0.0.1:9002").await?;

    let read_buf = &mut [0u8; 8192 * 2];
    let write_buf = &mut [0u8; 8192 * 2];
    let fragments_buf = &mut [0u8; 8192 * 2];
    let rng = StdRng::try_from_rng(&mut SysRng).unwrap();

    let websocketz = WebSocket::connect::<16>(
        ConnectOptions::default(),
        FromTokio::new(stream),
        rng,
        read_buf,
        write_buf,
        fragments_buf,
    )
    .await?;

    let (mut websocketz_read, mut websocketz_write) = websocketz.split_with(split);

    websocketz_write
        .send(Message::Text("Hello, WebSocket!"))
        .await?;

    websocketz_write
        .send_fragmented(Message::Text("Hello, Fragmented WebSocket!"), 4)
        .await?;

    while let Some(message) = next!(websocketz_read).transpose()? {
        println!("Received message: {message:?}");
    }

    Ok(())
}

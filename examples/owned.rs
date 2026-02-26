//! Run with
//!
//! ```not_rust
//! cargo run --example owned
//! ```

use std::pin::pin;

use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embedded_io_adapters::tokio_1::FromTokio;
use futures::{SinkExt, StreamExt};
use rand::{SeedableRng, rngs::StdRng};
use tokio::net::TcpStream;
use websocketz::{Message, WebSocket, http::Header, options::ConnectOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let domain = "websockets.chilkat.io";

    let addr = tokio::net::lookup_host((domain, 80))
        .await?
        .next()
        .ok_or("Failed to resolve domain")?;

    let stream = TcpStream::connect(addr).await?;

    let read_buf = &mut [0u8; 8192 * 2];
    let write_buf = &mut [0u8; 8192 * 2];
    let fragments_buf = &mut [0u8; 8192 * 2];
    let rng = StdRng::from_os_rng();

    let websocketz = WebSocket::connect::<16>(
        ConnectOptions::default()
            .with_path_unchecked("/wsChilkatEcho.ashx")
            .with_headers(&[Header {
                name: "Host",
                value: domain.as_bytes(),
            }]),
        FromTokio::new(stream),
        rng,
        read_buf,
        write_buf,
        fragments_buf,
    )
    .await?;

    println!(
        "Number of framable bytes after handshake: {}",
        websocketz.framable()
    );

    let websocketz = websocketz.owned::<1024, NoopRawMutex>();

    let (stream, sink) = websocketz.split();
    let (mut stream, mut sink) = (pin!(stream), pin!(sink));

    sink.send(Message::Text("Hello, WebSocket!")).await?;

    loop {
        tokio::select! {
            msg = stream.next() => match msg.transpose()? {
                None => {
                    println!("EOF");

                    break;
                }
                Some(msg) => {
                    println!("Received message: {msg:?}");
                }
            },
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                println!("Sending message...");

                sink.send(Message::Text("Hello, WebSocket!")).await?; // This deadlocks

                println!("Message sent!");
            }
        }
    }

    Ok(())
}

//! Run with
//!
//! ```not_rust
//! cargo run --example simple
//! ```

use embedded_io_adapters::tokio_1::FromTokio;
use rand::{
    SeedableRng,
    rngs::{StdRng, SysRng},
};
use tokio::net::TcpStream;
use websocketz::{Message, WebSocket, http::Header, next, options::ConnectOptions};

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
    let rng = StdRng::try_from_rng(&mut SysRng).unwrap();

    let mut websocketz = WebSocket::connect::<16>(
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

    loop {
        websocketz.send(Message::Text("Hello, WebSocket!")).await?;

        match next!(websocketz) {
            None => {
                println!("EOF");

                break;
            }
            Some(Ok(msg)) => {
                println!("Received message: {msg:?}");
            }
            Some(Err(err)) => {
                eprintln!("Error receiving message: {err:?}");

                break;
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    Ok(())
}

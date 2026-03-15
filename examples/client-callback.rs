//! Run with
//!
//! ```not_rust
//! cargo run --example client-callback
//! ```
//!
//! Run this example with the `server-callback` example.

use std::time::Duration;

use embedded_io_adapters::tokio_1::FromTokio;
use httparse::Header;
use rand::{
    SeedableRng,
    rngs::{StdRng, SysRng},
};
use tokio::net::TcpStream;
use websocketz::{Message, WebSocket, http::Response, next, options::ConnectOptions};

#[derive(Debug, thiserror::Error)]
#[error("No `Server-Header: Server-Value` header in the response")]
struct CustomError {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect("127.0.0.1:9002").await?;

    let read_buf = &mut [0u8; 8192];
    let write_buf = &mut [0u8; 8192];
    let fragments_buf = &mut [0u8; 8192];
    let rng = StdRng::try_from_rng(&mut SysRng).unwrap();

    let (mut websocketz, custom) = WebSocket::connect_with(
        ConnectOptions::default()
            // Additional request headers
            .with_headers(&[Header {
                name: "Client-Header",
                value: b"Client-Value",
            }]),
        FromTokio::new(stream),
        rng,
        read_buf,
        write_buf,
        fragments_buf,
        |response: &Response<'_, 16>| {
            // Fail the handshake if `Server-Header: Server-Value` header does not exist in the server response.

            response
                .headers()
                .iter()
                .find(|h| h.name.eq_ignore_ascii_case("Server-Header"))
                .and_then(|h| core::str::from_utf8(h.value).ok())
                .filter(|v| v.eq_ignore_ascii_case("Server-Value"))
                .map(|_| ())
                .ok_or(CustomError {})?;

            // Create a custom value, depending on the response.
            Ok::<&'static str, CustomError>("Ok!")
        },
    )
    .await?;

    println!("Extracted: {custom}");

    println!(
        "Number of framable bytes after handshake: {}",
        websocketz.framable()
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                websocketz.send(Message::Text("Hi")).await?;

            },
            _ = async {
                while let Some(message) = next!(websocketz).transpose()? {
                    println!("Received message: {message:?}");
                }

                Ok::<(), Box<dyn std::error::Error>>(())
            } => {}
        }
    }
}

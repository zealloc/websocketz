//! Run with
//!
//! ```not_rust
//! cargo run --example autobahn-server
//! ```

use embedded_io_adapters::tokio_1::FromTokio;
use rand::{
    SeedableRng,
    rngs::{StdRng, SysRng},
};
use tokio::net::TcpListener;
use websocketz::{Message, WebSocket, error::Error, next, options::AcceptOptions, send};

const SIZE: usize = 24 * 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:9002").await?;
    println!("Server started, listening on 127.0.0.1:9002");

    loop {
        let (stream, _) = listener.accept().await?;

        let future = async move {
            let mut read_buf = vec![0u8; SIZE];
            let mut write_buf = vec![0u8; SIZE];
            let mut fragments_buf = vec![0u8; SIZE];

            let mut websocketz = WebSocket::accept::<16>(
                AcceptOptions::default(),
                FromTokio::new(stream),
                StdRng::try_from_rng(&mut SysRng).unwrap(),
                &mut read_buf,
                &mut write_buf,
                &mut fragments_buf,
            )
            .await?;

            while let Some(message) = next!(websocketz) {
                match message {
                    Ok(message) => match message {
                        Message::Text(payload) => send!(websocketz, Message::Text(payload))?,
                        Message::Binary(payload) => send!(websocketz, Message::Binary(payload))?,
                        _ => {}
                    },
                    Err(err) => {
                        eprintln!("Error reading message: {err}");

                        send!(websocketz, Message::Close(None))?;

                        break;
                    }
                }
            }

            Ok::<(), Error<std::io::Error>>(())
        };

        tokio::spawn(async move {
            if let Err(err) = future.await {
                eprintln!("Error handling connection: {err}");
            }
        });
    }
}

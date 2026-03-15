//! Run with
//!
//! ```not_rust
//! cargo run --example autobahn-client
//! ```

use embedded_io_adapters::tokio_1::FromTokio;
use rand::{
    SeedableRng,
    rngs::{StdRng, SysRng},
};
use tokio::net::TcpStream;
use websocketz::{
    CloseCode, CloseFrame, Message, WebSocket, http::Header, next, options::ConnectOptions, send,
    send_fragmented,
};

async fn connect<'buf>(
    path: &str,
    read_buf: &'buf mut [u8],
    write_buf: &'buf mut [u8],
    fragments_buf: &'buf mut [u8],
) -> Result<WebSocket<'buf, FromTokio<TcpStream>, StdRng>, Box<dyn std::error::Error>> {
    let stream = TcpStream::connect("localhost:9001").await?;

    let headers = &[Header {
        name: "Host",
        value: b"localhost:9001",
    }];

    let websocketz = WebSocket::connect::<16>(
        ConnectOptions::new_unchecked(path).with_headers(headers),
        FromTokio::new(stream),
        StdRng::try_from_rng(&mut SysRng).unwrap(),
        read_buf,
        write_buf,
        fragments_buf,
    )
    .await?;

    println!(
        "Number of framable bytes after handshake: {}",
        websocketz.framable()
    );

    Ok(websocketz)
}

async fn get_case_count() -> Result<u32, Box<dyn std::error::Error>> {
    let read_buf = &mut [0u8; 1024];
    let write_buf = &mut [0u8; 1024];
    let fragments_buf = &mut [0u8; 1024];

    let mut websocketz = connect("/getCaseCount", read_buf, write_buf, fragments_buf).await?;

    let count = match next!(websocketz)
        .transpose()?
        .ok_or("No message received")?
    {
        Message::Text(payload) => payload.parse::<u32>()?,

        _ => {
            return Err("Expected a text message".into());
        }
    };

    websocketz
        .send(Message::Close(Some(CloseFrame::no_reason(
            CloseCode::Normal,
        ))))
        .await?;

    Ok(count)
}

const SIZE: usize = 24 * 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let count = get_case_count().await?;

    for case in 1..=count {
        println!("Running case {case} of {count}");

        let mut read_buf = vec![0u8; SIZE];
        let mut write_buf = vec![0u8; SIZE];
        let mut fragments_buf = vec![0u8; SIZE];

        let mut websocketz = connect(
            &format!("/runCase?case={case}&agent=websocketz"),
            &mut read_buf,
            &mut write_buf,
            &mut fragments_buf,
        )
        .await?;

        while let Some(message) = next!(websocketz) {
            match message {
                Ok(message) => match message {
                    Message::Text(payload) => send!(websocketz, Message::Text(payload))?,
                    Message::Binary(payload) => {
                        // we can also fragment messages
                        send_fragmented!(websocketz, Message::Binary(payload), SIZE / 4)?
                    }
                    _ => {}
                },
                Err(err) => {
                    eprintln!("Error reading message: {err}");

                    send!(websocketz, Message::Close(None))?;

                    break;
                }
            }
        }
    }

    let read_buf = &mut [0u8; 1024];
    let write_buf = &mut [0u8; 1024];

    let mut websocketz = connect(
        "/updateReports?agent=websocketz",
        read_buf,
        write_buf,
        &mut [],
    )
    .await?;

    websocketz
        .send(Message::Close(Some(CloseFrame::no_reason(
            CloseCode::Normal,
        ))))
        .await?;

    Ok(())
}

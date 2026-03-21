use embedded_io_adapters::tokio_1::FromTokio;
use rand::{
    SeedableRng,
    rngs::{StdRng, SysRng},
};

use crate::{CloseCode, Message, WebSocket, next};

const SIZE: usize = 128;

// cSpell:disable
const BINARY_MESSAGES: &[&[u8]] = &[
    b"Hello, world!",
    b"Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
    b"Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium.",
    b"Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit.",
    b"Curabitur pretium tincidunt lacus. Nulla gravida orci a odio.",
    b"Aenean nec eros. Vestibulum ante ipsum primis in faucibus orci luctus et.",
    b"Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi.",
    b"Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu.",
    b"In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo.",
];

const STR_MESSAGES: &[&str] = &[
    "Hello, world!",
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
    "Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium.",
    "Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit.",
    "Curabitur pretium tincidunt lacus. Nulla gravida orci a odio.",
    "Aenean nec eros. Vestibulum ante ipsum primis in faucibus orci luctus et.",
    "Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi.",
    "Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu.",
    "In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo.",
];
// cSpell:enable

#[derive(Debug, thiserror::Error)]
#[error("Custom error")]
struct CustomError {}

mod macros {
    use tokio::io::{DuplexStream, ReadHalf, WriteHalf};

    use crate::{send, send_fragmented};

    use super::*;

    #[tokio::test]
    #[ignore = "Assert that macros compile with split websocketz"]
    async fn macros() {
        fn split(
            stream: FromTokio<DuplexStream>,
        ) -> (
            FromTokio<ReadHalf<DuplexStream>>,
            FromTokio<WriteHalf<DuplexStream>>,
        ) {
            let (read, write) = tokio::io::split(stream.into_inner());

            (FromTokio::new(read), FromTokio::new(write))
        }

        let (stream, _) = tokio::io::duplex(16);

        let read_buf = &mut [0u8; SIZE];
        let write_buf = &mut [0u8; SIZE];
        let fragments_buf = &mut [0u8; SIZE];

        let mut websocketz = WebSocket::client(
            FromTokio::new(stream),
            StdRng::try_from_rng(&mut SysRng).unwrap(),
            read_buf,
            write_buf,
            fragments_buf,
        );

        let _ = next!(websocketz);
        let _ = send!(websocketz, Message::Text("Message"));
        let _ = send_fragmented!(websocketz, Message::Text("Message"), 2);

        let (mut websocketz_read, mut websocketz_write) = websocketz.split_with(split);

        let _ = next!(websocketz_read);
        let _ = send!(websocketz_write, Message::Text("Message"));
        let _ = send_fragmented!(websocketz_write, Message::Text("Message"), 2);
    }
}

mod client {
    use crate::options::ConnectOptions;

    use super::*;

    #[tokio::test]
    async fn send() {
        let (client, server) = tokio::io::duplex(16);

        let read_buf = &mut [0u8; SIZE];
        let write_buf = &mut [0u8; SIZE];
        let fragments_buf = &mut [0u8; SIZE];

        let server = async move {
            let mut fastwebsockets =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);

            let mut bin_index = 0;
            let mut str_index = 0;

            loop {
                match fastwebsockets.read_frame().await {
                    Ok(frame) => match frame.opcode {
                        fastwebsockets::OpCode::Binary => {
                            assert_eq!(frame.payload, BINARY_MESSAGES[bin_index]);
                            bin_index += 1;
                        }
                        fastwebsockets::OpCode::Text => {
                            let text = core::str::from_utf8(&frame.payload).unwrap();
                            assert_eq!(text, STR_MESSAGES[str_index]);
                            str_index += 1;
                        }
                        _ => panic!("Unexpected frame opcode"),
                    },
                    Err(fastwebsockets::WebSocketError::UnexpectedEOF) => break,
                    _ => panic!("Unexpected frame"),
                }
            }
        };

        let client = async move {
            let mut websocketz = WebSocket::client(
                FromTokio::new(client),
                StdRng::try_from_rng(&mut SysRng).unwrap(),
                read_buf,
                write_buf,
                fragments_buf,
            );

            for binary in BINARY_MESSAGES {
                websocketz
                    .send(Message::Binary(binary))
                    .await
                    .expect("Failed to send binary message");
            }

            for text in STR_MESSAGES {
                websocketz
                    .send(Message::Text(text))
                    .await
                    .expect("Failed to send text message");
            }
        };

        tokio::join!(server, client);
    }

    #[tokio::test]
    async fn send_fragmented() {
        let (client, server) = tokio::io::duplex(16);

        let read_buf = &mut [0u8; SIZE];
        let write_buf = &mut [0u8; SIZE];
        let fragments_buf = &mut [0u8; SIZE];

        let server = async move {
            let mut fastwebsockets = fastwebsockets::FragmentCollector::new(
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server),
            );

            let mut bin_index = 0;
            let mut str_index = 0;

            loop {
                match fastwebsockets.read_frame().await {
                    Ok(frame) => match frame.opcode {
                        fastwebsockets::OpCode::Binary => {
                            assert_eq!(frame.payload, BINARY_MESSAGES[bin_index]);
                            bin_index += 1;
                        }
                        fastwebsockets::OpCode::Text => {
                            let text = core::str::from_utf8(&frame.payload).unwrap();
                            assert_eq!(text, STR_MESSAGES[str_index]);
                            str_index += 1;
                        }
                        _ => panic!("Unexpected frame opcode"),
                    },
                    Err(fastwebsockets::WebSocketError::UnexpectedEOF) => break,
                    _ => panic!("Unexpected frame"),
                }
            }
        };

        let client = async move {
            let mut websocketz = WebSocket::client(
                FromTokio::new(client),
                StdRng::try_from_rng(&mut SysRng).unwrap(),
                read_buf,
                write_buf,
                fragments_buf,
            );

            for binary in BINARY_MESSAGES {
                websocketz
                    .send_fragmented(Message::Binary(binary), 16)
                    .await
                    .expect("Failed to send binary message");
            }

            for text in STR_MESSAGES {
                websocketz
                    .send_fragmented(Message::Text(text), 16)
                    .await
                    .expect("Failed to send text message");
            }
        };

        tokio::join!(server, client);
    }

    #[tokio::test]
    async fn receive() {
        let (client, server) = tokio::io::duplex(16);

        let read_buf = &mut [0u8; SIZE];
        let write_buf = &mut [0u8; SIZE];
        let fragments_buf = &mut [0u8; SIZE];

        let server = async move {
            let mut fastwebsockets =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);

            for binary in BINARY_MESSAGES {
                fastwebsockets
                    .write_frame(fastwebsockets::Frame::binary(
                        fastwebsockets::Payload::Borrowed(binary),
                    ))
                    .await
                    .expect("Failed to send binary message");
            }

            for text in STR_MESSAGES {
                fastwebsockets
                    .write_frame(fastwebsockets::Frame::text(
                        fastwebsockets::Payload::Borrowed(text.as_bytes()),
                    ))
                    .await
                    .expect("Failed to send text message");
            }
        };

        let client = async move {
            let mut websocketz = WebSocket::client(
                FromTokio::new(client),
                StdRng::try_from_rng(&mut SysRng).unwrap(),
                read_buf,
                write_buf,
                fragments_buf,
            );

            let mut bin_index = 0;
            let mut str_index = 0;

            loop {
                match next!(websocketz) {
                    Some(Ok(Message::Binary(payload))) => {
                        assert_eq!(payload, BINARY_MESSAGES[bin_index]);
                        bin_index += 1;
                    }
                    Some(Ok(Message::Text(payload))) => {
                        assert_eq!(payload, STR_MESSAGES[str_index]);
                        str_index += 1;
                    }
                    None => break,
                    message => panic!("Unexpected message: {message:?}"),
                }
            }
        };

        tokio::join!(server, client);
    }

    mod handshake {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        use crate::{
            error::{Error, HandshakeError},
            http::Request,
            options::AcceptOptions,
        };

        use super::*;

        macro_rules! quick_handshake_error {
            ($response:ident, $error:ident) => {
                let (client, server) = tokio::io::duplex(16);
                let read_buf = &mut [0u8; SIZE * 2];
                let write_buf = &mut [0u8; SIZE * 2];
                let fragments_buf = &mut [];

                let server = async move {
                    let (mut read, mut write) = tokio::io::split(server);

                    tokio::join!(
                        async move {
                            let mut buffer = [0u8; 16];
                            loop {
                                let n = read
                                    .read(&mut buffer)
                                    .await
                                    .expect("Failed to read from client");
                                if n == 0 {
                                    break;
                                }
                            }
                        },
                        async move {
                            write
                                .write_all($response.as_bytes())
                                .await
                                .expect("Failed to write response");
                        }
                    )
                };

                let client = async move {
                    match WebSocket::connect::<16>(
                        ConnectOptions::default(),
                        FromTokio::new(client),
                        StdRng::try_from_rng(&mut SysRng).unwrap(),
                        read_buf,
                        write_buf,
                        fragments_buf,
                    )
                    .await
                    {
                        Ok(_) => panic!("Expected error, but got Ok"),
                        Err(error) => {
                            assert!(matches!(error, Error::Handshake(HandshakeError::$error)));
                        }
                    }
                };

                tokio::join!(server, client);
            };
        }

        #[tokio::test]
        async fn invalid_status_code() {
            const RESPONSE: &str = "HTTP/1.1 200 OK\r\n\
            Upgrade: websocket\r\n\
            Connection: upgrade\r\n\
            Sec-WebSocket-Accept: dGhlIHNhbXBsZSBub25jZQ==\r\n\
            \r\n";

            quick_handshake_error!(RESPONSE, InvalidStatusCode);
        }

        #[tokio::test]
        async fn invalid_upgrade_header() {
            const RESPONSE: &str = "HTTP/1.1 101 Switching Protocols\r\n\
            Upgrade: not-websocket\r\n\
            Connection: upgrade\r\n\
            Sec-WebSocket-Accept: dGhlIHNhbXBsZSBub25jZQ==\r\n\
            \r\n";

            quick_handshake_error!(RESPONSE, MissingOrInvalidUpgrade);
        }

        #[tokio::test]
        async fn invalid_connection_header() {
            const RESPONSE: &str = "HTTP/1.1 101 Switching Protocols\r\n\
            Upgrade: websocket\r\n\
            Connection: not-upgrade\r\n\
            Sec-WebSocket-Accept: dGhlIHNhbXBsZSBub25jZQ==\r\n\
            \r\n";

            quick_handshake_error!(RESPONSE, MissingOrInvalidConnection);
        }

        #[tokio::test]
        async fn missing_accept_header() {
            const RESPONSE: &str = "HTTP/1.1 101 Switching Protocols\r\n\
            Upgrade: websocket\r\n\
            Connection: upgrade\r\n\
            \r\n";

            quick_handshake_error!(RESPONSE, MissingOrInvalidAccept);
        }

        #[tokio::test]
        async fn invalid_accept_header() {
            const RESPONSE: &str = "HTTP/1.1 101 Switching Protocols\r\n\
            Upgrade: websocket\r\n\
            Connection: upgrade\r\n\
            Sec-WebSocket-Accept: dGhlIHNhbXBsZSBub25jZQ==\r\n\
            \r\n";

            quick_handshake_error!(RESPONSE, MissingOrInvalidAccept);
        }

        #[tokio::test]
        async fn connection_closed() {
            let (client, server) = tokio::io::duplex(16);

            // The server refuses the handshake and closes the connection.
            let server = async move {
                let read_buf = &mut [0u8; SIZE * 2];
                let write_buf = &mut [0u8; SIZE * 2];
                let fragments_buf = &mut [];

                let _ = WebSocket::accept_with(
                    AcceptOptions::default(),
                    FromTokio::new(server),
                    StdRng::try_from_rng(&mut SysRng).unwrap(),
                    read_buf,
                    write_buf,
                    fragments_buf,
                    |_: &Request<'_, 16>| Err::<(), _>(CustomError {}),
                )
                .await;
            };

            let client = async move {
                let read_buf = &mut [0u8; SIZE * 2];
                let write_buf = &mut [0u8; SIZE * 2];
                let fragments_buf = &mut [];

                match WebSocket::connect::<16>(
                    ConnectOptions::default(),
                    FromTokio::new(client),
                    StdRng::try_from_rng(&mut SysRng).unwrap(),
                    read_buf,
                    write_buf,
                    fragments_buf,
                )
                .await
                {
                    Ok(_) => panic!("Expected error, but got Ok"),
                    Err(error) => {
                        assert!(matches!(
                            error,
                            Error::Handshake(HandshakeError::ConnectionClosed)
                        ));
                    }
                }
            };

            tokio::join!(server, client);
        }

        #[tokio::test]
        async fn ok() {
            let (client, server) = tokio::io::duplex(16);

            // Handshake requires larger buffers than SIZE
            let read_buf = &mut [0u8; SIZE * 2];
            let write_buf = &mut [0u8; SIZE * 2];
            let fragments_buf = &mut [];

            let server = async move {
                let io = hyper_util::rt::TokioIo::new(server);
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(
                        io,
                        hyper::service::service_fn(|mut req| async move {
                            let (response, fut) =
                                fastwebsockets::upgrade::upgrade(&mut req).unwrap();

                            tokio::spawn(async move {
                                let mut ws = fut.await.unwrap();

                                ws.write_frame(fastwebsockets::Frame::close(1000, b"close"))
                                    .await
                                    .unwrap();
                            });

                            Ok::<_, fastwebsockets::WebSocketError>(response)
                        }),
                    )
                    .with_upgrades()
                    .await
                    .unwrap();
            };

            let client = async move {
                let mut websocketz = WebSocket::connect::<16>(
                    ConnectOptions::default(),
                    FromTokio::new(client),
                    StdRng::try_from_rng(&mut SysRng).unwrap(),
                    read_buf,
                    write_buf,
                    fragments_buf,
                )
                .await
                .unwrap()
                .with_auto_close(false)
                .with_auto_pong(false);

                match next!(websocketz) {
                    Some(Ok(Message::Close(Some(frame)))) => {
                        assert_eq!(frame.code(), CloseCode::Normal);
                        assert_eq!(frame.reason(), "close");
                    }
                    message => {
                        panic!("Unexpected message: {message:?}");
                    }
                }
            };

            tokio::join!(server, client);
        }
    }
}

mod server {
    use bytes::Bytes;
    use http::{
        Request,
        header::{CONNECTION, UPGRADE},
    };
    use http_body_util::Empty;
    use tokio::io::AsyncWriteExt;

    use crate::{
        CloseFrame,
        error::{Error, HandshakeError},
        options::AcceptOptions,
    };

    use super::*;

    struct SpawnExecutor;

    impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        fn execute(&self, fut: Fut) {
            tokio::task::spawn(fut);
        }
    }

    #[tokio::test]
    async fn send() {
        let (server, client) = tokio::io::duplex(16);

        let read_buf = &mut [0u8; SIZE];
        let write_buf = &mut [0u8; SIZE];
        let fragments_buf = &mut [0u8; SIZE];

        let server = async move {
            let mut websocketz = WebSocket::server(
                FromTokio::new(server),
                StdRng::try_from_rng(&mut SysRng).unwrap(),
                read_buf,
                write_buf,
                fragments_buf,
            );

            for binary in BINARY_MESSAGES {
                websocketz
                    .send(Message::Binary(binary))
                    .await
                    .expect("Failed to send binary message");
            }

            for text in STR_MESSAGES {
                websocketz
                    .send(Message::Text(text))
                    .await
                    .expect("Failed to send text message");
            }
        };

        let client = async move {
            let mut fastwebsockets =
                fastwebsockets::WebSocket::after_handshake(client, fastwebsockets::Role::Client);

            let mut bin_index = 0;
            let mut str_index = 0;

            loop {
                match fastwebsockets.read_frame().await {
                    Ok(frame) => match frame.opcode {
                        fastwebsockets::OpCode::Binary => {
                            assert_eq!(frame.payload, BINARY_MESSAGES[bin_index]);
                            bin_index += 1;
                        }
                        fastwebsockets::OpCode::Text => {
                            let text = core::str::from_utf8(&frame.payload).unwrap();
                            assert_eq!(text, STR_MESSAGES[str_index]);
                            str_index += 1;
                        }
                        _ => panic!("Unexpected frame opcode"),
                    },
                    Err(fastwebsockets::WebSocketError::UnexpectedEOF) => break,
                    _ => panic!("Unexpected frame"),
                }
            }
        };

        tokio::join!(server, client);
    }

    #[tokio::test]
    async fn send_fragmented() {
        let (server, client) = tokio::io::duplex(16);

        let read_buf = &mut [0u8; SIZE];
        let write_buf = &mut [0u8; SIZE];
        let fragments_buf = &mut [0u8; SIZE];

        let server = async move {
            let mut websocketz = WebSocket::server(
                FromTokio::new(server),
                StdRng::try_from_rng(&mut SysRng).unwrap(),
                read_buf,
                write_buf,
                fragments_buf,
            );

            for binary in BINARY_MESSAGES {
                websocketz
                    .send_fragmented(Message::Binary(binary), 16)
                    .await
                    .expect("Failed to send binary message");
            }

            for text in STR_MESSAGES {
                websocketz
                    .send_fragmented(Message::Text(text), 16)
                    .await
                    .expect("Failed to send text message");
            }
        };

        let client = async move {
            let mut fastwebsockets = fastwebsockets::FragmentCollector::new(
                fastwebsockets::WebSocket::after_handshake(client, fastwebsockets::Role::Client),
            );

            let mut bin_index = 0;
            let mut str_index = 0;

            loop {
                match fastwebsockets.read_frame().await {
                    Ok(frame) => match frame.opcode {
                        fastwebsockets::OpCode::Binary => {
                            assert_eq!(frame.payload, BINARY_MESSAGES[bin_index]);
                            bin_index += 1;
                        }
                        fastwebsockets::OpCode::Text => {
                            let text = core::str::from_utf8(&frame.payload).unwrap();
                            assert_eq!(text, STR_MESSAGES[str_index]);
                            str_index += 1;
                        }
                        _ => panic!("Unexpected frame opcode"),
                    },
                    Err(fastwebsockets::WebSocketError::UnexpectedEOF) => break,
                    _ => panic!("Unexpected frame"),
                }
            }
        };

        tokio::join!(server, client);
    }

    #[tokio::test]
    async fn receive() {
        let (server, client) = tokio::io::duplex(16);

        let read_buf = &mut [0u8; SIZE];
        let write_buf = &mut [0u8; SIZE];
        let fragments_buf = &mut [0u8; SIZE];

        let client = async move {
            let mut fastwebsockets =
                fastwebsockets::WebSocket::after_handshake(client, fastwebsockets::Role::Client);

            for binary in BINARY_MESSAGES {
                fastwebsockets
                    .write_frame(fastwebsockets::Frame::binary(
                        fastwebsockets::Payload::Borrowed(binary),
                    ))
                    .await
                    .expect("Failed to send binary message");
            }

            for text in STR_MESSAGES {
                fastwebsockets
                    .write_frame(fastwebsockets::Frame::text(
                        fastwebsockets::Payload::Borrowed(text.as_bytes()),
                    ))
                    .await
                    .expect("Failed to send text message");
            }
        };

        let server = async move {
            let mut websocketz = WebSocket::server(
                FromTokio::new(server),
                StdRng::try_from_rng(&mut SysRng).unwrap(),
                read_buf,
                write_buf,
                fragments_buf,
            );

            let mut bin_index = 0;
            let mut str_index = 0;

            loop {
                match next!(websocketz) {
                    Some(Ok(Message::Binary(payload))) => {
                        assert_eq!(payload, BINARY_MESSAGES[bin_index]);
                        bin_index += 1;
                    }
                    Some(Ok(Message::Text(payload))) => {
                        assert_eq!(payload, STR_MESSAGES[str_index]);
                        str_index += 1;
                    }
                    None => break,
                    message => panic!("Unexpected message: {message:?}"),
                }
            }
        };

        tokio::join!(server, client);
    }

    mod handshake {
        use super::*;

        macro_rules! quick_handshake_error {
            ($request:ident, $error:ident) => {
                let (server, mut client) = tokio::io::duplex(16);

                let read_buf = &mut [0u8; SIZE * 2];
                let write_buf = &mut [0u8; SIZE * 2];
                let fragments_buf = &mut [];

                let server = async move {
                    match WebSocket::accept::<16>(
                        AcceptOptions::default(),
                        FromTokio::new(server),
                        StdRng::try_from_rng(&mut SysRng).unwrap(),
                        read_buf,
                        write_buf,
                        fragments_buf,
                    )
                    .await
                    {
                        Ok(_) => panic!("Expected error, but got Ok"),
                        Err(error) => {
                            assert!(matches!(error, Error::Handshake(HandshakeError::$error)));
                        }
                    }
                };

                let client = async move {
                    client.write_all($request.as_bytes()).await.unwrap();
                };

                tokio::join!(server, client);
            };
        }

        #[tokio::test]
        async fn wrong_http_method() {
            const REQUEST: &str = "POST / HTTP/1.1\r\n\
            Host: localhost\r\n\
            Upgrade: websocket\r\n\
            Connection: upgrade\r\n\
            Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
            Sec-WebSocket-Version: 13\r\n\
            \r\n";

            quick_handshake_error!(REQUEST, WrongHttpMethod);
        }

        #[tokio::test]
        async fn wrong_http_version() {
            const REQUEST: &str = "GET / HTTP/1.0\r\n\
            Host: localhost\r\n\
            Upgrade: websocket\r\n\
            Connection: upgrade\r\n\
            Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
            Sec-WebSocket-Version: 13\r\n\
            \r\n";

            quick_handshake_error!(REQUEST, WrongHttpVersion);
        }

        #[tokio::test]
        async fn invalid_sec_version() {
            const REQUEST: &str = "GET / HTTP/1.1\r\n\
            Host: localhost\r\n\
            Upgrade: websocket\r\n\
            Connection: upgrade\r\n\
            Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
            Sec-WebSocket-Version: 12\r\n\
            \r\n";

            quick_handshake_error!(REQUEST, MissingOrInvalidSecVersion);
        }

        #[tokio::test]
        async fn missing_sec_key() {
            const REQUEST: &str = "GET / HTTP/1.1\r\n\
            Host: localhost\r\n\
            Upgrade: websocket\r\n\
            Connection: upgrade\r\n\
            Sec-WebSocket-Version: 13\r\n\
            \r\n";

            quick_handshake_error!(REQUEST, MissingSecKey);
        }

        #[tokio::test]
        async fn connection_closed() {
            let (_, server) = tokio::io::duplex(16);

            let read_buf = &mut [0u8; SIZE * 2];
            let write_buf = &mut [0u8; SIZE * 2];
            let fragments_buf = &mut [];

            match WebSocket::accept::<16>(
                AcceptOptions::default(),
                FromTokio::new(server),
                StdRng::try_from_rng(&mut SysRng).unwrap(),
                read_buf,
                write_buf,
                fragments_buf,
            )
            .await
            {
                Ok(_) => panic!("Expected error, but got Ok"),
                Err(error) => {
                    assert!(matches!(
                        error,
                        Error::Handshake(HandshakeError::ConnectionClosed)
                    ));
                }
            }
        }

        #[tokio::test]
        async fn ok() {
            let (server, client) = tokio::io::duplex(16);

            // Handshake requires larger buffers than SIZE
            let read_buf = &mut [0u8; SIZE * 2];
            let write_buf = &mut [0u8; SIZE * 2];
            let fragments_buf = &mut [];

            let server = async move {
                let mut websocketz = WebSocket::accept::<16>(
                    AcceptOptions::default(),
                    FromTokio::new(server),
                    StdRng::try_from_rng(&mut SysRng).unwrap(),
                    read_buf,
                    write_buf,
                    fragments_buf,
                )
                .await
                .unwrap();

                websocketz
                    .send(Message::Close(Some(CloseFrame::new(
                        CloseCode::Normal,
                        "close",
                    ))))
                    .await
                    .unwrap();

                websocketz.into_inner()
            };

            let client = async move {
                let req = Request::builder()
                    .method("GET")
                    .uri("/")
                    .header(UPGRADE, "websocket")
                    .header(CONNECTION, "upgrade")
                    .header(
                        "Sec-WebSocket-Key",
                        fastwebsockets::handshake::generate_key(),
                    )
                    .header("Sec-WebSocket-Version", "13")
                    .body(Empty::<Bytes>::new())
                    .unwrap();

                let (mut fastwebsockets, _) =
                    fastwebsockets::handshake::client(&SpawnExecutor, req, client)
                        .await
                        .unwrap();

                match fastwebsockets.read_frame().await {
                    Ok(frame) => match frame.opcode {
                        fastwebsockets::OpCode::Close => {
                            let payload: &[u8] = frame.payload.as_ref();
                            let code = u16::from_be_bytes([payload[0], payload[1]]);
                            let reason = core::str::from_utf8(&payload[2..]).unwrap();

                            assert_eq!(code, 1000);
                            assert_eq!(reason, "close");
                        }
                        _ => panic!("Unexpected frame opcode"),
                    },
                    Err(fastwebsockets::WebSocketError::UnexpectedEOF) => {}
                    _ => panic!("Unexpected frame"),
                }
            };

            // Keep io to prevent BrokenPipe error
            let (_io, _) = tokio::join!(server, client);
        }
    }
}

mod fragmentation {
    use crate::{
        CloseFrame,
        error::{Error, FragmentationError},
    };

    use super::*;

    #[tokio::test]
    async fn invalid_fragment_size() {
        let (client, _) = tokio::io::duplex(16);

        let read_buf = &mut [0u8; SIZE];
        let write_buf = &mut [0u8; SIZE];
        let fragments_buf = &mut [0u8; SIZE];

        let mut websocketz = WebSocket::client(
            FromTokio::new(client),
            StdRng::try_from_rng(&mut SysRng).unwrap(),
            read_buf,
            write_buf,
            fragments_buf,
        );

        match websocketz.send_fragmented(Message::Text("test"), 0).await {
            Ok(_) => panic!("Expected InvalidFragmentSize error, but got Ok"),
            Err(error) => {
                assert!(matches!(
                    error,
                    Error::Fragmentation(FragmentationError::InvalidFragmentSize)
                ));
            }
        }
    }

    #[tokio::test]
    async fn only_text_and_binary_can_be_fragmented() {
        let (client, _) = tokio::io::duplex(16);

        let read_buf = &mut [0u8; SIZE];
        let write_buf = &mut [0u8; SIZE];
        let fragments_buf = &mut [0u8; SIZE];

        let mut websocketz = WebSocket::client(
            FromTokio::new(client),
            StdRng::try_from_rng(&mut SysRng).unwrap(),
            read_buf,
            write_buf,
            fragments_buf,
        );

        match websocketz.send_fragmented(Message::Ping(b"ping"), 16).await {
            Ok(_) => panic!("Expected CanNotBeFragmented error, but got Ok"),
            Err(error) => {
                assert!(matches!(
                    error,
                    Error::Fragmentation(FragmentationError::CanNotBeFragmented)
                ));
            }
        }

        match websocketz.send_fragmented(Message::Pong(b"pong"), 16).await {
            Ok(_) => panic!("Expected CanNotBeFragmented error, but got Ok"),
            Err(error) => {
                assert!(matches!(
                    error,
                    Error::Fragmentation(FragmentationError::CanNotBeFragmented)
                ));
            }
        }

        match websocketz
            .send_fragmented(
                Message::Close(Some(CloseFrame::new(CloseCode::Normal, "close"))),
                16,
            )
            .await
        {
            Ok(_) => panic!("Expected CanNotBeFragmented error, but got Ok"),
            Err(error) => {
                assert!(matches!(
                    error,
                    Error::Fragmentation(FragmentationError::CanNotBeFragmented)
                ));
            }
        }
    }
}

mod auto {
    use crate::{
        CloseFrame,
        error::{Error, WriteError},
    };

    use super::*;

    #[tokio::test]
    async fn pong() {
        let (client, server) = tokio::io::duplex(16);

        let client = async move {
            let read_buf = &mut [0u8; SIZE];
            let write_buf = &mut [0u8; SIZE];
            let fragments_buf = &mut [0u8; SIZE];

            let mut websocketz = WebSocket::client(
                FromTokio::new(client),
                StdRng::try_from_rng(&mut SysRng).unwrap(),
                read_buf,
                write_buf,
                fragments_buf,
            );

            // Send a ping frame
            websocketz
                .send(Message::Ping(b"ping"))
                .await
                .expect("Failed to send ping message");

            // Expect a pong frame in response
            match next!(websocketz) {
                Some(Ok(Message::Pong(payload))) => {
                    assert_eq!(payload, b"ping");
                }
                message => panic!("Unexpected message: {message:?}"),
            }
        };

        let server = async move {
            let read_buf = &mut [0u8; SIZE];
            let write_buf = &mut [0u8; SIZE];
            let fragments_buf = &mut [0u8; SIZE];

            let mut websocketz = WebSocket::server(
                FromTokio::new(server),
                StdRng::try_from_rng(&mut SysRng).unwrap(),
                read_buf,
                write_buf,
                fragments_buf,
            );

            while next!(websocketz).is_some() {}
        };

        tokio::join!(server, client);
    }

    #[tokio::test]
    async fn close() {
        let (client, server) = tokio::io::duplex(16);

        let client = async move {
            let read_buf = &mut [0u8; SIZE];
            let write_buf = &mut [0u8; SIZE];
            let fragments_buf = &mut [0u8; SIZE];

            let mut websocketz = WebSocket::client(
                FromTokio::new(client),
                StdRng::try_from_rng(&mut SysRng).unwrap(),
                read_buf,
                write_buf,
                fragments_buf,
            );

            // Send a close frame
            websocketz
                .send(Message::Close(Some(CloseFrame::new(
                    CloseCode::Normal,
                    "close",
                ))))
                .await
                .expect("Failed to send close message");

            // Expect a close frame in response
            match next!(websocketz) {
                Some(Ok(Message::Close(Some(frame)))) => {
                    assert_eq!(frame.code(), CloseCode::Normal);
                    assert_eq!(frame.reason(), "close");
                }
                message => panic!("Unexpected message: {message:?}"),
            }

            // Ensure the connection is closed
            assert!(next!(websocketz).is_none());

            // Attempt to send another message after close should fail
            match websocketz.send(Message::Text("test")).await {
                Ok(_) => panic!("Expected error after close, but got Ok"),
                Err(error) => {
                    assert!(matches!(error, Error::Write(WriteError::ConnectionClosed)));
                }
            }
        };

        let server = async move {
            let read_buf = &mut [0u8; SIZE];
            let write_buf = &mut [0u8; SIZE];
            let fragments_buf = &mut [0u8; SIZE];

            let mut websocketz = WebSocket::server(
                FromTokio::new(server),
                StdRng::try_from_rng(&mut SysRng).unwrap(),
                read_buf,
                write_buf,
                fragments_buf,
            );

            while next!(websocketz).is_some() {}

            // Attempt to send another message after close should fail
            match websocketz.send(Message::Text("test")).await {
                Ok(_) => panic!("Expected error after close, but got Ok"),
                Err(error) => {
                    assert!(matches!(error, Error::Write(WriteError::ConnectionClosed)));
                }
            }
        };

        tokio::join!(server, client);
    }
}

mod protocol {
    use tokio::io::AsyncWriteExt;

    use crate::error::{Error, ProtocolError, ReadError};

    use super::*;

    macro_rules! quick_protocol_error {
        ($frame:ident, $error:ident) => {
            let (client, mut server) = tokio::io::duplex(16);

            let client = async move {
                let read_buf = &mut [0u8; SIZE];
                let write_buf = &mut [0u8; SIZE];
                let fragments_buf = &mut [0u8; SIZE];

                let mut websocketz = WebSocket::client(
                    FromTokio::new(client),
                    StdRng::try_from_rng(&mut SysRng).unwrap(),
                    read_buf,
                    write_buf,
                    fragments_buf,
                );

                match next!(websocketz) {
                    Some(Err(error)) => {
                        std::println!("Received error: {error:?}");
                        assert!(matches!(
                            error,
                            Error::Read(ReadError::Protocol(ProtocolError::$error))
                        ));
                    }
                    message => panic!("Unexpected message: {message:?}"),
                }
            };

            let server = async move {
                server.write_all($frame).await.unwrap();

                server
            };

            tokio::join!(client, server);
        };
    }

    #[tokio::test]
    async fn invalid_close_frame() {
        const FRAME: &[u8] = &[
            0x88, // FIN=1, RSV1-3=0, opcode=0x8 (Close)
            0x01, // MASK=0 (unmasked), payload length = 1
            0x37, // Single byte of payload (invalid)
        ];

        quick_protocol_error!(FRAME, InvalidCloseFrame);
    }

    #[tokio::test]
    async fn invalid_close_code() {
        const FRAME: &[u8] = &[
            0x88, // FIN + opcode=0x8 (Close)
            0x02, // Payload length = 2 (only status code, no reason)
            0x03, 0xED, // Status code: 1005 (not allowed)
        ];

        quick_protocol_error!(FRAME, InvalidCloseCode);
    }

    #[tokio::test]
    async fn invalid_utf8_close() {
        const FRAME: &[u8] = &[
            0x88, // FIN + opcode=0x8 (Close)
            0x03, // Payload length = 3 (2 bytes code + 1 byte invalid UTF-8)
            0x03, 0xE8, // Status code: 1000 (normal closure)
            0xFF, // Invalid UTF-8 byte
        ];

        quick_protocol_error!(FRAME, InvalidUTF8);
    }

    #[tokio::test]
    async fn invalid_utf8_text() {
        const FRAME: &[u8] = &[
            0x81, // FIN + opcode 0x1 (text)
            0x01, // payload length = 1
            0xFF, // invalid UTF-8 byte
        ];

        quick_protocol_error!(FRAME, InvalidUTF8);
    }

    #[tokio::test]
    async fn invalid_fragment() {
        const FRAMES: &[u8] = &[
            // Start a fragmented text frame
            0x01, // FIN = 0, opcode = 0x1 (Text, not final)
            0x01, // Payload length = 1
            0x41, // 'A'
            // Try to send a new Binary message while the previous fragment isn't finished
            0x82, // FIN = 1, opcode = 0x2 (Binary, complete)
            0x01, // Payload length = 1
            0x42, // 'B'
        ];

        quick_protocol_error!(FRAMES, InvalidFragment);
    }

    #[tokio::test]
    async fn invalid_continuation_frame() {
        // Continuation frame without a preceding fragmented message
        const FRAME: &[u8] = &[
            0x80, // FIN = 1, opcode = 0x0 (Continuation)
            0x01, // Payload length = 1
            0x41, // ASCII 'A'
        ];

        quick_protocol_error!(FRAME, InvalidContinuationFrame);
    }
}

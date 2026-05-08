use base64::{Engine as _, engine::general_purpose};
use embedded_io_async::{Read, Write};
use framez::Framed;
use httparse::Header;
use rand_core::Rng;

use sha1::{Digest, Sha1};

use crate::{
    CloseCode, CloseFrame, FramesCodec, Message, OpCode,
    error::{Error, HandshakeError, ProtocolError, ReadError, WriteError},
    frame::Frame,
    http::{
        HeaderExt, InRequestCodec, InResponseCodec, OutRequest, OutRequestCodec, OutResponse,
        OutResponseCodec, Request, Response,
    },
    options::{AcceptOptions, ConnectOptions},
};

#[derive(Debug)]
#[doc(hidden)]
pub struct FragmentsState<'buf> {
    fragmented: Option<Fragmented>,
    fragments_buffer: &'buf mut [u8],
}

impl<'buf> FragmentsState<'buf> {
    #[inline]
    pub(crate) const fn new(fragments_buffer: &'buf mut [u8]) -> Self {
        Self {
            fragmented: None,
            fragments_buffer,
        }
    }

    #[inline]
    pub(crate) const fn empty() -> Self {
        Self::new(&mut [])
    }
}

#[derive(Debug)]
struct Fragmented {
    opcode: OpCode,
    index: usize,
}

#[derive(Debug, Clone, Copy)]
struct Auto {
    /// Auto pong frame handling.
    pong: bool,
    /// Auto close frame handling.
    close: bool,
}

impl Auto {
    #[inline]
    const fn positive() -> Self {
        Self {
            pong: true,
            close: true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub struct ConnectionState {
    /// Must be set to `true` if the user sends a close frame or the other side sends a close frame.
    ///
    /// If the connection is closed, every write will return a [`WriteError::ConnectionClosed`].
    pub closed: bool,
    /// Auto handling of ping/pong and close frames.
    auto: Auto,
}

impl ConnectionState {
    #[inline]
    #[allow(clippy::new_without_default)]
    pub const fn new() -> Self {
        Self {
            closed: false,
            auto: Auto::positive(),
        }
    }
}

#[derive(Debug)]
#[doc(hidden)]
pub struct WebSocketCore<'buf, RW, RNG> {
    pub framed: Framed<'buf, FramesCodec<RNG>, RW>,
    pub fragments_state: FragmentsState<'buf>,
    pub state: ConnectionState,
}

impl<'buf, RW, RNG> WebSocketCore<'buf, RW, RNG> {
    #[inline]
    const fn from_framed(
        framed: Framed<'buf, FramesCodec<RNG>, RW>,
        fragments_state: FragmentsState<'buf>,
    ) -> Self {
        Self {
            framed,
            fragments_state,
            state: ConnectionState::new(),
        }
    }

    #[inline]
    pub(crate) const fn new_from_framed(
        framed: Framed<'buf, FramesCodec<RNG>, RW>,
        fragments_state: FragmentsState<'buf>,
    ) -> Self {
        Self::from_framed(framed, fragments_state)
    }

    #[inline]
    const fn new(
        inner: RW,
        rng: RNG,
        read_buffer: &'buf mut [u8],
        write_buffer: &'buf mut [u8],
        fragments_state: FragmentsState<'buf>,
    ) -> Self {
        Self::new_from_framed(
            Framed::new(FramesCodec::new(rng), inner, read_buffer, write_buffer),
            fragments_state,
        )
    }

    #[inline]
    pub(crate) const fn client(
        inner: RW,
        rng: RNG,
        read_buffer: &'buf mut [u8],
        write_buffer: &'buf mut [u8],
        fragments_state: FragmentsState<'buf>,
    ) -> Self {
        Self::new(inner, rng, read_buffer, write_buffer, fragments_state).into_client()
    }

    #[inline]
    pub(crate) const fn server(
        inner: RW,
        rng: RNG,
        read_buffer: &'buf mut [u8],
        write_buffer: &'buf mut [u8],
        fragments_state: FragmentsState<'buf>,
    ) -> Self {
        Self::new(inner, rng, read_buffer, write_buffer, fragments_state).into_server()
    }

    #[inline]
    const fn into_server(mut self) -> Self {
        self.framed.codec_mut().set_mask(false);
        self.framed.codec_mut().set_unmask(true);
        self
    }

    #[inline]
    const fn into_client(mut self) -> Self {
        self.framed.codec_mut().set_mask(true);
        self.framed.codec_mut().set_unmask(false);
        self
    }

    #[inline]
    pub(crate) const fn set_auto_pong(&mut self, auto_pong: bool) {
        self.state.auto.pong = auto_pong;
    }

    #[inline]
    pub(crate) const fn set_auto_close(&mut self, auto_close: bool) {
        self.state.auto.close = auto_close;
    }

    /// Returns reference to the reader/writer.
    #[inline]
    pub(crate) const fn inner(&self) -> &RW {
        self.framed.inner()
    }

    /// Returns mutable reference to the reader/writer.
    #[inline]
    pub(crate) const fn inner_mut(&mut self) -> &mut RW {
        self.framed.inner_mut()
    }

    /// Consumes the [`WebsocketsCore`] and returns the reader/writer.
    #[inline]
    pub(crate) fn into_inner(self) -> RW {
        self.framed.into_parts().1
    }

    /// Returns the number of bytes that can be framed.
    #[inline]
    pub(crate) const fn framable(&self) -> usize {
        self.framed.framable()
    }

    fn generate_sec_key(&mut self) -> [u8; 24]
    where
        RNG: Rng,
    {
        let mut key: [u8; 16] = [0; 16];

        debug_assert!(key.len() == 16, "Key should be 16 bytes long");

        self.framed.codec_mut().rng_mut().fill_bytes(&mut key);

        // 24 = ((4 * key.len() + 2) / 3 + 3) & !3 = ((4 * 16 + 2) / 3 + 3) & !3
        let mut encoded: [u8; 24] = [0; 24];

        general_purpose::STANDARD
            .encode_slice(key, &mut encoded)
            .expect("Bug: sec_key encoding failed");

        encoded
    }

    fn generate_sec_accept(sec_key: &[u8]) -> [u8; 28] {
        let mut sha1 = Sha1::new();

        sha1.update(sec_key);
        sha1.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");

        let hash = sha1.finalize();

        debug_assert!(hash.len() == 20, "SHA1 hash should be 20 bytes long");

        // 28 = ((4 * hash.len() + 2) / 3 + 3) & !3 = ((4 * 20 + 2) / 3 + 3) & !3
        let mut encoded: [u8; 28] = [0; 28];

        general_purpose::STANDARD
            .encode_slice(hash, &mut encoded)
            .expect("Bug: sec_accept encoding failed");

        encoded
    }

    pub(crate) async fn client_handshake<const N: usize, F, T, E>(
        mut self,
        options: ConnectOptions<'_, '_>,
        on_response: F,
    ) -> Result<(Self, T), Error<RW::Error, E>>
    where
        F: for<'a> Fn(&Response<'a, N>) -> Result<T, E>,
        RW: Read + Write,
        RNG: Rng,
    {
        let sec_key = self.generate_sec_key();

        let headers = &[
            Header {
                name: "upgrade",
                value: b"websocket",
            },
            Header {
                name: "connection",
                value: b"upgrade",
            },
            Header {
                name: "sec-websocket-version",
                value: b"13",
            },
            Header {
                name: "sec-websocket-key",
                value: &sec_key,
            },
        ];

        let request = OutRequest::get_unchecked(options.path, headers, options.headers);

        let (codec, inner, state) = self.framed.into_parts();

        let mut framed = Framed::from_parts(OutRequestCodec::new(), inner, state.reset());

        framed
            .send(request)
            .await
            .map_err(|err| Error::Write(WriteError::WriteHttp(err)))?;

        let (_, inner, state) = framed.into_parts();

        let mut framed = Framed::from_parts(InResponseCodec::<N>::new(), inner, state.reset());

        let custom = match framez::next!(framed) {
            None => {
                return Err(Error::Handshake(HandshakeError::ConnectionClosed));
            }
            Some(Err(err)) => {
                return Err(Error::Read(ReadError::ReadHttp(err)));
            }
            Some(Ok(response)) => {
                let custom = on_response(&response).map_err(HandshakeError::Other)?;

                if !matches!(response.code(), 101) {
                    return Err(Error::Handshake(HandshakeError::InvalidStatusCode));
                }

                if !response
                    .headers()
                    .header_value_str("upgrade")
                    .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
                {
                    return Err(Error::Handshake(HandshakeError::MissingOrInvalidUpgrade));
                }

                if !response
                    .headers()
                    .header_value_str("connection")
                    .is_some_and(|v| v.eq_ignore_ascii_case("upgrade"))
                {
                    return Err(Error::Handshake(HandshakeError::MissingOrInvalidConnection));
                }

                let sec_accept = Self::generate_sec_accept(&sec_key);

                if response
                    .headers()
                    .header_value("sec-websocket-accept")
                    .is_none_or(|v| v != sec_accept)
                {
                    return Err(Error::Handshake(HandshakeError::MissingOrInvalidAccept));
                }

                custom
            }
        };

        let (_, inner, state) = framed.into_parts();

        let framed = Framed::from_parts(codec, inner, state);

        Ok((Self::from_framed(framed, self.fragments_state), custom))
    }

    pub(crate) async fn server_handshake<const N: usize, F, T, E>(
        self,
        options: AcceptOptions<'_, '_>,
        on_request: F,
    ) -> Result<(Self, T), Error<RW::Error, E>>
    where
        F: for<'a> Fn(&Request<'a, N>) -> Result<T, E>,
        RW: Read + Write,
    {
        let (codec, inner, state) = self.framed.into_parts();

        let mut framed = Framed::from_parts(InRequestCodec::<N>::new(), inner, state);

        let (accept_key, custom) = match framez::next!(framed) {
            None => {
                return Err(Error::Handshake(HandshakeError::ConnectionClosed));
            }
            Some(Err(err)) => {
                return Err(Error::Read(ReadError::ReadHttp(err)));
            }
            Some(Ok(request)) => {
                let custom = on_request(&request).map_err(HandshakeError::Other)?;

                if !matches!(request.method(), "GET") {
                    return Err(Error::Handshake(HandshakeError::WrongHttpMethod));
                }

                // http version must be 1.1 or higher
                if request.version() < 1 {
                    return Err(Error::Handshake(HandshakeError::WrongHttpVersion));
                }

                if !request
                    .headers()
                    .header_value_str("sec-websocket-version")
                    .is_some_and(|v| v.eq_ignore_ascii_case("13"))
                {
                    return Err(Error::Handshake(HandshakeError::MissingOrInvalidSecVersion));
                }

                let sec_key = request
                    .headers()
                    .header_value("sec-websocket-key")
                    .ok_or(Error::Handshake(HandshakeError::MissingSecKey))?;

                (Self::generate_sec_accept(sec_key), custom)
            }
        };

        let headers = &[
            Header {
                name: "upgrade",
                value: b"websocket",
            },
            Header {
                name: "connection",
                value: b"upgrade",
            },
            Header {
                name: "sec-websocket-version",
                value: b"13",
            },
            Header {
                name: "sec-websocket-accept",
                value: &accept_key,
            },
        ];

        let response = OutResponse::switching_protocols(headers, options.headers);

        let (_, inner, state) = framed.into_parts();

        let mut framed = Framed::from_parts(OutResponseCodec::new(), inner, state);

        framed
            .send(response)
            .await
            .map_err(|err| Error::Write(WriteError::WriteHttp(err)))?;

        let (_, inner, state) = framed.into_parts();

        let framed = Framed::from_parts(codec, inner, state);

        Ok((Self::from_framed(framed, self.fragments_state), custom))
    }

    #[doc(hidden)]
    pub const fn auto(
        &self,
    ) -> impl FnOnce(Frame<'_>) -> Result<OnFrame<'_>, ProtocolError> + 'static {
        let state = self.state;

        move |frame| {
            if state.auto.pong && frame.opcode() == OpCode::Ping {
                return Ok(OnFrame::Send(Message::Pong(frame.payload())));
            }

            if state.auto.close && frame.opcode() == OpCode::Close && !state.closed {
                let close_frame = match Self::extract_close_frame(&frame) {
                    Ok(close_frame) => close_frame,
                    Err(err) => return Err(err),
                };

                match close_frame {
                    Some(frame) => {
                        return Ok(OnFrame::Send(Message::Close(Some(frame))));
                    }
                    None => {
                        return Ok(OnFrame::Send(Message::Close(Some(CloseFrame::no_reason(
                            CloseCode::Normal,
                        )))));
                    }
                }
            }

            Ok(OnFrame::Noop(frame))
        }
    }

    fn extract_close_frame<'this>(
        frame: &Frame<'this>,
    ) -> Result<Option<CloseFrame<'this>>, ProtocolError> {
        let payload = frame.payload();

        match payload.len() {
            0 => {}
            1 => {
                return Err(ProtocolError::InvalidCloseFrame);
            }
            _ => {
                let code = CloseCode::from_u16(u16::from_be_bytes([payload[0], payload[1]]));

                if !code.is_allowed() {
                    return Err(ProtocolError::InvalidCloseCode);
                }

                match core::str::from_utf8(&payload[2..]) {
                    Ok(reason) => {
                        return Ok(Some(CloseFrame::new(code, reason)));
                    }
                    Err(_) => {
                        return Err(ProtocolError::InvalidUTF8);
                    }
                }
            }
        }

        Ok(None)
    }

    pub(crate) fn on_frame<'this>(
        fragments_state: &'this mut FragmentsState<'_>,
        frame: Frame<'this>,
    ) -> Option<Result<Option<Message<'this>>, OnFrameError>> {
        match frame.opcode() {
            OpCode::Text | OpCode::Binary => {
                if frame.is_final() {
                    if fragments_state.fragmented.is_some() {
                        return Some(Err(OnFrameError::Protocol(ProtocolError::InvalidFragment)));
                    }

                    match frame.opcode() {
                        OpCode::Binary => {
                            return Some(Ok(Some(Message::Binary(frame.payload()))));
                        }
                        OpCode::Text => match core::str::from_utf8(frame.payload()) {
                            Ok(text) => {
                                return Some(Ok(Some(Message::Text(text))));
                            }
                            Err(_) => {
                                return Some(Err(OnFrameError::Protocol(
                                    ProtocolError::InvalidUTF8,
                                )));
                            }
                        },
                        _ => unreachable!("Already matched for OpCode::Text | OpCode::Binary"),
                    }
                }

                if frame.payload().len() > fragments_state.fragments_buffer.len() {
                    return Some(Err(OnFrameError::FragmentsBufferTooSmall));
                }

                fragments_state.fragments_buffer[..frame.payload().len()]
                    .copy_from_slice(frame.payload());

                fragments_state.fragmented = Some(Fragmented {
                    opcode: frame.opcode(),
                    index: frame.payload().len(),
                });
            }
            OpCode::Continuation => {
                let message = match fragments_state.fragmented.as_mut() {
                    None => {
                        return Some(Err(OnFrameError::Protocol(
                            ProtocolError::InvalidContinuationFrame,
                        )));
                    }
                    Some(fragmented) => {
                        if fragmented.index + frame.payload().len()
                            > fragments_state.fragments_buffer.len()
                        {
                            return Some(Err(OnFrameError::FragmentsBufferTooSmall));
                        }

                        fragments_state.fragments_buffer[fragmented.index..]
                            [..frame.payload().len()]
                            .copy_from_slice(frame.payload());

                        fragmented.index += frame.payload().len();

                        if frame.is_final() {
                            match fragmented.opcode {
                                OpCode::Text => {
                                    match core::str::from_utf8(
                                        &fragments_state.fragments_buffer[..fragmented.index],
                                    ) {
                                        Ok(text) => Some(Message::Text(text)),
                                        Err(_) => {
                                            return Some(Err(OnFrameError::Protocol(
                                                ProtocolError::InvalidUTF8,
                                            )));
                                        }
                                    }
                                }
                                OpCode::Binary => Some(Message::Binary(
                                    &fragments_state.fragments_buffer[..fragmented.index],
                                )),
                                _ => unreachable!(
                                    "Opcode can only be set to OpCode::Text | OpCode::Binary in the first match branch"
                                ),
                            }
                        } else {
                            None
                        }
                    }
                };

                if let Some(message) = message {
                    fragments_state.fragmented = None;

                    return Some(Ok(Some(message)));
                }
            }
            OpCode::Close => {
                let close_frame = match Self::extract_close_frame(&frame) {
                    Ok(close_frame) => close_frame,
                    Err(err) => return Some(Err(OnFrameError::Protocol(err))),
                };

                return Some(Ok(Some(Message::Close(close_frame))));
            }
            OpCode::Ping => {
                return Some(Ok(Some(Message::Ping(frame.payload()))));
            }
            OpCode::Pong => {
                return Some(Ok(Some(Message::Pong(frame.payload()))));
            }
        }

        Some(Ok(None))
    }

    pub(crate) async fn send(&mut self, message: Message<'_>) -> Result<(), Error<RW::Error>>
    where
        RW: Write,
        RNG: Rng,
    {
        crate::functions::send(
            &mut self.framed.core.codec,
            &mut self.framed.core.inner,
            &mut self.framed.core.state.write,
            &mut self.state,
            message,
        )
        .await
    }

    pub(crate) async fn send_fragmented(
        &mut self,
        message: Message<'_>,
        fragment_size: usize,
    ) -> Result<(), Error<RW::Error>>
    where
        RW: Write,
        RNG: Rng,
    {
        crate::functions::send_fragmented(
            &mut self.framed.core.codec,
            &mut self.framed.core.inner,
            &mut self.framed.core.state.write,
            &mut self.state,
            message,
            fragment_size,
        )
        .await
    }
}

#[derive(Debug)]
#[doc(hidden)]
pub enum OnFrame<'a> {
    Send(Message<'a>),
    Noop(Frame<'a>),
}

#[derive(Debug)]
#[doc(hidden)]
pub enum OnFrameError {
    Protocol(ProtocolError),
    FragmentsBufferTooSmall,
}

impl<I> From<OnFrameError> for Error<I> {
    fn from(err: OnFrameError) -> Self {
        match err {
            OnFrameError::Protocol(err) => Error::Read(ReadError::Protocol(err)),
            OnFrameError::FragmentsBufferTooSmall => {
                Error::Read(ReadError::FragmentsBufferTooSmall)
            }
        }
    }
}

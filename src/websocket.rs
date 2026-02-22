use core::cell::RefCell;

use embedded_io_async::{Read, Write};
use framez::{
    Framed,
    state::{ReadState, ReadWriteState, WriteState},
};
use rand::RngCore;

use crate::{
    FragmentsState, Frame, FramesCodec, Message, OnFrame, OwnedMessage, WebSocketCore,
    error::{Error, ProtocolError},
    http::{Request, Response},
    next,
    options::{AcceptOptions, ConnectOptions},
};

/// A WebSocket connection.
///
/// # Defaults:
///
/// - `auto_pong`: `true`
/// - `auto_close`: `true`
#[derive(Debug)]
pub struct WebSocket<'buf, RW, Rng> {
    #[doc(hidden)]
    pub core: WebSocketCore<'buf, RW, Rng>,
}

impl<'buf, RW, Rng> WebSocket<'buf, RW, Rng> {
    /// Creates a new [`WebSocket`] client after a successful handshake.
    pub const fn client(
        inner: RW,
        rng: Rng,
        read_buffer: &'buf mut [u8],
        write_buffer: &'buf mut [u8],
        fragments_buffer: &'buf mut [u8],
    ) -> Self {
        Self {
            core: WebSocketCore::client(
                inner,
                rng,
                read_buffer,
                write_buffer,
                FragmentsState::new(fragments_buffer),
            ),
        }
    }

    /// Creates a new [`WebSocket`] server after a successful handshake.
    pub const fn server(
        inner: RW,
        rng: Rng,
        read_buffer: &'buf mut [u8],
        write_buffer: &'buf mut [u8],
        fragments_buffer: &'buf mut [u8],
    ) -> Self {
        Self {
            core: WebSocketCore::server(
                inner,
                rng,
                read_buffer,
                write_buffer,
                FragmentsState::new(fragments_buffer),
            ),
        }
    }

    /// Creates a new [`WebSocket`] client and performs the handshake.
    ///
    /// # Generic Parameters
    /// `N`: The maximum number of headers to accept in the handshake response.
    pub async fn connect<const N: usize>(
        options: ConnectOptions<'_, '_>,
        inner: RW,
        rng: Rng,
        read_buffer: &'buf mut [u8],
        write_buffer: &'buf mut [u8],
        fragments_buffer: &'buf mut [u8],
    ) -> Result<Self, Error<RW::Error>>
    where
        RW: Read + Write,
        Rng: RngCore,
    {
        Ok(Self::connect_with::<N, _, _, _>(
            options,
            inner,
            rng,
            read_buffer,
            write_buffer,
            fragments_buffer,
            |_| Ok(()),
        )
        .await?
        .0)
    }

    /// Creates a new [`WebSocket`] client and performs the handshake with a custom response handler.
    ///
    /// # Generic Parameters
    /// `N`: The maximum number of headers to accept in the handshake response.
    pub async fn connect_with<const N: usize, F, T, E>(
        options: ConnectOptions<'_, '_>,
        inner: RW,
        rng: Rng,
        read_buffer: &'buf mut [u8],
        write_buffer: &'buf mut [u8],
        fragments_buffer: &'buf mut [u8],
        on_response: F,
    ) -> Result<(Self, T), Error<RW::Error, E>>
    where
        F: for<'a> Fn(&Response<'a, N>) -> Result<T, E>,
        RW: Read + Write,
        Rng: RngCore,
    {
        Self::client(inner, rng, read_buffer, write_buffer, fragments_buffer)
            .client_handshake::<N, _, _, _>(options, on_response)
            .await
    }

    /// Creates a new [`WebSocket`] server and performs the handshake.
    ///
    /// # Generic Parameters
    /// `N`: The maximum number of headers to accept in the handshake request.
    pub async fn accept<const N: usize>(
        options: AcceptOptions<'_, '_>,
        inner: RW,
        rng: Rng,
        read_buffer: &'buf mut [u8],
        write_buffer: &'buf mut [u8],
        fragments_buffer: &'buf mut [u8],
    ) -> Result<Self, Error<RW::Error>>
    where
        RW: Read + Write,
    {
        Ok(Self::accept_with::<N, _, _, _>(
            options,
            inner,
            rng,
            read_buffer,
            write_buffer,
            fragments_buffer,
            |_| Ok(()),
        )
        .await?
        .0)
    }

    /// Creates a new [`WebSocket`] server and performs the handshake with a custom request handler.
    ///
    /// # Generic Parameters
    /// `N`: The maximum number of headers to accept in the handshake request.
    pub async fn accept_with<const N: usize, F, T, E>(
        options: AcceptOptions<'_, '_>,
        inner: RW,
        rng: Rng,
        read_buffer: &'buf mut [u8],
        write_buffer: &'buf mut [u8],
        fragments_buffer: &'buf mut [u8],
        on_request: F,
    ) -> Result<(Self, T), Error<RW::Error, E>>
    where
        F: for<'a> Fn(&Request<'a, N>) -> Result<T, E>,
        RW: Read + Write,
    {
        Self::server(inner, rng, read_buffer, write_buffer, fragments_buffer)
            .server_handshake::<N, _, _, _>(options, on_request)
            .await
    }

    /// Sets whether to automatically send a Pong response.
    #[inline]
    pub const fn with_auto_pong(mut self, auto_pong: bool) -> Self {
        self.core.set_auto_pong(auto_pong);
        self
    }

    /// Sets whether to automatically close the connection on receiving a Close frame.
    #[inline]
    pub const fn with_auto_close(mut self, auto_close: bool) -> Self {
        self.core.set_auto_close(auto_close);
        self
    }

    /// Returns reference to the reader/writer.
    #[inline]
    pub const fn inner(&self) -> &RW {
        self.core.inner()
    }

    /// Returns mutable reference to the reader/writer.
    #[inline]
    pub const fn inner_mut(&mut self) -> &mut RW {
        self.core.inner_mut()
    }

    /// Consumes the [`WebSocket`] and returns the reader/writer.
    #[inline]
    pub fn into_inner(self) -> RW {
        self.core.into_inner()
    }

    /// Returns the number of bytes that can be framed.
    #[inline]
    pub const fn framable(&self) -> usize {
        self.core.framable()
    }

    async fn client_handshake<const N: usize, F, T, E>(
        self,
        options: ConnectOptions<'_, '_>,
        on_response: F,
    ) -> Result<(Self, T), Error<RW::Error, E>>
    where
        F: for<'a> Fn(&Response<'a, N>) -> Result<T, E>,
        RW: Read + Write,
        Rng: RngCore,
    {
        let (core, custom) = self
            .core
            .client_handshake::<N, _, _, _>(options, on_response)
            .await?;

        Ok((Self { core }, custom))
    }

    async fn server_handshake<const N: usize, F, T, E>(
        self,
        options: AcceptOptions<'_, '_>,
        on_request: F,
    ) -> Result<(Self, T), Error<RW::Error, E>>
    where
        F: for<'a> Fn(&Request<'a, N>) -> Result<T, E>,
        RW: Read + Write,
    {
        let (core, custom) = self
            .core
            .server_handshake::<N, _, _, _>(options, on_request)
            .await?;

        Ok((Self { core }, custom))
    }

    /// Sends a WebSocket message.
    pub async fn send(&mut self, message: Message<'_>) -> Result<(), Error<RW::Error>>
    where
        RW: Write,
        Rng: RngCore,
    {
        self.core.send(message).await
    }

    /// Sends a fragmented WebSocket message.
    pub async fn send_fragmented(
        &mut self,
        message: Message<'_>,
        fragment_size: usize,
    ) -> Result<(), Error<RW::Error>>
    where
        RW: Write,
        Rng: RngCore,
    {
        self.core.send_fragmented(message, fragment_size).await
    }

    /// Splits the [`WebSocket`] into a [`WebSocketRead`] and a [`WebSocketWrite`] with the provided `split` function.
    ///
    /// # Note
    ///
    /// `auto_pong` and `auto_close` will `NOT` be applied to the split instances.
    pub fn split_with<F, R, W>(
        self,
        split: F,
    ) -> (WebSocketRead<'buf, R>, WebSocketWrite<'buf, W, Rng>)
    where
        F: FnOnce(RW) -> (R, W),
    {
        let (codec, inner, state) = self.core.framed.into_parts();
        let (read_codec, write_codec) = codec.split();

        let (read, write) = split(inner);

        let framed_read = Framed::from_parts(
            read_codec,
            read,
            ReadWriteState::new(state.read, WriteState::empty()),
        );

        let framed_write = Framed::from_parts(
            write_codec,
            write,
            ReadWriteState::new(ReadState::empty(), state.write),
        );

        (
            WebSocketRead::new_from_framed(framed_read, self.core.fragments_state),
            WebSocketWrite::new_from_framed(framed_write),
        )
    }

    #[doc(hidden)]
    pub const fn auto(
        &self,
    ) -> impl FnOnce(Frame<'_>) -> Result<OnFrame<'_>, ProtocolError> + 'static {
        self.core.auto()
    }

    #[doc(hidden)]
    pub const fn caller(&self) -> crate::functions::ReadAutoCaller {
        crate::functions::ReadAutoCaller
    }
}

/// Read half of a WebSocket connection.
#[derive(Debug)]
pub struct WebSocketRead<'buf, RW> {
    #[doc(hidden)]
    pub core: WebSocketCore<'buf, RW, ()>,
}

impl<'buf, RW> WebSocketRead<'buf, RW> {
    const fn new_from_framed(
        framed: Framed<'buf, FramesCodec<()>, RW>,
        fragments_state: FragmentsState<'buf>,
    ) -> Self {
        Self {
            core: WebSocketCore::new_from_framed(framed, fragments_state),
        }
    }

    /// Creates a new [`WebSocketRead`] client after a successful handshake.
    pub const fn client(
        inner: RW,
        read_buffer: &'buf mut [u8],
        fragments_buffer: &'buf mut [u8],
    ) -> Self {
        Self {
            core: WebSocketCore::client(
                inner,
                (),
                read_buffer,
                &mut [],
                FragmentsState::new(fragments_buffer),
            ),
        }
    }

    /// Creates a new [`WebSocketRead`] server after a successful handshake.
    pub const fn server(
        inner: RW,
        read_buffer: &'buf mut [u8],
        fragments_buffer: &'buf mut [u8],
    ) -> Self {
        Self {
            core: WebSocketCore::server(
                inner,
                (),
                read_buffer,
                &mut [],
                FragmentsState::new(fragments_buffer),
            ),
        }
    }

    /// Returns reference to the reader.
    #[inline]
    pub const fn inner(&self) -> &RW {
        self.core.inner()
    }

    /// Returns mutable reference to the reader.
    #[inline]
    pub const fn inner_mut(&mut self) -> &mut RW {
        self.core.inner_mut()
    }

    /// Consumes the [`WebSocketRead`] and returns the reader.
    #[inline]
    pub fn into_inner(self) -> RW {
        self.core.into_inner()
    }

    /// Returns the number of bytes that can be framed.
    #[inline]
    pub const fn framable(&self) -> usize {
        self.core.framable()
    }

    #[doc(hidden)]
    pub const fn auto(&self) {}

    #[doc(hidden)]
    pub const fn caller(&self) -> crate::functions::ReadCaller {
        crate::functions::ReadCaller
    }
}

/// Write half of a WebSocket connection.
#[derive(Debug)]
pub struct WebSocketWrite<'buf, RW, Rng> {
    #[doc(hidden)]
    pub core: WebSocketCore<'buf, RW, Rng>,
}

impl<'buf, RW, Rng> WebSocketWrite<'buf, RW, Rng> {
    const fn new_from_framed(framed: Framed<'buf, FramesCodec<Rng>, RW>) -> Self {
        Self {
            core: WebSocketCore::new_from_framed(framed, FragmentsState::empty()),
        }
    }

    /// Creates a new [`WebSocketWrite`] client after a successful handshake.
    pub const fn client(inner: RW, rng: Rng, write_buffer: &'buf mut [u8]) -> Self {
        Self {
            core: WebSocketCore::client(inner, rng, &mut [], write_buffer, FragmentsState::empty()),
        }
    }

    /// Creates a new [`WebSocketWrite`] server after a successful handshake.
    pub const fn server(inner: RW, rng: Rng, write_buffer: &'buf mut [u8]) -> Self {
        Self {
            core: WebSocketCore::server(inner, rng, &mut [], write_buffer, FragmentsState::empty()),
        }
    }

    /// Returns reference to the writer.
    #[inline]
    pub const fn inner(&self) -> &RW {
        self.core.inner()
    }

    /// Returns mutable reference to the writer.
    #[inline]
    pub const fn inner_mut(&mut self) -> &mut RW {
        self.core.inner_mut()
    }

    /// Consumes the [`WebSocketWrite`] and returns the writer.
    #[inline]
    pub fn into_inner(self) -> RW {
        self.core.into_inner()
    }

    /// Sends a WebSocket message.
    pub async fn send(&mut self, message: Message<'_>) -> Result<(), Error<RW::Error>>
    where
        RW: Write,
        Rng: RngCore,
    {
        self.core.send(message).await
    }

    /// Sends a fragmented WebSocket message.
    pub async fn send_fragmented(
        &mut self,
        message: Message<'_>,
        fragment_size: usize,
    ) -> Result<(), Error<RW::Error>>
    where
        RW: Write,
        Rng: RngCore,
    {
        self.core.send_fragmented(message, fragment_size).await
    }
}

#[derive(Debug)]
pub struct OwnedWebSocket<'buf, const N: usize, RW, Rng> {
    // XXX: the idea of the refcell is to have a stream and a sink at the same time using the same core
    // but this would panic when using select on the stream and then using the sink to send a message, because the stream would have a mutable reference to the core and then the sink would try to borrow it again.
    // See clippy warning
    // I think we can only have a stream or a sink at a time, mutably borrowing the core
    core: RefCell<WebSocket<'buf, RW, Rng>>,
}

impl<'buf, const N: usize, RW, Rng> OwnedWebSocket<'buf, N, RW, Rng>
where
    RW: Read + Write,
    Rng: RngCore,
{
    pub async fn next(
        &self,
    ) -> Option<Result<Result<OwnedMessage<N>, heapless::CapacityError>, Error<RW::Error>>> {
        let mut websocket = self.core.borrow_mut();

        match 'next: loop {
            match websocket
                .caller()
                .call_next::<N, _, _, _>(websocket.auto(), &mut websocket.core)
                .await
            {
                Some(Ok(None)) => continue 'next,
                Some(Ok(Some(item))) => break 'next Some(Ok(item)),
                Some(Err(err)) => break 'next Some(Err(err)),
                None => break 'next None,
            }
        } {
            None => None,
            Some(Ok(msg)) => Some(Ok(msg)),
            Some(Err(err)) => Some(Err(err)),
        }
    }
}

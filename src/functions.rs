use embedded_io_async::{Read, Write};
use framez::state::{ReadState, WriteState};
use rand::RngCore;

use crate::{
    ConnectionState, Frame, Message, OnFrame, OwnedMessage, WebSocketCore,
    codec::FramesCodec,
    error::{Error, ProtocolError, ReadError, WriteError},
    websocket_core::FragmentsState,
};

#[derive(Debug)]
pub struct ReadAutoCaller;

impl ReadAutoCaller {
    #[allow(clippy::too_many_arguments)]
    pub async fn call<'this, F, RW, Rng>(
        &self,
        auto: F,
        codec: &mut FramesCodec<Rng>,
        inner: &mut RW,
        read_state: &'this mut ReadState<'_>,
        write_state: &mut WriteState<'_>,
        fragments_state: &'this mut FragmentsState<'_>,
        state: &mut ConnectionState,
    ) -> Option<Result<Option<Message<'this>>, Error<RW::Error>>>
    where
        RW: Read + Write,
        Rng: RngCore,
        F: FnOnce(Frame<'_>) -> Result<OnFrame<'_>, ProtocolError> + 'static,
    {
        let frame = match framez::functions::maybe_next(read_state, codec, inner).await {
            Some(Ok(Some(frame))) => frame,
            Some(Ok(None)) => return Some(Ok(None)),
            Some(Err(err)) => return Some(Err(Error::Read(ReadError::ReadFrame(err)))),
            None => return None,
        };

        let frame = match auto(frame) {
            Ok(on_frame) => match on_frame {
                OnFrame::Send(message) => {
                    state.closed = message.is_close();

                    match framez::functions::send(write_state, codec, inner, message).await {
                        Ok(_) => match state.closed {
                            false => return Some(Ok(None)),
                            true => return None,
                        },
                        Err(err) => return Some(Err(Error::Write(WriteError::WriteFrame(err)))),
                    }
                }
                OnFrame::Noop(frame) => frame,
            },
            Err(err) => return Some(Err(Error::Read(ReadError::Protocol(err)))),
        };

        WebSocketCore::<RW, Rng>::on_frame(fragments_state, frame)
            .map(|result| result.map_err(Error::from))
    }

    // TODO: see comments in on WebSocket::next()
    // TODO: delete
    #[allow(clippy::too_many_arguments)]
    pub async fn call_next<const N: usize, F, RW, Rng>(
        &self,
        auto: F,
        websocket: &mut WebSocketCore<'_, RW, Rng>,
    ) -> Option<Result<Option<Result<OwnedMessage<N>, heapless::CapacityError>>, Error<RW::Error>>>
    where
        RW: Read + Write,
        Rng: RngCore,
        F: FnOnce(Frame<'_>) -> Result<OnFrame<'_>, ProtocolError> + 'static,
    {
        let frame = match framez::functions::maybe_next(
            &mut websocket.framed.core.state.read,
            &mut websocket.framed.core.codec,
            &mut websocket.framed.core.inner,
        )
        .await
        {
            Some(Ok(Some(frame))) => frame,
            Some(Ok(None)) => return Some(Ok(None)),
            Some(Err(err)) => return Some(Err(Error::Read(ReadError::ReadFrame(err)))),
            None => return None,
        };

        let frame = match auto(frame) {
            Ok(on_frame) => match on_frame {
                OnFrame::Send(message) => {
                    websocket.state.closed = message.is_close();

                    match framez::functions::send(
                        &mut websocket.framed.core.state.write,
                        &mut websocket.framed.core.codec,
                        &mut websocket.framed.core.inner,
                        message,
                    )
                    .await
                    {
                        Ok(_) => match websocket.state.closed {
                            false => return Some(Ok(None)),
                            true => return None,
                        },
                        Err(err) => return Some(Err(Error::Write(WriteError::WriteFrame(err)))),
                    }
                }
                OnFrame::Noop(frame) => frame,
            },
            Err(err) => return Some(Err(Error::Read(ReadError::Protocol(err)))),
        };

        WebSocketCore::<RW, Rng>::on_frame(&mut websocket.fragments_state, frame).map(|result| {
            result
                .map(|opt| opt.map(OwnedMessage::<N>::try_from))
                .map_err(Error::from)
        })
    }
}

#[derive(Debug)]
pub struct ReadCaller;

impl ReadCaller {
    #[allow(clippy::too_many_arguments)]
    pub async fn call<'this, RW, Rng>(
        &self,
        _auto: (),
        codec: &mut FramesCodec<Rng>,
        inner: &mut RW,
        read_state: &'this mut ReadState<'_>,
        _write_state: &mut WriteState<'_>,
        fragments_state: &'this mut FragmentsState<'_>,
        _state: &mut ConnectionState,
    ) -> Option<Result<Option<Message<'this>>, Error<RW::Error>>>
    where
        RW: Read,
    {
        let frame = match framez::functions::maybe_next(read_state, codec, inner).await {
            Some(Ok(Some(frame))) => frame,
            Some(Ok(None)) => return Some(Ok(None)),
            Some(Err(err)) => return Some(Err(Error::Read(ReadError::ReadFrame(err)))),
            None => return None,
        };

        WebSocketCore::<RW, Rng>::on_frame(fragments_state, frame)
            .map(|result| result.map_err(Error::from))
    }
}

pub async fn send<RW, Rng>(
    codec: &mut FramesCodec<Rng>,
    inner: &mut RW,
    write_state: &mut WriteState<'_>,
    state: &mut ConnectionState,
    message: Message<'_>,
) -> Result<(), Error<RW::Error>>
where
    RW: Write,
    Rng: RngCore,
{
    if state.closed {
        return Err(Error::Write(WriteError::ConnectionClosed));
    }

    state.closed = message.is_close();

    framez::functions::send(write_state, codec, inner, message)
        .await
        .map_err(|err| Error::Write(WriteError::WriteFrame(err)))?;

    Ok(())
}

pub async fn send_fragmented<RW, Rng>(
    codec: &mut FramesCodec<Rng>,
    inner: &mut RW,
    write_state: &mut WriteState<'_>,
    state: &mut ConnectionState,
    message: Message<'_>,
    fragment_size: usize,
) -> Result<(), Error<RW::Error>>
where
    RW: Write,
    Rng: RngCore,
{
    if state.closed {
        return Err(Error::Write(WriteError::ConnectionClosed));
    }

    for frame in message
        .fragments(fragment_size)
        .map_err(Error::Fragmentation)?
    {
        framez::functions::send(write_state, codec, inner, frame)
            .await
            .map_err(|err| Error::Write(WriteError::WriteFrame(err)))?;
    }

    Ok(())
}

use embedded_io_async::{Read, Write};
use framez::state::{ReadState, WriteState};
use rand_core::Rng;

use crate::{
    ConnectionState, Frame, Message, OnFrame, WebSocketCore,
    codec::FramesCodec,
    error::{Error, ProtocolError, ReadError, WriteError},
    websocket_core::FragmentsState,
};

#[derive(Debug)]
pub struct ReadAutoCaller;

impl ReadAutoCaller {
    #[allow(clippy::too_many_arguments)]
    pub async fn call<'this, F, RW, RNG>(
        &self,
        auto: F,
        codec: &mut FramesCodec<RNG>,
        inner: &mut RW,
        read_state: &'this mut ReadState<'_>,
        write_state: &mut WriteState<'_>,
        fragments_state: &'this mut FragmentsState<'_>,
        state: &mut ConnectionState,
    ) -> Option<Result<Option<Message<'this>>, Error<RW::Error>>>
    where
        RW: Read + Write,
        RNG: Rng,
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

        WebSocketCore::<RW, RNG>::on_frame(fragments_state, frame)
            .map(|result| result.map_err(Error::from))
    }
}

#[derive(Debug)]
pub struct ReadCaller;

impl ReadCaller {
    #[allow(clippy::too_many_arguments)]
    pub async fn call<'this, RW, RNG>(
        &self,
        _auto: (),
        codec: &mut FramesCodec<RNG>,
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

        WebSocketCore::<RW, RNG>::on_frame(fragments_state, frame)
            .map(|result| result.map_err(Error::from))
    }
}

pub async fn send<RW, RNG>(
    codec: &mut FramesCodec<RNG>,
    inner: &mut RW,
    write_state: &mut WriteState<'_>,
    state: &mut ConnectionState,
    message: Message<'_>,
) -> Result<(), Error<RW::Error>>
where
    RW: Write,
    RNG: Rng,
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

pub async fn send_fragmented<RW, RNG>(
    codec: &mut FramesCodec<RNG>,
    inner: &mut RW,
    write_state: &mut WriteState<'_>,
    state: &mut ConnectionState,
    message: Message<'_>,
    fragment_size: usize,
) -> Result<(), Error<RW::Error>>
where
    RW: Write,
    RNG: Rng,
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

use crate::{
    CloseFrame, Frame, OpCode, OwnedCloseFrame, error::FragmentationError,
    fragments::FragmentsIterator,
};

/// A WebSocket message.
#[derive(Debug)]
pub enum Message<'a> {
    /// A text WebSocket message
    Text(&'a str),
    /// A binary WebSocket message
    Binary(&'a [u8]),
    /// A ping message with the specified payload
    ///
    /// The payload here must have a length less than 125 bytes
    Ping(&'a [u8]),
    /// A pong message with the specified payload
    ///
    /// The payload here must have a length less than 125 bytes
    Pong(&'a [u8]),
    /// A close message with the optional close frame.
    Close(Option<CloseFrame<'a>>),
}

impl<'a> Message<'a> {
    /// Indicates whether a message is a text message.
    pub const fn is_text(&self) -> bool {
        matches!(*self, Message::Text(_))
    }

    /// Indicates whether a message is a binary message.
    pub const fn is_binary(&self) -> bool {
        matches!(*self, Message::Binary(_))
    }

    /// Indicates whether a message is a ping message.
    pub const fn is_ping(&self) -> bool {
        matches!(*self, Message::Ping(_))
    }

    /// Indicates whether a message is a pong message.
    pub const fn is_pong(&self) -> bool {
        matches!(*self, Message::Pong(_))
    }

    /// Indicates whether a message is a close message.
    pub const fn is_close(&self) -> bool {
        matches!(*self, Message::Close(_))
    }

    pub(crate) const fn opcode(&self) -> OpCode {
        match self {
            Message::Text(_) => OpCode::Text,
            Message::Binary(_) => OpCode::Binary,
            Message::Ping(_) => OpCode::Ping,
            Message::Pong(_) => OpCode::Pong,
            Message::Close(_) => OpCode::Close,
        }
    }

    /// Get the length of the message's payload in bytes.
    pub(crate) const fn payload_len(&self) -> usize {
        match self {
            Message::Text(payload) => payload.len(),
            Message::Binary(payload) => payload.len(),
            Message::Ping(payload) => payload.len(),
            Message::Pong(payload) => payload.len(),
            Message::Close(Some(frame)) => 2 + frame.reason().len(),
            Message::Close(None) => 0,
        }
    }

    pub(crate) fn write(&self, dst: &mut [u8]) -> Option<usize> {
        if dst.len() < self.payload_len() {
            return None;
        }

        match self {
            Message::Text(payload) => {
                dst[..payload.len()].copy_from_slice(payload.as_bytes());
            }
            Message::Binary(payload) => {
                dst[..payload.len()].copy_from_slice(payload);
            }
            Message::Ping(payload) => {
                dst[..payload.len()].copy_from_slice(payload);
            }
            Message::Pong(payload) => {
                dst[..payload.len()].copy_from_slice(payload);
            }
            Message::Close(Some(frame)) => {
                let code: u16 = frame.code().into_u16();
                let code = code.to_be_bytes();

                dst[0..2].copy_from_slice(&code);
                dst[2..2 + frame.reason().len()].copy_from_slice(frame.reason().as_bytes());
            }
            Message::Close(None) => {}
        }

        Some(self.payload_len())
    }

    /// See [`FragmentsIterator::new()`] for details on how to create the iterator.
    pub(crate) fn fragments(
        &self,
        fragment_size: usize,
    ) -> Result<impl Iterator<Item = Frame<'a>>, FragmentationError> {
        if fragment_size < 1 {
            return Err(FragmentationError::InvalidFragmentSize);
        }

        match self {
            Message::Text(payload) => Ok(FragmentsIterator::new(
                OpCode::Text,
                payload.as_bytes(),
                fragment_size,
            )),
            Message::Binary(payload) => Ok(FragmentsIterator::new(
                OpCode::Binary,
                payload,
                fragment_size,
            )),
            _ => Err(FragmentationError::CanNotBeFragmented),
        }
    }
}

/// An owned WebSocket message.
#[derive(Debug)]
pub enum OwnedMessage<const N: usize> {
    /// A text WebSocket message
    Text(heapless::String<N>),
    /// A binary WebSocket message
    Binary(heapless::Vec<u8, N>),
    /// A ping message with the specified payload
    ///
    /// The payload here must have a length less than 125 bytes
    Ping(heapless::Vec<u8, N>),
    /// A pong message with the specified payload
    ///
    /// The payload here must have a length less than 125 bytes
    Pong(heapless::Vec<u8, N>),
    /// A close message with the optional close frame.
    Close(Option<OwnedCloseFrame<N>>),
}

impl<const N: usize> TryFrom<Message<'_>> for OwnedMessage<N> {
    type Error = heapless::CapacityError;

    fn try_from(value: Message<'_>) -> Result<Self, Self::Error> {
        match value {
            Message::Text(payload) => Ok(Self::Text(heapless::String::try_from(payload)?)),
            Message::Binary(payload) => Ok(Self::Binary(heapless::Vec::try_from(payload)?)),
            Message::Ping(payload) => Ok(Self::Ping(heapless::Vec::try_from(payload)?)),
            Message::Pong(payload) => Ok(Self::Pong(heapless::Vec::try_from(payload)?)),
            Message::Close(Some(frame)) => Ok(Self::Close(Some(OwnedCloseFrame::new(
                frame.code(),
                heapless::String::try_from(frame.reason())?,
            )))),
            Message::Close(None) => Ok(Self::Close(None)),
        }
    }
}

use crate::CloseCode;

/// A WebSocket Close frame.
#[derive(Debug)]
pub struct CloseFrame<'a> {
    /// The reason as a code.
    code: CloseCode,
    /// The reason as text string.
    reason: &'a str,
}

impl<'a> CloseFrame<'a> {
    /// Creates a new [`CloseFrame`].
    pub const fn new(code: CloseCode, reason: &'a str) -> Self {
        Self { code, reason }
    }

    /// Creates a new [`CloseFrame`] with no reason.
    pub const fn no_reason(code: CloseCode) -> Self {
        Self::new(code, "")
    }

    /// Returns the close code.
    pub const fn code(&self) -> CloseCode {
        self.code
    }

    /// Returns the reason as a string slice.
    pub const fn reason(&self) -> &'a str {
        self.reason
    }
}

/// An owned WebSocket Close frame.
#[derive(Debug)]
pub struct OwnedCloseFrame<const N: usize> {
    /// The reason as a code.
    code: CloseCode,
    /// The reason as text string.
    reason: heapless::String<N>,
}

impl<const N: usize> OwnedCloseFrame<N> {
    /// Creates a new [`OwnedCloseFrame`].
    pub const fn new(code: CloseCode, reason: heapless::String<N>) -> Self {
        Self { code, reason }
    }

    /// Creates a new [`OwnedCloseFrame`] with no reason.
    pub const fn no_reason(code: CloseCode) -> Self {
        Self::new(code, heapless::String::new())
    }

    /// Returns the close code.
    pub const fn code(&self) -> CloseCode {
        self.code
    }

    /// Returns the reason as a string slice.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl<const N: usize> TryFrom<CloseFrame<'_>> for OwnedCloseFrame<N> {
    type Error = heapless::CapacityError;

    fn try_from(value: CloseFrame<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            code: value.code(),
            reason: heapless::String::try_from(value.reason())?,
        })
    }
}

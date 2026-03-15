//! Noop implementation of embedded-io-async traits and rand-core for testing purposes.

use core::convert::Infallible;

use embedded_io_async::{ErrorType, Read, Write};
use rand_core::TryRng;

#[derive(Debug)]
pub struct Noop;

impl ErrorType for Noop {
    type Error = Infallible;
}

impl Read for Noop {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        Ok(buf.len())
    }
}

impl Write for Noop {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        Ok(buf.len())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl TryRng for Noop {
    type Error = Infallible;

    fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
        Ok(0)
    }

    fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
        Ok(0)
    }

    fn try_fill_bytes(&mut self, _dst: &mut [u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

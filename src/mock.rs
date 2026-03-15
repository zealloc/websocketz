//! Noop implementation of embedded-io-async traits and rand-core for testing purposes.

use core::convert::Infallible;

use embedded_io_async::{ErrorType, Read, Write};
use rand_core::RngCore;

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

impl RngCore for Noop {
    fn next_u32(&mut self) -> u32 {
        0
    }

    fn next_u64(&mut self) -> u64 {
        0
    }

    fn fill_bytes(&mut self, _dst: &mut [u8]) {}
}

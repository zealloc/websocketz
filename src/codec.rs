use framez::{decode::Decoder, encode::Encoder};
use rand::RngExt;
use rand_core::Rng;

use crate::{
    Frame, FrameMut, Header, Message, OpCode,
    error::{FrameDecodeError, FrameEncodeError},
};

#[derive(Debug)]
enum DecodeState {
    Init,
    DecodedHeader {
        fin: bool,
        opcode: OpCode,
        masked: bool,
        length_code: u8,
        extra: usize,
        min_src_len: usize,
    },
    DecodedPayloadLength {
        fin: bool,
        opcode: OpCode,
        mask: Option<[u8; 4]>,
        payload_len: usize,
        min_src_len: usize,
    },
}

#[derive(Debug)]
pub struct FramesCodec<R = ()> {
    unmask: bool,
    mask: bool,
    decode_state: DecodeState,
    rng: R,
}

impl<R> FramesCodec<R> {
    pub const fn new(rng: R) -> Self {
        Self {
            unmask: false,
            mask: false,
            decode_state: DecodeState::Init,
            rng,
        }
    }

    pub const fn set_unmask(&mut self, unmask: bool) {
        self.unmask = unmask;
    }

    pub const fn set_mask(&mut self, mask: bool) {
        self.mask = mask;
    }

    pub const fn rng_mut(&mut self) -> &mut R {
        &mut self.rng
    }

    /// Check if the codec is configured for a client.
    ///
    /// [`Self::mask`] and `NOT` [`Self::unmask`]
    const fn is_client(&self) -> bool {
        self.mask && !self.unmask
    }

    /// Check if the codec is configured a server.
    ///
    /// [`Self::unmask`] and `NOT` [`Self::mask`]
    const fn is_server(&self) -> bool {
        self.unmask && !self.mask
    }

    pub fn split(self) -> (FramesCodec<()>, FramesCodec<R>) {
        (
            FramesCodec {
                unmask: self.unmask,
                mask: self.mask,
                decode_state: self.decode_state,
                rng: (),
            },
            FramesCodec {
                unmask: self.unmask,
                mask: self.mask,
                decode_state: DecodeState::Init, // We don't care about the decode state in the second codec (writer)
                rng: self.rng,
            },
        )
    }

    #[cfg(test)]
    const fn into_client(mut self) -> Self {
        self.mask = true;
        self.unmask = false;
        self
    }

    #[cfg(test)]
    const fn into_server(mut self) -> Self {
        self.mask = false;
        self.unmask = true;
        self
    }
}

impl<R> framez::decode::DecodeError for FramesCodec<R> {
    type Error = FrameDecodeError;
}

impl<'buf, R> Decoder<'buf> for FramesCodec<R> {
    type Item = Frame<'buf>;

    fn decode(&mut self, src: &'buf mut [u8]) -> Result<Option<(Self::Item, usize)>, Self::Error> {
        const MIN_HEADER_SIZE: usize = 2;

        loop {
            match self.decode_state {
                DecodeState::Init => {
                    if src.len() < MIN_HEADER_SIZE {
                        return Ok(None);
                    }

                    let fin = src[0] & 0b10000000 != 0;
                    let rsv1 = src[0] & 0b01000000 != 0;
                    let rsv2 = src[0] & 0b00100000 != 0;
                    let rsv3 = src[0] & 0b00010000 != 0;

                    if rsv1 || rsv2 || rsv3 {
                        return Err(FrameDecodeError::ReservedBitsNotZero);
                    }

                    let opcode = OpCode::try_from_u8(src[0] & 0b00001111)?;
                    let masked = src[1] & 0b10000000 != 0;

                    if self.is_server() && !masked {
                        return Err(FrameDecodeError::UnmaskedFrameFromClient);
                    }

                    if self.is_client() && masked {
                        return Err(FrameDecodeError::MaskedFrameFromServer);
                    }

                    let length_code = src[1] & 0x7F;
                    let extra = match length_code {
                        126 => 2,
                        127 => 8,
                        _ => 0,
                    };

                    let min_src_len = MIN_HEADER_SIZE + extra + masked as usize * 4;

                    self.decode_state = DecodeState::DecodedHeader {
                        fin,
                        opcode,
                        masked,
                        length_code,
                        extra,
                        min_src_len,
                    };
                }
                DecodeState::DecodedHeader {
                    fin,
                    opcode,
                    masked,
                    length_code,
                    extra,
                    min_src_len,
                } => {
                    if src.len() < min_src_len {
                        return Ok(None);
                    }

                    let payload_len = match extra {
                        0 => length_code as usize,
                        2 => u16::from_be_bytes([src[2], src[3]]) as usize,
                        8 => usize::try_from(u64::from_be_bytes([
                            src[2], src[3], src[4], src[5], src[6], src[7], src[8], src[9],
                        ]))
                        .map_err(|_| FrameDecodeError::PayloadTooLarge)?,
                        _ => unreachable!("Extra must be 0, 2, or 8"),
                    };

                    let mask = masked.then(|| {
                        [
                            src[2 + extra],
                            src[3 + extra],
                            src[4 + extra],
                            src[5 + extra],
                        ]
                    });

                    // All control frames MUST have a payload length of 125 bytes or less
                    // and MUST NOT be fragmented. (RFC 6455)
                    if opcode.is_control() {
                        if !fin {
                            return Err(FrameDecodeError::ControlFrameFragmented);
                        }

                        if payload_len > 125 {
                            return Err(FrameDecodeError::ControlFrameTooLarge);
                        }
                    }

                    let min_src_len = min_src_len + payload_len;

                    self.decode_state = DecodeState::DecodedPayloadLength {
                        fin,
                        opcode,
                        mask,
                        payload_len,
                        min_src_len,
                    };
                }
                DecodeState::DecodedPayloadLength {
                    fin,
                    opcode,
                    mask,
                    payload_len,
                    min_src_len,
                } => {
                    if src.len() < min_src_len {
                        return Ok(None);
                    }

                    let start = min_src_len - payload_len;
                    let end = min_src_len;
                    let payload = &mut src[start..end];

                    let mut frame = FrameMut::new(fin, opcode, mask, payload);

                    if self.is_server() {
                        frame.unmask();
                    }

                    self.decode_state = DecodeState::Init;

                    return Ok(Some((frame.into_frame(), min_src_len)));
                }
            }
        }
    }
}

impl<R: Rng> FramesCodec<R> {
    #[inline(always)]
    fn encode_inner<F>(
        &mut self,
        fin: bool,
        opcode: OpCode,
        payload_len: usize,
        write_payload: F,
        dst: &mut [u8],
    ) -> Result<usize, FrameEncodeError>
    where
        F: FnOnce(&mut [u8]) -> Option<usize>,
    {
        let header = Header::new(fin, opcode, payload_len);

        let head_len = header
            .write(&mut dst[..])
            .ok_or(FrameEncodeError::BufferTooSmall)?;

        let mask: Option<[u8; 4]> = self.is_client().then(|| self.rng.random());

        let head_len = match mask {
            None => head_len,
            Some(mask) => {
                if head_len + 4 > dst.len() {
                    return Err(FrameEncodeError::BufferTooSmall);
                }

                dst[1] |= 0x80;
                dst[head_len..head_len + 4].copy_from_slice(&mask);

                head_len + 4
            }
        };

        let payload_len_written =
            write_payload(&mut dst[head_len..]).ok_or(FrameEncodeError::BufferTooSmall)?;

        if let Some(mask) = mask {
            crate::mask::unmask(&mut dst[head_len..head_len + payload_len_written], mask);
        }

        Ok(head_len + payload_len_written)
    }
}

impl<R: Rng> Encoder<Message<'_>> for FramesCodec<R> {
    type Error = FrameEncodeError;

    fn encode(&mut self, item: Message, dst: &mut [u8]) -> Result<usize, Self::Error> {
        self.encode_inner(
            true,
            item.opcode(),
            item.payload_len(),
            |buf| item.write(buf),
            dst,
        )
    }
}

impl<R: Rng> Encoder<Frame<'_>> for FramesCodec<R> {
    type Error = FrameEncodeError;

    fn encode(&mut self, item: Frame, dst: &mut [u8]) -> Result<usize, Self::Error> {
        self.encode_inner(
            item.is_final(),
            item.opcode(),
            item.payload().len(),
            |buf| item.write_payload(buf),
            dst,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod decode {
        use super::*;

        #[test]
        fn reserved_bits_not_zero() {
            let mut src = [0b11111111, 0b00000000];

            let mut codec = FramesCodec::new(());

            let error = codec.decode(&mut src).unwrap_err();

            assert!(matches!(error, FrameDecodeError::ReservedBitsNotZero));
        }

        #[test]
        fn unmasked_frame_from_client() {
            const UNMASKED_FRAME: &[u8] = &[
                0x81, // FIN=1, Text frame (opcode=0x1)
                0x02, // MASK=0, Payload length=2
                0x48, 0x69, // Payload: 'H', 'i'
            ];

            let src = &mut UNMASKED_FRAME.to_vec();

            let mut codec = FramesCodec::new(()).into_server();

            let error = codec.decode(src).unwrap_err();

            assert!(matches!(error, FrameDecodeError::UnmaskedFrameFromClient));
        }

        #[test]
        fn masked_frame_from_server() {
            #[rustfmt::skip]
            const MASKED_FRAME: &[u8] = &[
                0x81,             // FIN=1, opcode=0x1 (text)
                0x82,             // MASK=1 (bit 7), payload length=2 (bits 0–6)
                0x12, 0x34, 0x56, 0x78, // Masking key
                0x48 ^ 0x12,      // 'H' (0x48) masked
                0x69 ^ 0x34       // 'i' (0x69) masked
            ];

            let src = &mut MASKED_FRAME.to_vec();

            let mut codec = FramesCodec::new(()).into_client();

            let error = codec.decode(src).unwrap_err();

            assert!(matches!(error, FrameDecodeError::MaskedFrameFromServer));
        }

        #[test]
        fn invalid_opcode() {
            let mut src = [0b00001111, 0b00000000];

            let mut codec = FramesCodec::new(());

            let error = codec.decode(&mut src).unwrap_err();

            assert!(matches!(error, FrameDecodeError::InvalidOpCode));
        }

        #[test]
        #[cfg(target_pointer_width = "32")]
        #[ignore = "TODO"]
        fn payload_too_large() {
            //TODO
        }

        #[test]
        fn control_frame_fragmented() {
            const FRAGMENTED_CONTROL_FRAME: &[u8] = &[
                0x09, // FIN=0 (fragmented), opcode=0x9 (Ping)
                0x80, // MASK=1, payload length=0
                0x00, 0x00, 0x00, 0x00, // Masking key (no payload, but key required)
            ];

            let src = &mut FRAGMENTED_CONTROL_FRAME.to_vec();

            let mut codec = FramesCodec::new(());

            let error = codec.decode(src).unwrap_err();

            assert!(matches!(error, FrameDecodeError::ControlFrameFragmented));
        }

        #[test]
        fn control_frame_too_large() {
            fn build_control_frame_too_large() -> std::vec::Vec<u8> {
                let mut frame = std::vec![
                    0x89, // FIN=1, opcode=0x9 (Ping)
                    0xFE, // MASK=1, length=126
                    0x00, 0x7E, // Extended payload length = 126
                    0x12, 0x34, 0x56, 0x78, // Masking key
                ];

                let payload: std::vec::Vec<u8> = (0..126)
                    .map(|i| b'A' ^ [0x12, 0x34, 0x56, 0x78][i % 4]) // masked 'A'
                    .collect();

                frame.extend(payload);
                frame
            }

            let src = &mut build_control_frame_too_large();

            let mut codec = FramesCodec::new(());

            let error = codec.decode(src).unwrap_err();

            assert!(matches!(error, FrameDecodeError::ControlFrameTooLarge));
        }
    }

    mod encode {
        use rand::{
            SeedableRng,
            rngs::{StdRng, SysRng},
        };

        use super::*;

        #[test]
        fn buffer_too_small() {
            let dst = &mut [0u8; 16];
            let message = Message::Binary(&[0; 24]);

            let mut codec = FramesCodec::new(StdRng::try_from_rng(&mut SysRng).unwrap());

            let error = codec.encode(message, dst).unwrap_err();

            assert!(matches!(error, FrameEncodeError::BufferTooSmall));
        }
    }
}

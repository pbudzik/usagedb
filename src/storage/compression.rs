use std::io::{Read, Write, Result as IoResult};
use zstd::stream::{read::Decoder as ZstdDecoder, write::Encoder as ZstdEncoder};
use lz4_flex::frame::{FrameDecoder, FrameEncoder};

#[derive(Debug, Clone, Copy)]
pub enum CompressionCodec {
    Zstd,
    Lz4,
    None,
}

pub fn compress(data: &[u8], codec: CompressionCodec) -> IoResult<Vec<u8>> {
    match codec {
        CompressionCodec::Zstd => {
            let mut encoder = ZstdEncoder::new(Vec::new(), 3)?;
            encoder.write_all(data)?;
            encoder.finish()
        }
        CompressionCodec::Lz4 => {
            let mut encoder = FrameEncoder::new(Vec::new());
            encoder.write_all(data)?;
            encoder.finish().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        }
        CompressionCodec::None => Ok(data.to_vec()),
    }
}

pub fn decompress(data: &[u8], codec: CompressionCodec) -> IoResult<Vec<u8>> {
    match codec {
        CompressionCodec::Zstd => {
            let mut decoder = ZstdDecoder::new(data)?;
            let mut result = Vec::new();
            decoder.read_to_end(&mut result)?;
            Ok(result)
        }
        CompressionCodec::Lz4 => {
            let mut decoder = FrameDecoder::new(data);
            let mut result = Vec::new();
            decoder.read_to_end(&mut result)?;
            Ok(result)
        }
        CompressionCodec::None => Ok(data.to_vec()),
    }
}

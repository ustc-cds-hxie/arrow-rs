// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Contains codec interface and supported codec implementations.
//!
//! See [`Compression`](crate::basic::Compression) enum for all available compression
//! algorithms.
//!
#[cfg_attr(
    feature = "experimental",
    doc = r##"
# Example

```no_run
use parquet::{basic::Compression, compression::{create_codec, CodecOptionsBuilder}};

let codec_options = CodecOptionsBuilder::default()
    .set_backward_compatible_lz4(false)
    .build();
let mut codec = match create_codec(Compression::SNAPPY, &codec_options) {
 Ok(Some(codec)) => codec,
 _ => panic!(),
};

let data = vec![b'p', b'a', b'r', b'q', b'u', b'e', b't'];
let mut compressed = vec![];
codec.compress(&data[..], &mut compressed).unwrap();

let mut output = vec![];
codec.decompress(&compressed[..], &mut output, None).unwrap();

assert_eq!(output, data);
```
"##
)]
use crate::basic::Compression as CodecType;
use crate::errors::{ParquetError, Result};
use crate::data_type::DataTypeConstraint;

// convert byte array
use byteorder::{ByteOrder, BigEndian};

/// Parquet compression codec interface.
pub trait Codec: Send {
    /// Compresses data stored in slice `input_buf` and appends the compressed result
    /// to `output_buf`.
    ///
    /// Note that you'll need to call `clear()` before reusing the same `output_buf`
    /// across different `compress` calls.
    fn compress<T: DataTypeConstraint>(
        &mut self, 
        input_buf: &[T], 
        output_buf: &mut Vec<u8>,
    ) -> Result<()>;

    /// Decompresses data stored in slice `input_buf` and appends output to `output_buf`.
    ///
    /// If the uncompress_size is provided it will allocate the exact amount of memory.
    /// Otherwise, it will estimate the uncompressed size, allocating an amount of memory
    /// greater or equal to the real uncompress_size.
    ///
    /// Returns the total number of bytes written.
    fn decompress<T: DataTypeConstraint>(
        &mut self,
        input_buf: &[u8],
        output_buf: &mut Vec<T>,
        uncompress_size: Option<usize>,
    ) -> Result<usize>;

    // determine if the input is allowed for compress, or the output is allowed for decompress
    // check if the input/output data type is allowed
    fn allowed<T: DataTypeConstraint>(&mut self, iodata: &[T]) -> bool {
        match T::typename() {
            "u8" | "u16" | "u32" | "u64" => true,
            "i8" | "i16" | "i32" | "i64" => true,
            "f32" | "f64" => true,
            _ => false,
        }
    }

    // convert input [T] -> [u8]
    // convert output [u8] -> [T]
    fn convert_data_type<T: DataTypeConstraint, U: DataTypeConstraint>(&mut self, input_buf: &[T], output_buf: &mut Vec<U>) -> Result<usize> {
        
        if input_buf.len() == 0 { 
            return OK(0); 
        }
        // [u8] -> <T>
        if T::typename() == "u8" {
            match U::typename() {
                "u8"  => { 
                    output_buf.append(&mut input_buf);
                },
                "u16" => { 
                    output_buf.resize(input_buf.len() / std::mem::size_of(u16), 0u16);
                    BigEndian::read_u16_into(input_buf, &mut output_buf); 
                },
                "u32" => {
                    output_buf.resize(input_buf.len() / std::mem::size_of(u32), 0u32);
                    BigEndian::read_u32_into(input_buf, &mut output_buf); 
                },
                "u64" => {
                    output_buf.resize(input_buf.len() / std::mem::size_of(u64), 0u64);
                    BigEndian::read_u64_into(input_buf, &mut output_buf); 
                },
                "i8"  => { 
                    output_buf.append(&mut input_buf.iter().map(|&x| x as u8).collect::<Vec<_>>());
                },
                "i16" => {
                    output_buf.resize(input_buf.len() / std::mem::size_of(i16), 0i16);
                    BigEndian::read_i16_into(input_buf, &mut output_buf); 
                },
                "i32" => {
                    output_buf.resize(input_buf.len() / std::mem::size_of(i32), 0i32);
                    BigEndian::read_u32_into(input_buf, &mut output_buf); 
                },
                "i64" => {
                    output_buf.resize(input_buf.len() / std::mem::size_of(i64), 0i64);
                    BigEndian::read_i64_into(input_buf, &mut output_buf); 
                },
                "f32" => {
                    output_buf.resize(input_buf.len() / std::mem::size_of(f32), 0f32);
                    BigEndian::read_f32_into(input_buf, &mut output_buf); 
                },
                "f64" => {
                    output_buf.resize(input_buf.len() / std::mem::size_of(f64), 0f64);
                    BigEndian::read_f64_into(input_buf, &mut output_buf); 
                },
                _ => { 
                    panic!("Unsupported data type: {:?}", input_buf[0].typename());
                },
            }
            return OK(input_buf.len())
        }

        // <T> -> [u8]
        if U::typename() == "u8" {
            match T::typename() {
                "u8"  => { 
                    output_buf.append(&mut input_buf);
                },
                "u16" => { 
                    new_input_buf.resize(input_buf.len() * std::mem::size_of(u16), 0u8);
                    BigEndian::write_u16_into(input_buf, &mut new_input_buf); 
                },
                "u32" => {
                    new_input_buf.resize(input_buf.len() * std::mem::size_of(u32), 0u8);
                    BigEndian::write_u32_into(input_buf, &mut new_input_buf); 
                },
                "u64" => {
                    new_input_buf.resize(input_buf.len() * std::mem::size_of(u64), 0u8);
                    BigEndian::write_u64_into(input_buf, &mut new_input_buf); 
                },
                "i8"  => { 
                    output_buf.append(&mut input_buf.iter().map(|&x| x as u8).collect::<Vec<_>>());
                },
                "i16" => {
                    new_input_buf.resize(input_buf.len() * std::mem::size_of(i16), 0u8);
                    BigEndian::write_i16_into(input_buf, &mut new_input_buf); 
                },
                "i32" => {
                    new_input_buf.resize(input_buf.len() * std::mem::size_of(i32), 0u8);
                    BigEndian::write_i32_into(input_buf, &mut new_input_buf); 
                },
                "i64" => {
                    new_input_buf.resize(input_buf.len() * std::mem::size_of(i64), 0u8);
                    BigEndian::write_i64_into(input_buf, &mut new_input_buf); 
                },
                "f32" => {
                    new_input_buf.resize(input_buf.len() * std::mem::size_of(f32), 0u8);
                    BigEndian::write_f32_into(input_buf, &mut new_input_buf); 
                },
                "f64" => {
                    new_input_buf.resize(input_buf.len() * std::mem::size_of(f64), 0u8);
                    BigEndian::write_f64_into(input_buf, &mut new_input_buf); 
                },
                _ => { 
                    panic!("Unsupported data type: {:?}", input_buf[0].typename());
                },
            }
            return OK(input_buf.len())
        }
           
    }
}

/// Struct to hold `Codec` creation options.
#[derive(Debug, PartialEq, Eq)]
pub struct CodecOptions {
    /// Whether or not to fallback to other LZ4 older implementations on error in LZ4_HADOOP.
    backward_compatible_lz4: bool,
}

impl Default for CodecOptions {
    fn default() -> Self {
        CodecOptionsBuilder::default().build()
    }
}

pub struct CodecOptionsBuilder {
    /// Whether or not to fallback to other LZ4 older implementations on error in LZ4_HADOOP.
    backward_compatible_lz4: bool,
}

impl Default for CodecOptionsBuilder {
    fn default() -> Self {
        Self {
            backward_compatible_lz4: true,
        }
    }
}

impl CodecOptionsBuilder {
    /// Enable/disable backward compatible LZ4.
    ///
    /// If backward compatible LZ4 is enable, on LZ4_HADOOP error it will fallback
    /// to the older versions LZ4 algorithms. That is LZ4_FRAME, for backward compatibility
    /// with files generated by older versions of this library, and LZ4_RAW, for backward
    /// compatibility with files generated by older versions of parquet-cpp.
    ///
    /// If backward compatible LZ4 is disabled, on LZ4_HADOOP error it will return the error.
    pub fn set_backward_compatible_lz4(mut self, value: bool) -> CodecOptionsBuilder {
        self.backward_compatible_lz4 = value;
        self
    }

    pub fn build(self) -> CodecOptions {
        CodecOptions {
            backward_compatible_lz4: self.backward_compatible_lz4,
        }
    }
}

/// Given the compression type `codec`, returns a codec used to compress and decompress
/// bytes for the compression type.
/// This returns `None` if the codec type is `UNCOMPRESSED`.
pub fn create_codec(
    codec: CodecType,
    _options: &CodecOptions,
) -> Result<Option<Box<dyn Codec>>> {
    match codec {
        #[cfg(any(feature = "brotli", test))]
        CodecType::BROTLI => Ok(Some(Box::new(BrotliCodec::new()))),
        #[cfg(any(feature = "flate2", test))]
        CodecType::GZIP => Ok(Some(Box::new(GZipCodec::new()))),
        #[cfg(any(feature = "snap", test))]
        CodecType::SNAPPY => Ok(Some(Box::new(SnappyCodec::new()))),
        #[cfg(any(feature = "lz4", test))]
        CodecType::LZ4 => Ok(Some(Box::new(LZ4HadoopCodec::new(
            _options.backward_compatible_lz4,
        )))),
        #[cfg(any(feature = "zstd", test))]
        CodecType::ZSTD => Ok(Some(Box::new(ZSTDCodec::new()))),
        #[cfg(any(feature = "lz4", test))]
        CodecType::LZ4_RAW => Ok(Some(Box::new(LZ4RawCodec::new()))),
        // q-compress
        CodecType::QCOM => Ok(Some(Box::new(QcomCodec::new()))),
        CodecType::UNCOMPRESSED => Ok(None),
        _ => Err(nyi_err!("The codec type {} is not supported yet", codec)),
    }
}

#[cfg(any(feature = "snap", test))]
mod snappy_codec {
    use snap::raw::{decompress_len, max_compress_len, Decoder, Encoder};

    use crate::compression::Codec;
    use crate::errors::Result;

    /// Codec for Snappy compression format.
    pub struct SnappyCodec {
        decoder: Decoder,
        encoder: Encoder,
    }

    impl SnappyCodec {
        /// Creates new Snappy compression codec.
        pub(crate) fn new() -> Self {
            Self {
                decoder: Decoder::new(),
                encoder: Encoder::new(),
            }
        }
    }

    impl Codec for SnappyCodec {
        fn decompress<T: DataTypeConstraint>(
            &mut self,
            input_buf: &[u8],
            external_output_buf: &mut Vec<T>,
            uncompress_size: Option<usize>,
        ) -> Result<usize> {

            // internal output buf as [u8]
            let mut output_buf : Vec<u8> = Vec::new();

            let len = match uncompress_size {
                Some(size) => size,
                None => decompress_len(input_buf)?,
            };
            let offset = output_buf.len();
            output_buf.resize(offset + len, 0);
            let retsize = self.decoder
                .decompress(input_buf, &mut output_buf[offset..])
                .map_err(|e| e.into());
            
            // convert input to other data types
            self.convert_data_type(&mut output_buf, external_output_buf);

            retsize
        }

        fn compress<T: DataTypeConstraint>(
            &mut self, 
            external_input_buf: &[T], 
            output_buf: &mut Vec<u8>,
        ) -> Result<()> {

            // internal input buf as [u8]
            let mut input_buf: Vec<u8>= Vec::new();
            self.convert_data_type(external_input_buf, &mut input_buf);

            let output_buf_len = output_buf.len();
            let required_len = max_compress_len(input_buf.len());
            output_buf.resize(output_buf_len + required_len, 0);
            let n = self
                .encoder
                .compress(input_buf, &mut output_buf[output_buf_len..])?;
            output_buf.truncate(output_buf_len + n);
            Ok(())
        }


    }
}
#[cfg(any(feature = "snap", test))]
pub use snappy_codec::*;

#[cfg(any(feature = "flate2", test))]
mod gzip_codec {

    use std::io::{Read, Write};

    use flate2::{read, write, Compression};

    use crate::compression::Codec;
    use crate::errors::Result;

    /// Codec for GZIP compression algorithm.
    pub struct GZipCodec {}

    impl GZipCodec {
        /// Creates new GZIP compression codec.
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    impl Codec for GZipCodec {
        fn decompress<T: DataTypeConstraint>(
            &mut self,
            input_buf: &[u8],
            external_output_buf: &mut Vec<T>,
            _uncompress_size: Option<usize>,
        ) -> Result<usize> {

            // internal output buf as [u8]
            let mut output_buf : Vec<u8> = Vec::new();

            let mut decoder = read::GzDecoder::new(input_buf);
            let retsize = decoder.read_to_end(output_buf).map_err(|e| e.into());

            // convert input to other data types
            self.convert_data_type(&mut output_buf, external_output_buf);

            retsize
        }

        fn compress<T: DataTypeConstraint>(
            &mut self, 
            external_input_buf: &[T], 
            output_buf: &mut Vec<u8>,
        ) -> Result<()> {

            // internal input buf as [u8]
            let mut input_buf: Vec<u8>= Vec::new();
            self.convert_data_type(external_input_buf, &mut input_buf);

            let mut encoder = write::GzEncoder::new(output_buf, Compression::default());
            encoder.write_all(input_buf)?;
            encoder.try_finish().map_err(|e| e.into())
        }
    }
}
#[cfg(any(feature = "flate2", test))]
pub use gzip_codec::*;

#[cfg(any(feature = "brotli", test))]
mod brotli_codec {

    use std::io::{Read, Write};

    use crate::compression::Codec;
    use crate::errors::Result;

    const BROTLI_DEFAULT_BUFFER_SIZE: usize = 4096;
    const BROTLI_DEFAULT_COMPRESSION_QUALITY: u32 = 1; // supported levels 0-9
    const BROTLI_DEFAULT_LG_WINDOW_SIZE: u32 = 22; // recommended between 20-22

    /// Codec for Brotli compression algorithm.
    pub struct BrotliCodec {}

    impl BrotliCodec {
        /// Creates new Brotli compression codec.
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    impl Codec for BrotliCodec {
        fn decompress<T: DataTypeConstraint>(
            &mut self,
            input_buf: &[u8],
            external_output_buf: &mut Vec<T>,
            uncompress_size: Option<usize>,
        ) -> Result<usize> {

            // internal output buf as [u8]
            let mut output_buf : Vec<u8> = Vec::new();

            let buffer_size = uncompress_size.unwrap_or(BROTLI_DEFAULT_BUFFER_SIZE);
            let retsize = brotli::Decompressor::new(input_buf, buffer_size)
                .read_to_end(output_buf)
                .map_err(|e| e.into());

            // convert input to other data types
            self.convert_data_type(&mut output_buf, external_output_buf);

            retsize
        }

        fn compress<T: DataTypeConstraint>(
            &mut self, 
            external_input_buf: &[T], 
            output_buf: &mut Vec<u8>,
        ) -> Result<()> {

            // internal input buf as [u8]
            let mut input_buf: Vec<u8>= Vec::new();
            self.convert_data_type(external_input_buf, &mut input_buf);

            let mut encoder = brotli::CompressorWriter::new(
                output_buf,
                BROTLI_DEFAULT_BUFFER_SIZE,
                BROTLI_DEFAULT_COMPRESSION_QUALITY,
                BROTLI_DEFAULT_LG_WINDOW_SIZE,
            );
            encoder.write_all(input_buf)?;
            encoder.flush().map_err(|e| e.into())
        }
    }
}
#[cfg(any(feature = "brotli", test))]
pub use brotli_codec::*;

#[cfg(any(feature = "lz4", test))]
mod lz4_codec {
    use std::io::{Read, Write};

    use crate::compression::Codec;
    use crate::errors::Result;

    const LZ4_BUFFER_SIZE: usize = 4096;

    /// Codec for LZ4 compression algorithm.
    pub struct LZ4Codec {}

    impl LZ4Codec {
        /// Creates new LZ4 compression codec.
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    impl Codec for LZ4Codec {
        fn decompress<T: DataTypeConstraint>(
            &mut self,
            input_buf: &[u8],
            external_output_buf: &mut Vec<T>,
            _uncompress_size: Option<usize>,
        ) -> Result<usize> {

            // internal output buf as [u8]
            let mut output_buf : Vec<u8> = Vec::new();

            let mut decoder = lz4::Decoder::new(input_buf)?;
            let mut buffer: [u8; LZ4_BUFFER_SIZE] = [0; LZ4_BUFFER_SIZE];
            let mut total_len = 0;
            loop {
                let len = decoder.read(&mut buffer)?;
                if len == 0 {
                    break;
                }
                total_len += len;
                output_buf.write_all(&buffer[0..len])?;
            }

            // convert input to other data types
            self.convert_data_type(&mut output_buf, external_output_buf);

            Ok(total_len)
        }

        fn compress<T: DataTypeConstraint>(
            &mut self, 
            external_input_buf: &[T], 
            output_buf: &mut Vec<u8>,
        ) -> Result<()> {

            // internal input buf as [u8]
            let mut input_buf: Vec<u8>= Vec::new();
            self.convert_data_type(external_input_buf, &mut input_buf);

            let mut encoder = lz4::EncoderBuilder::new().build(output_buf)?;
            let mut from = 0;
            loop {
                let to = std::cmp::min(from + LZ4_BUFFER_SIZE, input_buf.len());
                encoder.write_all(&input_buf[from..to])?;
                from += LZ4_BUFFER_SIZE;
                if from >= input_buf.len() {
                    break;
                }
            }
            encoder.finish().1.map_err(|e| e.into())
        }
    }
}
#[cfg(any(feature = "lz4", test))]
pub use lz4_codec::*;

#[cfg(any(feature = "zstd", test))]
mod zstd_codec {
    use std::io::{self, Write};

    use crate::compression::Codec;
    use crate::errors::Result;

    /// Codec for Zstandard compression algorithm.
    pub struct ZSTDCodec {}

    impl ZSTDCodec {
        /// Creates new Zstandard compression codec.
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    /// Compression level (1-21) for ZSTD. Choose 1 here for better compression speed.
    const ZSTD_COMPRESSION_LEVEL: i32 = 1;

    impl Codec for ZSTDCodec {
        fn decompress<T: DataTypeConstraint>(
            &mut self,
            input_buf: &[u8],
            external_output_buf: &mut Vec<T>,
            _uncompress_size: Option<usize>,
        ) -> Result<usize> {

            // internal output buf as [u8]
            let mut output_buf : Vec<u8> = Vec::new();

            let mut decoder = zstd::Decoder::new(input_buf)?;
            let retsize = match io::copy(&mut decoder, output_buf) {
                Ok(n) => Ok(n as usize),
                Err(e) => Err(e.into()),
            };

            // convert input to other data types
            self.convert_data_type(&mut output_buf, external_output_buf);

            retsize
        }

        fn compress<T: DataTypeConstraint>(
            &mut self, 
            external_input_buf: &[T], 
            output_buf: &mut Vec<u8>,
        ) -> Result<()> {

            // internal input buf as [u8]
            let mut input_buf: Vec<u8>= Vec::new();
            self.convert_data_type(external_input_buf, &mut input_buf);

            let mut encoder = zstd::Encoder::new(output_buf, ZSTD_COMPRESSION_LEVEL)?;
            encoder.write_all(input_buf)?;
            match encoder.finish() {
                Ok(_) => Ok(()),
                Err(e) => Err(e.into()),
            }
        }
    }
}
#[cfg(any(feature = "zstd", test))]
pub use zstd_codec::*;

#[cfg(any(feature = "lz4", test))]
mod lz4_raw_codec {
    use crate::compression::Codec;
    use crate::errors::ParquetError;
    use crate::errors::Result;

    /// Codec for LZ4 Raw compression algorithm.
    pub struct LZ4RawCodec {}

    impl LZ4RawCodec {
        /// Creates new LZ4 Raw compression codec.
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    impl Codec for LZ4RawCodec {
        fn decompress<T: DataTypeConstraint>(
            &mut self,
            input_buf: &[u8],
            external_output_buf: &mut Vec<T>,
            uncompress_size: Option<usize>,
        ) -> Result<usize> {

            // internal output buf as [u8]
            let mut output_buf : Vec<u8> = Vec::new();

            let offset = output_buf.len();
            let required_len = match uncompress_size {
                Some(uncompress_size) => uncompress_size,
                None => {
                    return Err(ParquetError::General(
                        "LZ4RawCodec unsupported without uncompress_size".into(),
                    ))
                }
            };
            output_buf.resize(offset + required_len, 0);
            let retsize = match lz4::block::decompress_to_buffer(
                input_buf,
                Some(required_len.try_into().unwrap()),
                &mut output_buf[offset..],
            ) {
                Ok(n) => {
                    if n != required_len {
                        return Err(ParquetError::General(
                            "LZ4RawCodec uncompress_size is not the expected one".into(),
                        ));
                    }
                    Ok(n)
                }
                Err(e) => Err(e.into()),
            };

            // convert input to other data types
            self.convert_data_type(&mut output_buf, external_output_buf);

            retsize
        }

        fn compress<T: DataTypeConstraint>(
            &mut self, 
            external_input_buf: &[T], 
            output_buf: &mut Vec<u8>,
        ) -> Result<()> {

            // internal input buf as [u8]
            let mut input_buf: Vec<u8>= Vec::new();
            self.convert_data_type(external_input_buf, &mut input_buf);

            let offset = output_buf.len();
            let required_len = lz4::block::compress_bound(input_buf.len())?;
            output_buf.resize(offset + required_len, 0);
            match lz4::block::compress_to_buffer(
                input_buf,
                None,
                false,
                &mut output_buf[offset..],
            ) {
                Ok(n) => {
                    output_buf.truncate(offset + n);
                    Ok(())
                }
                Err(e) => Err(e.into()),
            }
        }
    }
}
#[cfg(any(feature = "lz4", test))]
pub use lz4_raw_codec::*;

#[cfg(any(feature = "lz4", test))]
mod lz4_hadoop_codec {
    use crate::compression::lz4_codec::LZ4Codec;
    use crate::compression::lz4_raw_codec::LZ4RawCodec;
    use crate::compression::Codec;
    use crate::errors::{ParquetError, Result};
    use std::io;

    /// Size of u32 type.
    const SIZE_U32: usize = std::mem::size_of::<u32>();

    /// Length of the LZ4_HADOOP prefix.
    const PREFIX_LEN: usize = SIZE_U32 * 2;

    /// Codec for LZ4 Hadoop compression algorithm.
    pub struct LZ4HadoopCodec {
        /// Whether or not to fallback to other LZ4 implementations on error.
        /// Fallback is done to be backward compatible with older versions of this
        /// library and older versions parquet-cpp.
        backward_compatible_lz4: bool,
    }

    impl LZ4HadoopCodec {
        /// Creates new LZ4 Hadoop compression codec.
        pub(crate) fn new(backward_compatible_lz4: bool) -> Self {
            Self {
                backward_compatible_lz4,
            }
        }
    }

    /// Try to decompress the buffer as if it was compressed with the Hadoop Lz4Codec.
    /// Adapted from pola-rs [compression.rs:try_decompress_hadoop](https://pola-rs.github.io/polars/src/parquet2/compression.rs.html#225)
    /// Translated from the apache arrow c++ function [TryDecompressHadoop](https://github.com/apache/arrow/blob/bf18e6e4b5bb6180706b1ba0d597a65a4ce5ca48/cpp/src/arrow/util/compression_lz4.cc#L474).
    /// Returns error if decompression failed.
    fn try_decompress_hadoop(
        input_buf: &[u8],
        output_buf: &mut [u8],
    ) -> io::Result<usize> {
        // Parquet files written with the Hadoop Lz4Codec use their own framing.
        // The input buffer can contain an arbitrary number of "frames", each
        // with the following structure:
        // - bytes 0..3: big-endian uint32_t representing the frame decompressed size
        // - bytes 4..7: big-endian uint32_t representing the frame compressed size
        // - bytes 8...: frame compressed data
        //
        // The Hadoop Lz4Codec source code can be found here:
        // https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-nativetask/src/main/native/src/codec/Lz4Codec.cc
        let mut input_len = input_buf.len();
        let mut input = input_buf;
        let mut read_bytes = 0;
        let mut output_len = output_buf.len();
        let mut output: &mut [u8] = output_buf;
        while input_len >= PREFIX_LEN {
            let mut bytes = [0; SIZE_U32];
            bytes.copy_from_slice(&input[0..4]);
            let expected_decompressed_size = u32::from_be_bytes(bytes);
            let mut bytes = [0; SIZE_U32];
            bytes.copy_from_slice(&input[4..8]);
            let expected_compressed_size = u32::from_be_bytes(bytes);
            input = &input[PREFIX_LEN..];
            input_len -= PREFIX_LEN;

            if input_len < expected_compressed_size as usize {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Not enough bytes for Hadoop frame",
                ));
            }

            if output_len < expected_decompressed_size as usize {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Not enough bytes to hold advertised output",
                ));
            }
            let decompressed_size = lz4::block::decompress_to_buffer(
                &input[..expected_compressed_size as usize],
                Some(output_len as i32),
                output,
            )?;
            if decompressed_size != expected_decompressed_size as usize {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Unexpected decompressed size",
                ));
            }
            input_len -= expected_compressed_size as usize;
            output_len -= expected_decompressed_size as usize;
            read_bytes += expected_decompressed_size as usize;
            if input_len > expected_compressed_size as usize {
                input = &input[expected_compressed_size as usize..];
                output = &mut output[expected_decompressed_size as usize..];
            } else {
                break;
            }
        }
        if input_len == 0 {
            Ok(read_bytes)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Not all input are consumed",
            ))
        }
    }

    impl Codec for LZ4HadoopCodec {
        fn decompress<T: DataTypeConstraint>(
            &mut self,
            input_buf: &[u8],
            external_output_buf: &mut Vec<T>,
            uncompress_size: Option<usize>,
        ) -> Result<usize> {

            // internal output buf as [u8]
            let mut output_buf : Vec<u8> = Vec::new();

            let output_len = output_buf.len();
            let required_len = match uncompress_size {
                Some(n) => n,
                None => {
                    return Err(ParquetError::General(
                        "LZ4HadoopCodec unsupported without uncompress_size".into(),
                    ))
                }
            };
            output_buf.resize(output_len + required_len, 0);
            let retsize = match try_decompress_hadoop(input_buf, &mut output_buf[output_len..]) {
                Ok(n) => {
                    if n != required_len {
                        return Err(ParquetError::General(
                            "LZ4HadoopCodec uncompress_size is not the expected one"
                                .into(),
                        ));
                    }
                    Ok(n)
                }
                Err(e) if !self.backward_compatible_lz4 => Err(e.into()),
                // Fallback done to be backward compatible with older versions of this
                // libray and older versions of parquet-cpp.
                Err(_) => {
                    // Truncate any inserted element before tryingg next algorithm.
                    output_buf.truncate(output_len);
                    match LZ4Codec::new().decompress(
                        input_buf,
                        output_buf,
                        uncompress_size,
                    ) {
                        Ok(n) => Ok(n),
                        Err(_) => {
                            // Truncate any inserted element before tryingg next algorithm.
                            output_buf.truncate(output_len);
                            LZ4RawCodec::new().decompress(
                                input_buf,
                                output_buf,
                                uncompress_size,
                            )
                        }
                    }
                }
            };

            // convert input to other data types
            self.convert_data_type(&mut output_buf, external_output_buf);

            retsize
        }

        fn compress<T: DataTypeConstraint>(
            &mut self, 
            external_input_buf: &[T], 
            output_buf: &mut Vec<u8>,
        ) -> Result<()> {

            // internal input buf as [u8]
            let mut input_buf: Vec<u8>= Vec::new();
            self.convert_data_type(external_input_buf, &mut input_buf);

            // Allocate memory to store the LZ4_HADOOP prefix.
            let offset = output_buf.len();
            output_buf.resize(offset + PREFIX_LEN, 0);

            // Append LZ4_RAW compressed bytes after prefix.
            LZ4RawCodec::new().compress(input_buf, output_buf)?;

            // Prepend decompressed size and compressed size in big endian to be compatible
            // with LZ4_HADOOP.
            let output_buf = &mut output_buf[offset..];
            let compressed_size = output_buf.len() - PREFIX_LEN;
            let compressed_size = compressed_size as u32;
            let uncompressed_size = input_buf.len() as u32;
            output_buf[..SIZE_U32].copy_from_slice(&uncompressed_size.to_be_bytes());
            output_buf[SIZE_U32..PREFIX_LEN].copy_from_slice(&compressed_size.to_be_bytes());

            Ok(())
        }
    }
}
#[cfg(any(feature = "lz4", test))]
pub use lz4_hadoop_codec::*;

#[cfg(any(feature = "qcom", test))]
mod qcom_codec {
    use crate::compression::Codec;
    use crate::errors::ParquetError;
    use crate::errors::Result;

    /// Codec for LZ4 Raw compression algorithm.
    pub struct QcomCodec {}

    impl QcomCodec {
        /// Creates new LZ4 Raw compression codec.
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    impl Codec for QcomCodec {
        fn decompress<T: DataTypeConstraint>(
            &mut self,
            input_buf: &[u8],
            output_buf: &mut Vec<T>,
            uncompress_size: Option<usize>,
        ) -> Result<usize> {

            output_buf = auto_decompress::<T>(input_buf).expect("failed to decompress");
            
            OK(output_buf.len())
        }

        fn compress<T: DataTypeConstraint>(
            &mut self, 
            external_input_buf: &[T], 
            output_buf: &mut Vec<u8>,
        ) -> Result<()> {

            output_buf = auto_compress::<T>(external_input_buf).expect("failed to decompress");
            OK(())
        }

        // determine if the input is allowed for compress, or the output is allowed for decompress
        // check if the input/output data type is allowed
        fn allowed<T: DataTypeConstraint>(&mut self, iodata: &[T]) -> bool {
            if iodata.len() == 0 { false }
            else {
                match iodata[0].typename() {
                    "u16" | "u32" | "u64" => true,
                    "i16" | "i32" | "i64" => true,
                    "f32" | "f64" => true,
                    _ => false,
                }
            }   
        }
    }
}
#[cfg(any(feature = "qcom", test))]
pub use qcom_codec::*;

#[cfg(test)]
mod tests {
    use super::*;

    use crate::util::test_common::rand_gen::random_bytes;

    fn test_roundtrip(c: CodecType, data: &[u8], uncompress_size: Option<usize>) {
        let codec_options = CodecOptionsBuilder::default()
            .set_backward_compatible_lz4(false)
            .build();
        let mut c1 = create_codec(c, &codec_options).unwrap().unwrap();
        let mut c2 = create_codec(c, &codec_options).unwrap().unwrap();

        // Compress with c1
        let mut compressed = Vec::new();
        let mut decompressed = Vec::new();
        c1.compress(data, &mut compressed)
            .expect("Error when compressing");

        // Decompress with c2
        let decompressed_size = c2
            .decompress(compressed.as_slice(), &mut decompressed, uncompress_size)
            .expect("Error when decompressing");
        assert_eq!(data.len(), decompressed_size);
        assert_eq!(data, decompressed.as_slice());

        decompressed.clear();
        compressed.clear();

        // Compress with c2
        c2.compress(data, &mut compressed)
            .expect("Error when compressing");

        // Decompress with c1
        let decompressed_size = c1
            .decompress(compressed.as_slice(), &mut decompressed, uncompress_size)
            .expect("Error when decompressing");
        assert_eq!(data.len(), decompressed_size);
        assert_eq!(data, decompressed.as_slice());

        decompressed.clear();
        compressed.clear();

        // Test does not trample existing data in output buffers
        let prefix = &[0xDE, 0xAD, 0xBE, 0xEF];
        decompressed.extend_from_slice(prefix);
        compressed.extend_from_slice(prefix);

        c2.compress(data, &mut compressed)
            .expect("Error when compressing");

        assert_eq!(&compressed[..4], prefix);

        let decompressed_size = c2
            .decompress(&compressed[4..], &mut decompressed, uncompress_size)
            .expect("Error when decompressing");

        assert_eq!(data.len(), decompressed_size);
        assert_eq!(data, &decompressed[4..]);
        assert_eq!(&decompressed[..4], prefix);
    }

    fn test_codec_with_size(c: CodecType) {
        let sizes = vec![100, 10000, 100000];
        for size in sizes {
            let data = random_bytes(size);
            test_roundtrip(c, &data, Some(data.len()));
        }
    }

    fn test_codec_without_size(c: CodecType) {
        let sizes = vec![100, 10000, 100000];
        for size in sizes {
            let data = random_bytes(size);
            test_roundtrip(c, &data, None);
        }
    }

    #[test]
    fn test_codec_snappy() {
        test_codec_with_size(CodecType::SNAPPY);
        test_codec_without_size(CodecType::SNAPPY);
    }

    #[test]
    fn test_codec_gzip() {
        test_codec_with_size(CodecType::GZIP);
        test_codec_without_size(CodecType::GZIP);
    }

    #[test]
    fn test_codec_brotli() {
        test_codec_with_size(CodecType::BROTLI);
        test_codec_without_size(CodecType::BROTLI);
    }

    #[test]
    fn test_codec_lz4() {
        test_codec_with_size(CodecType::LZ4);
    }

    #[test]
    fn test_codec_zstd() {
        test_codec_with_size(CodecType::ZSTD);
        test_codec_without_size(CodecType::ZSTD);
    }

    #[test]
    fn test_codec_lz4_raw() {
        test_codec_with_size(CodecType::LZ4_RAW);
    }
}

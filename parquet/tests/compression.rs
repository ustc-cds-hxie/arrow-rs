#![allow(dead_code)]
#![allow(unused_imports)]
#![feature(test)]
extern crate test;

use parquet::basic::Compression as CodecType;
use parquet::compression::Codec;
use parquet::{basic::Compression, compression::{create_codec, CodecOptionsBuilder}};
use byteorder::{ByteOrder, BigEndian};
use rand::{
    distributions::{uniform::SampleUniform, Distribution, Standard},
    thread_rng, Rng,
};

// q-compress
use q_compress::{auto_compress, auto_decompress, DEFAULT_COMPRESSION_LEVEL};

/// q-compress usage
/// 
/// ```rust
/// use q_compress::{auto_compress, auto_decompress, DEFAULT_COMPRESSION_LEVEL};
/// 
/// fn main() {
///     // your data
///     let mut my_ints = Vec::new();
///     for i in 0..100000 {
///       my_ints.push(i as i64);
///     }
///    
///     // Here we let the library choose a configuration with default compression
///     // level. If you know about the data you're compressing, you can compress
///     // faster by creating a `CompressorConfig`.
///     let bytes: Vec<u8> = auto_compress(&my_ints, DEFAULT_COMPRESSION_LEVEL);
///     println!("compressed down to {} bytes", bytes.len());
///    
///     // decompress
///     let recovered = auto_decompress::<i64>(&bytes).expect("failed to decompress");
///     println!("got back {} ints from {} to {}", recovered.len(), recovered[0], recovered.last().unwrap());
///   }
/// ```

/// convert an array of A into an array of u8
/// 
/// 1. string to u8 array (https://stackoverflow.com/questions/23850486/how-do-i-convert-a-string-into-a-vector-of-bytes-in-rust)
/// 
/// ```rust
/// &str to &[u8]:
/// 
/// let my_string: &str = "some string";
/// let my_bytes: &[u8] = my_string.as_bytes();
/// &str to Vec<u8>:
/// 
/// let my_string: &str = "some string";
/// let my_bytes: Vec<u8> = my_string.as_bytes().to_vec();
/// String to &[u8]:
/// 
/// let my_string: String = "some string".to_owned();
/// let my_bytes: &[u8] = my_string.as_bytes();
/// String to Vec<u8>:
/// 
/// let my_string: String = "some string".to_owned();
/// let my_bytes: Vec<u8> = my_string.into_bytes();
/// ```
/// 
/// 2. float to u8 array
/// 
/// 2.1 use byteorder to convert
/// 
/// Reads/Writes IEEE754 single-precision (4 bytes) floating point numbers from `src` into `dst`:
///  fn read_f32_into_unchecked(src: &[u8], dst: &mut [f32]) 
///  fn read_f64_into_unchecked(src: &[u8], dst: &mut [f32])
///  fn write_f32_into(src: &[f32], dst: &mut [u8])
///  fn write_f64_into(src: &[f32], dst: &mut [u8])
///
/// # Panics
///
/// Panics when `src.len() != 4*dst.len()`.
///
/// # Examples
///
/// Write and read `f32` numbers in little endian order:
///
/// ```rust
/// use byteorder::{ByteOrder, LittleEndian};
///
/// let mut bytes = [0; 16];
/// let numbers_given = [1.0, 2.0, 31.312e311, -11.32e91];
/// LittleEndian::write_f32_into(&numbers_given, &mut bytes);
///
/// let mut numbers_got = [0.0; 4];
/// unsafe {
///     LittleEndian::read_f32_into_unchecked(&bytes, &mut numbers_got);
/// }
/// assert_eq!(numbers_given, numbers_got);
/// ```
/// 
/// ```rust
/// use byteorder::{ByteOrder, LittleEndian};
///
/// let mut bytes = [0; 32];
/// let numbers_given = [1.0, 2.0, 31.312e311, -11.32e91];
/// LittleEndian::write_f64_into(&numbers_given, &mut bytes);
///
/// let mut numbers_got = [0.0; 4];
/// unsafe {
///     LittleEndian::read_f64_into_unchecked(&bytes, &mut numbers_got);
/// }
/// assert_eq!(numbers_given, numbers_got);
/// ```
/// 
/// 
/// 2.2 use std::slice to convert float array into u8 array (https://users.rust-lang.org/t/vec-f32-to-u8/21522/5 , https://stackoverflow.com/questions/29445026/converting-number-primitives-i32-f64-etc-to-byte-representations)
/// 
/// ```rust
/// // convert a float array into a u8 array
/// fn float32_to_byte_slice<'a>(floats: &'a [f32]) -> &'a [u8] {
///     unsafe {
///         std::slice::from_raw_parts(floats.as_ptr() as *const _, floats.len() * 4)
///     }
/// }
/// 
/// fn float64_to_byte_slice<'a>(floats: &'a [f64]) -> &'a [u8] {
///     unsafe {
///         std::slice::from_raw_parts(floats.as_ptr() as *const _, floats.len() * 8)
///     }
/// }
/// ```

use std::fmt::{Debug, Display};
trait NumberLike: Copy + Debug + Display + Default + PartialEq + 'static { }
impl NumberLike for u8 {}
impl NumberLike for u16 {}
impl NumberLike for u32 {}
impl NumberLike for u64 {}
impl NumberLike for i8 {}
impl NumberLike for i16 {}
impl NumberLike for i32 {}
impl NumberLike for i64 {}
impl NumberLike for f32 {}
impl NumberLike for f64 {}



// create a codec for compression / decompression
fn create_test_codec(codec: CodecType) -> Box<dyn Codec> {
    let codec_options = CodecOptionsBuilder::default()
        .set_backward_compatible_lz4(false)
        .build();
    let codec = match create_codec(codec, &codec_options) {
     Ok(Some(codec)) => codec,
     _ => panic!(),
    };
    codec
}

fn generate_test_data<T: NumberLike>(datasize: usize, data: &mut Vec<T>) where Standard: Distribution<T> {
    for _i in 0..datasize {
        data.push(rand::thread_rng().gen::<T>());
    }
}

fn random_bytes(n: usize) -> Vec<u8> {
    let mut result = vec![];
    let mut rng = thread_rng();
    for _ in 0..n {
        result.push(rng.gen_range(0..255));
    }
    result
}

fn random_numbers<T>(n: usize) -> Vec<T> where Standard: Distribution<T>
{
    let mut rng = thread_rng();
    Standard.sample_iter(&mut rng).take(n).collect()
}

fn do_qcom_compress<T: NumberLike>(c: CodecType) {
    let codec_options = CodecOptionsBuilder::default()
            .set_backward_compatible_lz4(false)
            .build();
    let mut c = create_codec(c, &codec_options).unwrap().unwrap();

    let mut compressed: Vec<u8> = Vec::new();

    let size = 100usize;

    match std::any::type_name::<T>() {
        "u8" => { panic!("QCOM cannot handle u8"); },
        "i8" => { panic!("QCOM cannot handle i8"); },

        "u16" => { 
            let mut data: Vec<u16> = Vec::new();
            let mut decompressed: Vec<u16> = Vec::new();
            
            generate_test_data(size, &mut data);
            c.compress_u16(&data, &mut compressed).expect("Error when qcom compressing");
            c.decompress_u16(&compressed, &mut decompressed, None).expect("Error when qcom decompressing");
            assert_eq!(decompressed, data);
        },
        "u32" => { 
            let mut data: Vec<u32> = Vec::new();
            let mut decompressed: Vec<u32> = Vec::new();
            
            generate_test_data(size, &mut data);
            c.compress_u32(&data, &mut compressed).expect("Error when qcom compressing");
            c.decompress_u32(&compressed, &mut decompressed, None).expect("Error when qcom decompressing");
            assert_eq!(decompressed, data);
        },
        "u64" => { 
            let mut data: Vec<u64> = Vec::new();
            let mut decompressed: Vec<u64> = Vec::new();
            
            generate_test_data(size, &mut data);
            c.compress_u64(&data, &mut compressed).expect("Error when qcom compressing");
            c.decompress_u64(&compressed, &mut decompressed, None).expect("Error when qcom decompressing");
            assert_eq!(decompressed, data);
        },
        "i16" => { 
            let mut data: Vec<i16> = Vec::new();
            let mut decompressed: Vec<i16> = Vec::new();
            
            generate_test_data(size, &mut data);
            c.compress_i16(&data, &mut compressed).expect("Error when qcom compressing");
            c.decompress_i16(&compressed, &mut decompressed, None).expect("Error when qcom decompressing");
            assert_eq!(decompressed, data);
        },
        "i32" => { 
            let mut data: Vec<i32> = Vec::new();
            let mut decompressed: Vec<i32> = Vec::new();
            
            generate_test_data(size, &mut data);
            c.compress_i32(&data, &mut compressed).expect("Error when qcom compressing");
            c.decompress_i32(&compressed, &mut decompressed, None).expect("Error when qcom decompressing");
            assert_eq!(decompressed, data);
        },
        "i64" => { 
            let mut data: Vec<i64> = Vec::new();
            let mut decompressed: Vec<i64> = Vec::new();
            
            generate_test_data(size, &mut data);
            c.compress_i64(&data, &mut compressed).expect("Error when qcom compressing");
            c.decompress_i64(&compressed, &mut decompressed, None).expect("Error when qcom decompressing");
            assert_eq!(decompressed, data);
        },
        "f32" => { 
            let mut data: Vec<f32> = Vec::new();
            let mut decompressed: Vec<f32> = Vec::new();
            
            generate_test_data(size, &mut data);
            c.compress_f32(&data, &mut compressed).expect("Error when qcom compressing");
            c.decompress_f32(&compressed, &mut decompressed, None).expect("Error when qcom decompressing");
            assert_eq!(decompressed, data);
        },
        "f64" => { 
            let mut data: Vec<f64> = Vec::new();
            let mut decompressed: Vec<f64> = Vec::new();
            
            generate_test_data(size, &mut data);
            c.compress_f64(&data, &mut compressed).expect("Error when qcom compressing");
            c.decompress_f64(&compressed, &mut decompressed, None).expect("Error when qcom decompressing");
            assert_eq!(decompressed, data);
        },
        _ => { panic!("QCOM cannot handle unknown type"); },
    }
}

fn test_roundtrip<T: NumberLike>(c: CodecType, _original_data: &Vec<T>, data: &[u8], uncompress_size: Option<usize>) {
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

fn test_codec_with_size<T: NumberLike>(c: CodecType) where Standard: Distribution<T> {
    let sizes = vec![100, 10000, 100000];
    
    for size in sizes {
        let mut data: Vec<T> = Vec::new();
        generate_test_data(size, &mut data);
        let data_u8 = random_bytes(size);
        match c {
            CodecType::SNAPPY | CodecType::GZIP | CodecType::BROTLI 
            | CodecType::LZ4 | CodecType::ZSTD | CodecType::LZ4_RAW => {
                test_roundtrip(c, &data, &data_u8, Some(data.len()));
            },
            CodecType::QCOM => {
                test_roundtrip(c, &data, &data_u8, Some(data.len()));
            },
            _ => {},
        }
        
    }
}

fn test_codec_without_size<T: NumberLike>(c: CodecType) where Standard: Distribution<T> {
    let sizes = vec![100, 10000, 100000];
    for size in sizes {
        let mut data: Vec<T> = Vec::new();
        generate_test_data(size, &mut data);
        let data_u8 = random_bytes(size);
        test_roundtrip(c, &data, &data_u8, None);
    }
}

#[test]
fn test_codec_snappy() {
    test_codec_with_size::<u8>(CodecType::SNAPPY);
    test_codec_without_size::<u8>(CodecType::SNAPPY);
    test_codec_with_size::<u64>(CodecType::SNAPPY);
    test_codec_without_size::<u64>(CodecType::SNAPPY);
}

#[test]
fn test_codec_gzip() {
    test_codec_with_size::<u8>(CodecType::GZIP);
    test_codec_without_size::<u8>(CodecType::GZIP);
    test_codec_with_size::<u64>(CodecType::GZIP);
    test_codec_without_size::<u64>(CodecType::GZIP);
}

#[test]
fn test_codec_brotli() {
    test_codec_with_size::<u8>(CodecType::BROTLI);
    test_codec_without_size::<u8>(CodecType::BROTLI);
    test_codec_with_size::<u64>(CodecType::BROTLI);
    test_codec_without_size::<u64>(CodecType::BROTLI);
}

#[test]
fn test_codec_lz4() {
    test_codec_with_size::<u8>(CodecType::LZ4);
    test_codec_with_size::<u64>(CodecType::LZ4);
}

#[test]
fn test_codec_zstd() {
    test_codec_with_size::<u8>(CodecType::ZSTD);
    test_codec_without_size::<u8>(CodecType::ZSTD);
    test_codec_with_size::<u64>(CodecType::ZSTD);
    test_codec_without_size::<u64>(CodecType::ZSTD);
}

#[test]
fn test_codec_lz4_raw() {
    test_codec_with_size::<u8>(CodecType::LZ4_RAW);
    test_codec_with_size::<u64>(CodecType::LZ4_RAW);
}

#[test]
fn test_codec_qcom() {
    do_qcom_compress::<u16>(CodecType::QCOM);
    do_qcom_compress::<u32>(CodecType::QCOM);
    do_qcom_compress::<u64>(CodecType::QCOM);
    do_qcom_compress::<i16>(CodecType::QCOM);
    do_qcom_compress::<i32>(CodecType::QCOM);
    do_qcom_compress::<i64>(CodecType::QCOM);
    do_qcom_compress::<f32>(CodecType::QCOM);
    do_qcom_compress::<f64>(CodecType::QCOM);
}

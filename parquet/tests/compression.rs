#![allow(dead_code)]
#![allow(unused_imports)]
#![feature(test)]
extern crate test;
extern crate log;

use log::{Level, debug};

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
///     debug!("compressed down to {} bytes", bytes.len());
///    
///     // decompress
///     let recovered = auto_decompress::<i64>(&bytes).expect("failed to decompress");
///     debug!("got back {} ints from {} to {}", recovered.len(), recovered[0], recovered.last().unwrap());
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

// DataTypeConstraint: to constrain what data types we can process
// convert_vec_to_vecbox_for_datatypeconstraint: convert Vec<DataTypeConstraint> to Vec<Box<dyn DataTypeConstraint>>
use parquet::data_type::{DataTypeConstraint, convert_vec_to_vecbox_for_datatypeconstraint};

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

fn generate_test_data<T: DataTypeConstraint>(datasize: usize, data: &mut Vec<T>) where Standard: Distribution<T> {
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

fn compare_two_vec_datatypeconstraint(
    lhs: &Vec<Box<dyn DataTypeConstraint>>, 
    rhs: &Vec<Box<dyn DataTypeConstraint>>) -> bool 
{
    let codec_options = CodecOptionsBuilder::default()
            .set_backward_compatible_lz4(false)
            .build();
    let mut c = create_codec(CodecType::SNAPPY, &codec_options).unwrap().unwrap();

    let mut lhs_u8 = Vec::new();
    let mut rhs_u8 = Vec::new();

    c.convert_to_u8(&*lhs[0], lhs, &mut lhs_u8).expect("convert_to_u8 failed");
    c.convert_to_u8(&*rhs[0], rhs, &mut rhs_u8).expect("convert_to_u8 failed");

    lhs_u8 == rhs_u8
}

fn do_qcom_compress<T: DataTypeConstraint + 'static + Copy>(c: CodecType) 
    where Standard: Distribution<T> 
{
    let codec_options = CodecOptionsBuilder::default()
            .set_backward_compatible_lz4(false)
            .build();
    let mut c1 = create_codec(c, &codec_options).unwrap().unwrap();

    let mut internal_data : Vec<T> = Vec::new();
    let mut data : Vec<Box<dyn DataTypeConstraint>> = Vec::new();
    let mut decompressed: Vec<Box<dyn DataTypeConstraint>> = Vec::new();
    let mut compressed: Vec<u8> = Vec::new();

    let size = 100usize;

    generate_test_data(size, &mut internal_data);
    convert_vec_to_vecbox_for_datatypeconstraint(&internal_data, &mut data);

    debug!("do_qcom_compress: \n\t codectype {:?} \n\t data type {:?} \n\t _origin_data {:?} \n\t data {:?}", 
                c, std::any::type_name::<T>(), internal_data, data);

    c1.compress(&*data[0], &data, &mut compressed).expect("Error when qcom compressing");
    c1.decompress(&compressed, &*data[0], &mut decompressed, None).expect("Error when qcom decompressing");
    
    debug!("do_qcom_compress: compressed {:?} {:?}", compressed.len(), compressed);
    debug!("do_qcom_compress: decompressed {:?} {:?}", decompressed.len(), decompressed);

    debug!("do_qcom_compress test if equal {:?} {:?} result {:?}", 
            data, decompressed.as_slice(),
            compare_two_vec_datatypeconstraint(&data, &decompressed)
        );
    assert_eq!(compare_two_vec_datatypeconstraint(&decompressed, &data), true);

}

fn test_roundtrip<T: DataTypeConstraint + 'static + Copy>(
    c: CodecType, 
    _original_data: &Vec<T>, 
    data: &Vec<Box<dyn DataTypeConstraint>>, 
    uncompress_size: Option<usize>) 
{

    debug!("test_roundtrip: \n\t codectype {:?} \n\t _origin_data {:?} \n\t data {:?} \n\t uncompress_size {:?}", 
                c, _original_data, data, uncompress_size);

    let codec_options = CodecOptionsBuilder::default()
            .set_backward_compatible_lz4(false)
            .build();
    let mut c1 = create_codec(c, &codec_options).unwrap().unwrap();
    let mut c2 = create_codec(c, &codec_options).unwrap().unwrap();

    // Compress with c1
    let mut compressed = Vec::new();
    let mut decompressed = Vec::new();

    debug!("test_roundtrip: start compressing data: {:?}", data);

    c1.compress(&*data[0], data, &mut compressed).expect("compress failed");

    debug!("test_roundtrip: compressed: {:?}", compressed);

    // Decompress with c2
    let decompressed_size = c2
        .decompress(compressed.as_slice(), &*data[0], &mut decompressed, uncompress_size)
        .expect("Error when decompressing");

    debug!("test_roundtrip: decompressed: {:?}", decompressed);

    debug!("test_roundtrip: data.len {:?} decompress_size {:?}", data.len(), decompressed_size);
    assert_eq!(data.len(), decompressed.len());

    debug!("test_roundtrip: test if equal {:?} {:?} result {:?}", 
            data, decompressed.as_slice(),
            compare_two_vec_datatypeconstraint(data, &decompressed)
        );

    assert_eq!(compare_two_vec_datatypeconstraint(data, &decompressed), true);

    decompressed.clear();
    compressed.clear();

    debug!("\n\n Now starting c2 compress\n");

    // Compress with c2
    c2.compress(&*data[0], data, &mut compressed)
        .expect("Error when compressing");

    debug!("\n\n Now starting c2 decompress\n");

    // Decompress with c1
    let _decompressed_size = c1
        .decompress(compressed.as_slice(), &*data[0], &mut decompressed, uncompress_size)
        .expect("Error when decompressing");

    debug!("\n\n Now starting c2 assert_eq! {:?} {:?}\n", data.len(), decompressed.len());

    assert_eq!(data.len(), decompressed.len());

    debug!("Now starting c2 test if equal {:?} {:?} result {:?}", 
            data, decompressed.as_slice(),
            compare_two_vec_datatypeconstraint(data, &decompressed)
        );

    assert_eq!(compare_two_vec_datatypeconstraint(data, &decompressed), true);

    decompressed.clear();
    compressed.clear();
}

fn test_codec_with_size<T: DataTypeConstraint + 'static + Copy>(c: CodecType) where Standard: Distribution<T> {
    // let sizes = vec![100, 10000, 100000];
    let sizes = vec![10];
    
    for size in sizes {
        let mut internal_data : Vec<T> = Vec::new();
        let mut data : Vec<Box<dyn DataTypeConstraint>> = Vec::new();

        debug!("sizeof {} {:?}", std::any::type_name::<T>().to_string(), std::mem::size_of::<T>().to_string());

        generate_test_data(size, &mut internal_data);

        debug!("internal_data: {:?} {:?}", internal_data.len(), internal_data);

        convert_vec_to_vecbox_for_datatypeconstraint(&internal_data, &mut data);

        debug!("data: {:?} {:?}", data.len(), data);

        debug!("compression_size {:?}", (internal_data.len() * std::mem::size_of::<T>()) as usize);

        match c {
            CodecType::SNAPPY | CodecType::GZIP | CodecType::BROTLI 
            | CodecType::LZ4 | CodecType::ZSTD | CodecType::LZ4_FRAME | CodecType::LZ4_RAW => {
                test_roundtrip(c, &internal_data, &data, Some((internal_data.len() * std::mem::size_of::<T>()) as usize));
                // test_roundtrip(c, &internal_data, &data, None);
            },
            _ => { assert_eq!(0, 1); },
        }
        
    }
}

#[test]
fn test_codec_snappy_u8() {
    test_codec_with_size::<u8>(CodecType::SNAPPY);
}
#[test]
fn test_codec_snappy_u64() {
    test_codec_with_size::<u64>(CodecType::SNAPPY);
}

#[test]
fn test_codec_gzip_u8() {
    test_codec_with_size::<u8>(CodecType::GZIP);
}
#[test]
fn test_codec_gzip_u64() {
    test_codec_with_size::<u64>(CodecType::GZIP);
}

#[test]
fn test_codec_brotli_u8() {
    test_codec_with_size::<u8>(CodecType::BROTLI);
}
#[test]
fn test_codec_brotli_u64() {
    test_codec_with_size::<u64>(CodecType::BROTLI);
}

#[test]
fn test_codec_lz4_u8() {
    test_codec_with_size::<u8>(CodecType::LZ4);
}
#[test]
fn test_codec_lz4_u64() {
    test_codec_with_size::<u64>(CodecType::LZ4);
}

#[test]
fn test_codec_zstd_u8() {
    test_codec_with_size::<u8>(CodecType::ZSTD);
}
#[test]
fn test_codec_zstd_u64() {
    test_codec_with_size::<u64>(CodecType::ZSTD);
}

#[test]
fn test_codec_lz4_frame_u8() {
    test_codec_with_size::<u8>(CodecType::LZ4_FRAME);
}
#[test]
fn test_codec_lz4_frame_u64() {
    test_codec_with_size::<u64>(CodecType::LZ4_FRAME);
}

#[test]
fn test_codec_lz4_raw_u8() {
    test_codec_with_size::<u8>(CodecType::LZ4_RAW);
}
#[test]
fn test_codec_lz4_raw_64() {
    test_codec_with_size::<u64>(CodecType::LZ4_RAW);
}

#[test]
fn test_codec_qcom_u16() {
    do_qcom_compress::<u16>(CodecType::QCOM);
}
#[test]
fn test_codec_qcom_u32() {
    do_qcom_compress::<u32>(CodecType::QCOM);
}
#[test]
fn test_codec_qcom_u64() {
    do_qcom_compress::<u64>(CodecType::QCOM);
}
#[test]
fn test_codec_qcom_i16() {
    do_qcom_compress::<i16>(CodecType::QCOM);
}
#[test]
fn test_codec_qcom_i32() {
    do_qcom_compress::<i32>(CodecType::QCOM);
}
#[test]
fn test_codec_qcom_i64() {
    do_qcom_compress::<i64>(CodecType::QCOM);
}
#[test]
fn test_codec_qcom_f32() {
    do_qcom_compress::<f32>(CodecType::QCOM);
}
#[test]
fn test_codec_qcom_f64() {
    do_qcom_compress::<f64>(CodecType::QCOM);
}
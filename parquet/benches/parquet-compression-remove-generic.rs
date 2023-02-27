/****************************************************************************
 * Copyright (c) 2023, Haiyong Xie
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not 
 * use this file except in compliance with the License. You may obtain a copy 
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   - Neither the name of the author nor the names of its contributors may be
 *     used to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER, AUTHOR OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ****************************************************************************/

//! Binary file to read data from a Parquet file.
//!
//! The binary can also be built from the source code and run as follows:
//! ```
//! export FILE_TO_COMPRESS="./data/devinrsmith-air-quality.20220714.zstd.parquet" 
//! export PARQUET_COLUMN="timestamp"
//! cargo bench --bench parquet-compression-remove-generic --features="arrow test_common experimental"
//! ```
//!

use criterion::{Criterion, black_box, criterion_group, criterion_main, PlotConfiguration, AxisScale, Throughput, BenchmarkId, BenchmarkGroup, measurement::WallTime};

extern crate parquet;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::data_type::DataTypeConstraint;
use parquet::basic::Compression as CodecType;
// use parquet::compression::Codec;
use parquet::compression::{create_codec, CodecOptionsBuilder};

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

// extract raw data from a given column of a parquet file
fn extract_column_data_from_parquet(input: &str, column: &str, output: &mut Vec<parquet::record::Field>) -> parquet::basic::Type {

	println!("input {:?}, column {:?}", input, column);

	let parquet_path = input;	
   	let file = File::open( Path::new(parquet_path) )
       .expect("Couldn't open parquet data");
       
	let reader:SerializedFileReader<File> = SerializedFileReader::new(file).unwrap();
	let parquet_metadata = reader.metadata();               
	
	// Writing the type signature here, to be super 
	// clear about the return type of get_fields()
	let fields:&[Arc<parquet::schema::types::Type>] = parquet_metadata
		.file_metadata()
		.schema()
		.get_fields(); 
	
	let mut p_type = parquet::basic::Type::INT64;
	let mut column_found = false;

	// find data type of the requested column
	for (_pos, col) in fields.iter().enumerate() {	       
		if col.name() == column {
			p_type = col.get_physical_type();
			column_found = true;
			break;
		}		
	} // for each column
	assert_eq!(column_found, true);

	let mut row_iter = reader.get_row_iter(None).unwrap();

	let mut columns : Vec<String> = Vec::new();
	columns.push(String::from(column));

	while let Some(record) = row_iter.next() {	
		// println!("{:?}", record);
		let mut column_iter = record.get_column_iter();
		while let Some(x) = column_iter.next() {
			if columns.contains(x.0) {
				// println!("{:?}", x);
				output.push(x.1.to_owned());
				// match x.1 {
				// 	parquet::record::Field::TimestampMicros(v) => output.push(*v),
				// 	parquet::record::Field::TimestampMillis(v) => output.push(*v),
				// 	parquet::record::Field::Long(v) => output.push(*v),
				// 	_ => {},
				// }
			}
		}
	}
	p_type
}

fn benchmark_parquet_compression(group: &mut BenchmarkGroup<WallTime>, alg_name: &str, ct: CodecType, orig_data: &Vec<Box<dyn DataTypeConstraint>>, data_u8_len: usize) {
	
	let mut compressed = Vec::new();
	let mut decompressed: Vec<Box<dyn DataTypeConstraint>> = Vec::new();

	// create codec
	let codec_options = CodecOptionsBuilder::default().set_backward_compatible_lz4(false).build();
    let mut c = create_codec(ct, &codec_options).unwrap().unwrap();

	group.throughput(Throughput::Elements(data_u8_len as u64));
    group.bench_function(BenchmarkId::new("pack", alg_name), |b| {
		let orig = orig_data;
        b.iter(|| {
            black_box(&mut compressed).clear();
			c.compress(&*orig[0], &orig, &mut compressed).expect("failed to compress");
        })
    });
    println!("{}: {} {} bytes compression_ratio {:.2}",
		alg_name, data_u8_len, compressed.len(),
		data_u8_len as f32 / compressed.len() as f32
	);

	group.throughput(Throughput::Elements(compressed.len() as u64));
    group.bench_function(BenchmarkId::new("unpack", alg_name), |b| {
        b.iter(|| {
            black_box(&mut decompressed).clear();
			c.decompress(&compressed, &*orig_data[0], &mut decompressed, Some(data_u8_len)).expect("failed to decompress");
        })
    });
}

fn compare_compress_generic(c: &mut Criterion) {
	// two benchmark groups
    let mut group = c.benchmark_group("compression");

	// plot config
	let plot_config_compress = PlotConfiguration::default()
													.summary_scale(AxisScale::Logarithmic);

    group.plot_config(plot_config_compress);

	// prepare raw data, load into memory

	// let uncompressed_u8 = std::fs::read(
	// 	std::env::var("FILE_TO_COMPRESS").expect("set $FILE_TO_COMPRESS")
	// ).expect("reading $FILE_TO_COMPRESS");

	// // q-compress
	// let i64_len = uncompressed_u8 / std::mem::size_of::<i64>();
	// let mut numbers_got: Vec<i64> = Vec::with_capacity(i64_len);
    // numbers_got.resize(i64_len, 0i64);

	// BigEndian::read_i64_into(&uncompressed_u8, &mut numbers_got);

	let mut uncompressed_field: Vec<parquet::record::Field> = Vec::new();

	let element_type = extract_column_data_from_parquet(
		// parquet file
		&std::env::var("FILE_TO_COMPRESS").expect("set $FILE_TO_COMPRESS"), 
		// column in parquet
		&std::env::var("PARQUET_COLUMN").expect("set $PARQUET_COLUMN"),
		// store data in a vec 
		&mut uncompressed_field);
	
	assert!(uncompressed_field.len() >= 1);

	let first_element = &uncompressed_field[0];

	println!("data type: {:?}", first_element);

	let mut orig_u8_len = 0;

	let mut uncompressed_data : Vec<Box<dyn DataTypeConstraint>> = Vec::new();

	// collect data
	for i in 0 .. uncompressed_field.len()  {
		let x = &uncompressed_field[i];
		match x {
			// 64 bit
			parquet::record::Field::TimestampMicros(v)
			| parquet::record::Field::TimestampMillis(v) 
			| parquet::record::Field::Long(v) => {
				uncompressed_data.push(Box::new(*v));
				orig_u8_len += 8;
			},
			parquet::record::Field::ULong(v) => {
				uncompressed_data.push(Box::new(*v));
				orig_u8_len += 8;
			},
			parquet::record::Field::Double(v) => {
				uncompressed_data.push(Box::new(*v));
				orig_u8_len += 8;
			},

			// 32 bit
			parquet::record::Field::Int(v)
			| parquet::record::Field::Date(v) => {
				uncompressed_data.push(Box::new(*v));
				orig_u8_len += 4;
			},
			parquet::record::Field::UInt(v) => {
				uncompressed_data.push(Box::new(*v));
				orig_u8_len += 4;
			},
			parquet::record::Field::Float(v) => {
				uncompressed_data.push(Box::new(*v));
				orig_u8_len += 4;
			},

			// 16 bit
			parquet::record::Field::Short(v) => {
				uncompressed_data.push(Box::new(*v));
				orig_u8_len += 2;
			},
			parquet::record::Field::UShort(v) => {
				uncompressed_data.push(Box::new(*v));
				orig_u8_len += 2;
			},

			// 8 bit
			// parquet::record::Field::Str(v) => { 
			// 	uncompressed_data.append(&mut v.as_bytes().to_vec());
			//  orig_u8_len += v.as_bytes().to_vec().len();
			// },

			// parquet::record::Field::Bytes(v) => {
			// 	uncompressed_data.append(&mut v.data().to_vec());
			//  orig_u8_len += v.data().to_vec().len();
			// },

			_ => {
				panic!("Error: column type mismatch. i {} x {:?} element_type {:?}", i, x, element_type);
			}

		}
	}

	println!("uncompressed_orig: {} items {} u8", uncompressed_data.len(), orig_u8_len);

	let all_codecs = vec![
		("SNAPPY", CodecType::SNAPPY),
		("GZIP", CodecType::GZIP),
		("BROTLI", CodecType::BROTLI),
		("LZ4", CodecType::LZ4),
		("ZSTD", CodecType::ZSTD),
		("LZ4_RAW", CodecType::LZ4_RAW),
		// ("LZ4_FRAME", CodecType::LZ4_FRAME),
		("QCOM", CodecType::QCOM),
	];

	for (alg_name, alg_ct) in all_codecs {
		benchmark_parquet_compression(&mut group, alg_name, alg_ct, &uncompressed_data,  orig_u8_len);
	}

	group.finish();

}

criterion_group!(
	name = benches;
	config = Criterion::default().sample_size(10);
	targets = compare_compress_generic);
criterion_main!(benches);
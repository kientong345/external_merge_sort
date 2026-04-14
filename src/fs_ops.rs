use std::{
    fs::File,
    io::{self, BufReader, Read, Seek},
};

use crate::model::ElementChunk;

pub fn fetch_chunk(filename: &str, count: usize, start_index: u64) -> io::Result<ElementChunk> {
    let file = File::open(filename)?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);

    let element_size = std::mem::size_of::<u64>() as u64;
    let offset = start_index * element_size;

    reader.seek(io::SeekFrom::Start(offset))?;

    let mut byte_buffer = Vec::with_capacity(count * element_size as usize);
    reader
        .take((count * element_size as usize) as u64)
        .read_to_end(&mut byte_buffer)?;

    let numbers: Vec<u64> = byte_buffer
        .chunks_exact(element_size as usize)
        .map(|chunk| u64::from_ne_bytes(chunk.try_into().unwrap()))
        .collect();

    Ok(ElementChunk::new(numbers))
}

pub fn store_chunk(chunk: ElementChunk, output_filename: &str) -> io::Result<()> {
    use std::io::{BufWriter, Write};
    let file = File::create(output_filename)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);

    let elements = &chunk.elements[chunk.read_cursor..];
    let bytes = unsafe {
        std::slice::from_raw_parts(
            elements.as_ptr() as *const u8,
            elements.len() * std::mem::size_of::<u64>(),
        )
    };
    writer.write_all(bytes)?;
    Ok(())
}

pub fn append_chunk(chunk: ElementChunk, output_filename: &str) -> io::Result<()> {
    use std::fs::OpenOptions;
    use std::io::{BufWriter, Write};
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_filename)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);

    let elements = &chunk.elements[chunk.read_cursor..];
    let bytes = unsafe {
        std::slice::from_raw_parts(
            elements.as_ptr() as *const u8,
            elements.len() * std::mem::size_of::<u64>(),
        )
    };
    writer.write_all(bytes)?;
    Ok(())
}

pub fn delete_file(filename: &str) -> io::Result<()> {
    std::fs::remove_file(filename)
}

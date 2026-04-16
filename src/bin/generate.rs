use std::io::{BufWriter, Write};

use rand::RngExt;

const OUTPUT_FILE: &str = "data.bin";
const NUM_ELEMENTS: u64 = 4_000_000_000; // 4B elements
const CHUNK_SIZE: usize = 400_000_000; // 400M elements

fn main() {
    let mut rng = rand::rng();
    let file = std::fs::File::create(OUTPUT_FILE).expect("Failed to create file");
    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);

    let mut buffer: Vec<u16> = Vec::with_capacity(CHUNK_SIZE);

    let total_chunks = (NUM_ELEMENTS + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64;

    println!(
        "Generating {} random u16 numbers (~{} GB)...",
        NUM_ELEMENTS,
        NUM_ELEMENTS * 2 / 1_000_000_000
    );

    for chunk_idx in 0..total_chunks {
        let remaining = NUM_ELEMENTS - chunk_idx * CHUNK_SIZE as u64;
        let count = remaining.min(CHUNK_SIZE as u64) as usize;

        buffer.clear();
        buffer.resize(count, 0);
        rng.fill(&mut buffer[..]);

        let bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(buffer.as_ptr() as *const u8, count * size_of::<u16>())
        };
        writer.write_all(bytes).expect("Failed to write to file");

        println!(
            "Progress: {}/{} chunks ({:.1}%)",
            chunk_idx + 1,
            total_chunks,
            (chunk_idx + 1) as f64 / total_chunks as f64 * 100.0
        );
    }

    writer.flush().expect("Failed to flush writer");

    println!(
        "Successfully generated {} random numbers in {}",
        NUM_ELEMENTS, OUTPUT_FILE
    );
}

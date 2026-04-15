use std::cmp::Reverse;
use std::collections::BinaryHeap;

use huge_sort::fs_ops::{append_chunk, delete_file, fetch_chunk, store_chunk};
use huge_sort::model::ElementChunk;
use threadpool::ThreadPool;

const INPUT_FILE: &str = "data.bin";
const OUTPUT_FILE: &str = "sorted_output.bin";
const CHUNK_SIZE: usize = 100_000_000; // 800MB
const CHUNK_BUFFER_SIZE: usize = 10_000_000; // 80MB
const WRITE_BUFFER_SIZE: usize = 100_000_000; // 800MB

const TMP_CHUNK_PREFIX: &str = "tmp_chunk_";

fn main() {
    println!("Phase 1: Reading and Sorting Chunks...");
    let chunk_count = chunk_and_sort();

    println!("Total chunks generated: {}", chunk_count);
    if chunk_count == 0 {
        println!("No data to process.");
        return;
    }

    println!("Phase 2: K-Way Merge Sorting...");
    merge_chunks(chunk_count);

    println!("Sorting completed successfully!");

    // print first 100 elements of OUTPUT_FILE
    let chunk = fetch_chunk(OUTPUT_FILE, 100, 0).expect("Failed to read chunk");
    println!("First 100 elements: {:?}", chunk.elements);
}

fn chunk_and_sort() -> usize {
    let pool = ThreadPool::new(16);

    let mut chunk_count = 0;
    let mut current_idx = 0;

    while let Ok(mut chunk) = fetch_chunk(INPUT_FILE, CHUNK_SIZE, current_idx) {
        if chunk.is_empty() {
            break;
        }

        let output_filename = format!("{}{}.bin", TMP_CHUNK_PREFIX, chunk_count);
        pool.execute(move || {
            chunk.sort();
            if let Err(e) = store_chunk(chunk, &output_filename) {
                eprintln!("Error processing chunk {}: {}", current_idx, e);
            }
        });

        chunk_count += 1;
        current_idx += CHUNK_SIZE as u64;

        // prevent queue overflow
        while pool.queued_count() > 32 {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    // wait until all tasks are done
    pool.join();

    chunk_count
}

fn merge_chunks(chunk_count: usize) {
    let mut chunk_buf: Vec<ElementChunk> = Vec::new();
    let mut tmp_file_cursors: Vec<u64> = vec![0; chunk_count];

    for i in 0..chunk_count {
        let filename = format!("{}{}.bin", TMP_CHUNK_PREFIX, i);
        let chunk = fetch_chunk(&filename, CHUNK_BUFFER_SIZE, 0).expect("Failed to read chunk");
        let read_len = chunk.len() as u64;
        chunk_buf.push(chunk);
        tmp_file_cursors[i] = read_len;
    }

    let mut min_heap: BinaryHeap<Reverse<(u64, usize)>> = BinaryHeap::new();

    for (i, chunk) in chunk_buf.iter_mut().enumerate() {
        if let Some(value) = chunk.pop_front() {
            min_heap.push(Reverse((value, i)));
        }
    }

    let mut write_buffer: Vec<u64> = Vec::with_capacity(WRITE_BUFFER_SIZE);

    let _ = delete_file(OUTPUT_FILE);

    while let Some(Reverse((value, chunk_idx))) = min_heap.pop() {
        write_buffer.push(value);

        if let Some(val) = chunk_buf[chunk_idx].pop_front() {
            min_heap.push(Reverse((val, chunk_idx)));
        } else {
            // chunk_buf[chunk_idx] exhausted, fetch next chunk base on tmp_file_cursors[chunk_idx]
            let filename = format!("{}{}.bin", TMP_CHUNK_PREFIX, chunk_idx);
            let next_offset = tmp_file_cursors[chunk_idx];

            if let Ok(mut new_chunk) = fetch_chunk(&filename, CHUNK_BUFFER_SIZE, next_offset) {
                if !new_chunk.is_empty() {
                    tmp_file_cursors[chunk_idx] += new_chunk.len() as u64;
                    let val = new_chunk.pop_front().unwrap();
                    chunk_buf[chunk_idx] = new_chunk;
                    min_heap.push(Reverse((val, chunk_idx)));
                }
            }
        }

        if write_buffer.len() >= WRITE_BUFFER_SIZE {
            let buffer_to_write =
                std::mem::replace(&mut write_buffer, Vec::with_capacity(WRITE_BUFFER_SIZE));
            append_chunk(ElementChunk::new(buffer_to_write), OUTPUT_FILE)
                .expect("Failed to write output");
        }
    }

    // flush write buffer
    if !write_buffer.is_empty() {
        append_chunk(ElementChunk::new(write_buffer), OUTPUT_FILE).expect("Failed to write output");
    }

    // Clean up temporary files
    for i in 0..chunk_count {
        let filename = format!("{}{}.bin", TMP_CHUNK_PREFIX, i);
        let _ = huge_sort::fs_ops::delete_file(&filename);
    }
}

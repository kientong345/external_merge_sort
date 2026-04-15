use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use huge_sort::fs_ops;
use huge_sort::model::ElementChunk;
use threadpool::ThreadPool;

const INPUT_FILE: &str = "data.bin";
const OUTPUT_FILE: &str = "sorted_output.bin";
const CHUNK_SIZE: usize = 100_000_000; // 100M elements (~800MB)
const CHUNK_BUFFER_SIZE: usize = 10_000_000; // 10M elements (~80MB)
const WRITE_BUFFER_SIZE: usize = 100_000_000; // 100M elements (~800MB)

const TMP_CHUNK_PREFIX: &str = "bench_chunk_";

static IO_NANOS: AtomicU64 = AtomicU64::new(0);

fn timed_fetch_chunk(filename: &str, count: usize, start_index: u64) -> io::Result<ElementChunk> {
    let t = Instant::now();
    let result = fs_ops::fetch_chunk(filename, count, start_index);
    IO_NANOS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    result
}

fn timed_store_chunk(chunk: ElementChunk, output_filename: &str) -> io::Result<()> {
    let t = Instant::now();
    let result = fs_ops::store_chunk(chunk, output_filename);
    IO_NANOS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    result
}

fn timed_append_chunk(chunk: ElementChunk, output_filename: &str) -> io::Result<()> {
    let t = Instant::now();
    let result = fs_ops::append_chunk(chunk, output_filename);
    IO_NANOS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    result
}

fn reset_io_counters() {
    IO_NANOS.store(0, Ordering::Relaxed);
}

fn io_secs() -> f64 {
    IO_NANOS.load(Ordering::Relaxed) as f64 / 1_000_000_000.0
}

fn format_secs(s: f64) -> String {
    if s >= 1.0 {
        format!("{:.3}s", s)
    } else {
        format!("{:.3}ms", s * 1000.0)
    }
}

fn main() {
    println!("===================================================");
    println!("  External Merge Sort - Benchmark");
    println!(
        "  Input: {}  |  Chunk: {}M  |  Buffer: {}M",
        INPUT_FILE,
        CHUNK_SIZE / 1_000_000,
        CHUNK_BUFFER_SIZE / 1_000_000,
    );
    println!("===================================================\n");

    // phase 1
    reset_io_counters();
    println!("Phase 1: Reading and Sorting Chunks...");
    let p1_start = Instant::now();
    let chunk_count = chunk_and_sort();
    let p1_total = p1_start.elapsed().as_secs_f64();
    let p1_io = io_secs();
    let p1_cpu = p1_total - p1_io;

    println!("  Chunks generated: {}", chunk_count);
    println!("      + I/O Time:    {}", format_secs(p1_io));
    println!("      + CPU Time:    {}", format_secs(p1_cpu));
    println!("      + Total:       {}\n", format_secs(p1_total));

    if chunk_count == 0 {
        println!("No data to process.");
        return;
    }

    // phase 2
    reset_io_counters();
    println!("Phase 2: K-Way Merge Sorting...");
    let p2_start = Instant::now();
    merge_chunks(chunk_count);
    let p2_total = p2_start.elapsed().as_secs_f64();
    let p2_io = io_secs();
    let p2_cpu = p2_total - p2_io;

    println!("      + I/O Time:    {}", format_secs(p2_io));
    println!("      + CPU Time:    {}", format_secs(p2_cpu));
    println!("      + Total:       {}\n", format_secs(p2_total));

    // summary
    let total_time = p1_total + p2_total;
    let total_io = p1_io + p2_io;
    let total_cpu = total_time - total_io;

    println!("===================================================");
    println!("  SUMMARY");
    println!("===================================================");
    println!("  Phase 1 (Sort):    {}", format_secs(p1_total));
    println!("  Phase 2 (Merge):   {}", format_secs(p2_total));
    println!("===================================================");
    println!(
        "  Total I/O Time:    {}  ({:.1}%)",
        format_secs(total_io),
        total_io / total_time * 100.0
    );

    println!(
        "  Total CPU Time:    {}  ({:.1}%)",
        format_secs(total_cpu),
        total_cpu / total_time * 100.0
    );
    println!("===================================================");
    println!("  Total Wall Time:   {}", format_secs(total_time));
    println!("===================================================");
}

fn chunk_and_sort() -> usize {
    let pool = ThreadPool::new(16);

    let mut chunk_count = 0;
    let mut current_idx: u64 = 0;

    while let Ok(mut chunk) = timed_fetch_chunk(INPUT_FILE, CHUNK_SIZE, current_idx) {
        if chunk.is_empty() {
            break;
        }

        let output_filename = format!("{}{}.bin", TMP_CHUNK_PREFIX, chunk_count);
        pool.execute(move || {
            chunk.sort();
            if let Err(e) = timed_store_chunk(chunk, &output_filename) {
                eprintln!("Error processing chunk {}: {}", current_idx, e);
            }
        });

        chunk_count += 1;
        current_idx += CHUNK_SIZE as u64;

        while pool.queued_count() > 32 {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    pool.join();
    chunk_count
}

fn merge_chunks(chunk_count: usize) {
    let mut chunk_buf: Vec<ElementChunk> = Vec::new();
    let mut tmp_file_cursors: Vec<u64> = vec![0; chunk_count];

    for i in 0..chunk_count {
        let filename = format!("{}{}.bin", TMP_CHUNK_PREFIX, i);
        let chunk =
            timed_fetch_chunk(&filename, CHUNK_BUFFER_SIZE, 0).expect("Failed to read chunk");
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
    let _ = fs_ops::delete_file(OUTPUT_FILE);

    while let Some(Reverse((value, chunk_idx))) = min_heap.pop() {
        write_buffer.push(value);

        if let Some(val) = chunk_buf[chunk_idx].pop_front() {
            min_heap.push(Reverse((val, chunk_idx)));
        } else {
            let filename = format!("{}{}.bin", TMP_CHUNK_PREFIX, chunk_idx);
            let next_offset = tmp_file_cursors[chunk_idx];

            if let Ok(mut new_chunk) = timed_fetch_chunk(&filename, CHUNK_BUFFER_SIZE, next_offset)
            {
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
            timed_append_chunk(ElementChunk::new(buffer_to_write), OUTPUT_FILE)
                .expect("Failed to write output");
        }
    }

    if !write_buffer.is_empty() {
        timed_append_chunk(ElementChunk::new(write_buffer), OUTPUT_FILE)
            .expect("Failed to write output");
    }

    for i in 0..chunk_count {
        let filename = format!("{}{}.bin", TMP_CHUNK_PREFIX, i);
        let _ = fs_ops::delete_file(&filename);
    }
}

# huge-sort

**External Merge Sort** — Sắp xếp hàng trăm triệu phần tử `u64` (3.2 GB) trên đĩa khi dữ liệu vượt quá dung lượng RAM, được viết bằng Rust.

![External Merge Sort Workflow](docs/workflow_diagram.png)

## Tổng quan

Chương trình thực hiện thuật toán **External Merge Sort** để sắp xếp một file nhị phân chứa 400 triệu số `u64` (~3.2 GB). Thay vì load toàn bộ dữ liệu vào RAM, dữ liệu được chia thành các chunk nhỏ, sắp xếp từng chunk trong bộ nhớ, sau đó merge chúng lại bằng **K-Way Merge** với Min-Heap.

## Workflow

### Phase 0 — Tạo dữ liệu (`generate.rs`)

File `generate.rs` tạo ra file `data.bin` chứa **400 triệu** số `u64` ngẫu nhiên (~3.2 GB) bằng `std::mt19937_64`:

```bash
cargo run --bin generate
# → data.bin (3,200,000,000 bytes)
```

### Phase 1 — Chia chunk & Sắp xếp song song

Dữ liệu trong `data.bin` được đọc thành nhiều **chunk**, mỗi chunk chứa **100 triệu phần tử** (~800 MB). Mỗi chunk được đẩy vào một **thread pool gồm 16 threads** để sắp xếp song song:

```
data.bin ──► [Chunk 0: 100M elements] ──► Thread Pool ──► tmp_chunk_0.bin (sorted)
             [Chunk 1: 100M elements] ──► Thread Pool ──► tmp_chunk_1.bin (sorted)
             [Chunk 2: 100M elements] ──► Thread Pool ──► tmp_chunk_2.bin (sorted)
             [Chunk 3: 100M elements] ──► Thread Pool ──► tmp_chunk_3.bin (sorted)
```

**Chi tiết:**
1.  Đọc 100M phần tử `u64` từ `data.bin` tại offset tương ứng (`fetch_chunk`).
2.  Sắp xếp chunk trong bộ nhớ bằng `sort_unstable()` (Introsort, không cần stable).
3.  Ghi chunk đã sắp xếp ra file tạm `tmp_chunk_{i}.bin` (`store_chunk`).
4.  Cơ chế **backpressure**: nếu hàng đợi thread pool vượt quá 32 task, main thread sẽ sleep để tránh OOM.

### Phase 2 — K-Way Merge với Min-Heap

Sau khi tất cả chunk đã được sắp xếp, chương trình thực hiện **K-Way Merge** bằng `BinaryHeap<Reverse<(u64, usize)>>` (Min-Heap):

```
tmp_chunk_0.bin ──► [Buffer 0: 10M elements] ──┐
tmp_chunk_1.bin ──► [Buffer 1: 10M elements] ──┤
tmp_chunk_2.bin ──► [Buffer 2: 10M elements] ──├──► Min-Heap ──► Write Buffer (100M) ──► sorted_output.bin
tmp_chunk_3.bin ──► [Buffer 3: 10M elements] ──┘
```

**Chi tiết:**
1.  Mỗi file chunk tạm được đọc vào một **buffer** gồm 10M phần tử (`CHUNK_BUFFER_SIZE`).
2.  Phần tử nhỏ nhất từ mỗi buffer được đẩy vào **Min-Heap**.
3.  Lặp lại:
    -   Pop phần tử nhỏ nhất từ heap → đẩy vào **write buffer**.
    -   Đẩy phần tử tiếp theo từ buffer tương ứng vào heap.
    -   Nếu buffer cạn → đọc thêm 10M phần tử tiếp theo từ file chunk tạm (lazy loading theo `tmp_file_cursors`).
    -   Khi write buffer đầy (100M phần tử, ~800 MB) → flush ra `sorted_output.bin`.
4.  Sau khi merge xong, xả write buffer còn lại và **dọn dẹp** tất cả file tạm.

### Kết quả

```
Phase 1: Reading and Sorting Chunks...
Total chunks generated: 4
Phase 2: K-Way Merge Sorting...
Sorting completed successfully!
First 100 elements: [31810656370, 36625493116, 109530701186, ...]
```

## Cấu trúc dự án

```
huge-sort/
├── Cargo.toml          # Cấu hình project Rust
└── src/
    ├── bin/algorithm.rs         # thuật toán chính
    ├── bin/generate.rs         # tạo dữ liệu
    ├── bin/benchmark.rs         # benchmark
    ├── lib.rs          # Re-export modules
    ├── model.rs        # ElementChunk — cấu trúc dữ liệu deque-like cho chunk
    └── fs_ops.rs       # I/O operations: fetch, store, append, delete
```

## Các tham số cấu hình

| Hằng số              | Giá trị         | Ý nghĩa                                       |
|-----------------------|-----------------|------------------------------------------------|
| `CHUNK_SIZE`          | 100,000,000     | Số phần tử mỗi chunk (~800 MB)                |
| `CHUNK_BUFFER_SIZE`   | 10,000,000      | Kích thước buffer đọc trong merge (~80 MB)     |
| `WRITE_BUFFER_SIZE`   | 100,000,000     | Kích thước write buffer trước khi flush (~800 MB) |
| Thread Pool Size      | 16              | Số thread song song cho Phase 1                |

## Dependencies

-   **[threadpool](https://crates.io/crates/threadpool)** `1.0` — Thread pool cho sắp xếp song song.

## Chạy chương trình

```bash
# 1. Tạo dữ liệu
cargo run --bin generate

# 2. Build & chạy external merge sort
cargo run --bin algorithm
```

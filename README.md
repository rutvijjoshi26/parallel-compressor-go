# Parallel Compressor (Go)

This repository contains a small research/educational parallel compressor written in Go. It implements a simple block-based compression format with an LZ77-style token stream and three implementations:

- `seq` — Sequential, single-threaded compressor/decompressor.
- `bsp` — Bulk Synchronous Parallel (static partitioning of blocks across workers).
- `ws`  — Work-stealing implementation using a Chase–Lev deque for dynamic load balancing.

The compressor is intended for experimentation and benchmarking of parallel strategies rather than production use.

---

## Highlights / Features

- Custom `.pcz` file format with a small file header and per-block compressed sizes.
- LZ77-like tokenization with a 64KB sliding window and short-match optimization.
- Block-level decision: store block as raw (mode `0xFF`) or LZ tokens (mode `0x00`) depending on which is smaller.
- Multiple parallelization strategies (BSP static partitions and work-stealing dynamic scheduling).
- Small, self-contained implementation with no external Go dependencies (Go 1.19).

---

## Build / Requirements

- Go 1.19 or newer
- Python 3 (optional, for the benchmarking script)
- `matplotlib` (optional, for plotting benchmark results)

To build the CLI binary (`pczip` used by the benchmark script):

```bash
cd "/Users/rutvijjoshi/Projects/Parallel compression"
go build -o pczip main.go
```

You can also run directly with `go run` during development:

```bash
go run main.go -mode compress -in input.bin -out output.pcz -impl ws -threads 4
```

---

## Usage

The program is a CLI with flags:

- `-mode` : `compress` or `decompress`
- `-in`   : input file path
- `-out`  : output file path
- `-impl` : implementation (`seq`, `bsp`, or `ws`) — default `seq`
- `-threads`: number of worker threads for parallel implementations (default `4`)

Examples

Compress with the sequential implementation:

```bash
go run main.go -mode compress -in sample.bin -out sample.pcz -impl seq
```

Compress with work-stealing (8 workers):

```bash
go run main.go -mode compress -in sample.bin -out sample_ws.pcz -impl ws -threads 8
```

Decompress (uses the same `impl` flags; any implementation will yield the same result):

```bash
go run main.go -mode decompress -in sample_ws.pcz -out sample_restored.bin -impl seq
```

Verify integrity (quick approach on macOS/Linux):

```bash
shasum -a 256 sample.bin sample_restored.bin
# or
md5 sample.bin sample_restored.bin
```

---

## File format (brief)

- Magic: 4 bytes `PCZ2` to identify the file.
- Filename length (uint16), original file size (uint64), filename bytes.
- Block size (uint32), number of blocks (uint64), then `NumBlocks` compressed-size entries (uint64 each).
- Followed by the concatenated compressed block payloads. Each block payload begins with a mode byte:
  - `0xFF` — raw (uncompressed) block bytes follow
  - `0x00` — LZ token stream follows (see `core/lz.go`)

This layout makes it easy to parallelize compression and decompression by operating on blocks independently.

---

## Design Notes

- LZ implementation: simple hash-table based LZ77 that looks for 4-byte anchors and extends matches.
- Block-level heuristic: if the LZ tokens are not smaller than the raw block, the block is stored raw (avoids inflation).
- BSP strategy: partitions blocks into contiguous chunks and lets each worker process its assigned chunk.
- Work-stealing strategy: uses a Chase–Lev deque implementation (`core/wsdeque.go`) to distribute block indices dynamically among workers; this often yields better load balance on heterogeneous or fragmented data.
- Barrier primitive (`core/barrier.go`) is used for simple synchronization where needed.

---

## Benchmarking

A benchmark script `benchmark.py` is included to generate datasets and measure speedups for the parallel implementations. It expects the built CLI to be named `pczip` in the repo root (the script builds it if needed).

To run the benchmark (may take significant time and disk space depending on configured dataset sizes):

```bash
# install python deps (matplotlib for plotting)
python3 -m pip install --user matplotlib

# build the binary used by the script
go build -o pczip main.go

# run the benchmark
python3 benchmark.py
```

The script generates datasets, runs `seq` to get a baseline, then runs `bsp` and `ws` with various thread counts, verifies integrity (decompress with `seq`) and produces PNG plots of speedups.

---

## Project layout

- `main.go`          — CLI entrypoint and flag parsing
- `go.mod`           — module file (module `proj3`)
- `core/`            — core compression implementation
  - `lz.go`          — LZ tokenization and decompression
  - `format.go`      — file header read/write
  - `sequential.go`  — sequential compressor/decompressor
  - `bsp.go`         — BSP-style parallel implementation
  - `worksteal.go`   — work-stealing parallel implementation
  - `wsdeque.go`     — Chase–Lev deque used by work-stealing
  - `barrier.go`     — small barrier synchronization primitive
- `benchmark.py`     — Python benchmarking / dataset generators

---

## Limitations & Caveats

- Not intended to be a fully hardened/production compressor — edge-cases and file-system errors are handled conservatively but there is no fuzzer-tested stability guarantee.
- Memory usage: parallel implementations may load the whole input into memory (BSP/WS), which increases peak memory compared to streaming sequential mode.
- The LZ matcher and token encoding are simple and aimed at teaching/experimentation rather than optimal compression ratio.

---

## Contributing

Contributions are welcome. If you plan to submit pull requests, consider:

- Adding tests for new features (small sample files, integrity checks).
- Keeping changes scoped and adding documentation to `README.md`.

If you'd like, I can add a `CONTRIBUTING.md` and a few basic unit tests.

---

## License

This project currently has no explicit license file. If you want to make it public, consider adding an open-source license such as MIT or Apache-2.0. Tell me which license to add and I will create a `LICENSE` file.

---

If you'd like, I can also:

- Commit this `README.md` to the repo and push it.
- Add a `.gitignore` tuned for Go builds and Python artifacts.
- Add a simple GitHub Actions CI workflow that runs `go test` (if/when tests are present) or `go vet`/`golangci-lint`.

Tell me which of the above you'd like me to do next.

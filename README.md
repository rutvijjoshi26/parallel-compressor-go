# pczip: a parallel block compressor in Go

[![CI](https://github.com/rutvijjoshi26/parallel-compressor-go/actions/workflows/ci.yml/badge.svg)](https://github.com/rutvijjoshi26/parallel-compressor-go/actions/workflows/ci.yml)

pczip splits its input into independent blocks, then compresses and
decompresses them in parallel with bounded memory. It has two block
schedulers, static partitioning and Chase–Lev work stealing, and an
LZ4-style codec written from scratch. Blocks go into a checksummed
container format, and the project uses only the Go standard library. The
decoder is fuzzed against hostile input, the concurrent code runs under the
race detector, and a failed run that writes to a file leaves no partial
output behind.

It is a systems exercise, not a zstd replacement. The results below say
plainly where it wins and where it loses.

## Results

On one core, `zstd -1` beats pczip on ratio, compression speed and
decompression speed for every dataset. pczip beats `gzip -1` on speed: it
compresses 2.4–4.2× faster and decompresses 1.9–2.2× faster. Its output is
22–39% larger than gzip's.

**One core each:** pczip `-workers 1`, single-threaded `gzip`, and
`zstd -T1`. Decompression is single-threaded for all three. Each dataset is
256 MiB and each figure is the median of 3 runs. MB/s means uncompressed
bytes per second, for compression and decompression alike.

| dataset | ratio: pczip / gzip -1 / zstd -1 | compress MB/s: pczip / gzip -1 / zstd -1 | decompress MB/s: pczip / gzip -d / zstd -d |
|---|---|---|---|
| text | 1.566 / 2.179 / 2.444 | 146 / 48 / 177 | 204 / 109 / 497 |
| logs | 3.735 / 5.118 / 6.108 | 238 / 101 / 330 | 391 / 186 / 665 |
| mixed | 1.697 / 2.074 / 2.225 | 210 / 50 / 270 | 296 / 136 / 709 |

- Against `zstd -1` on one core, zstd compresses 1.2–1.4× faster and
  decompresses 1.7–2.4× faster. pczip's output is 31–64% larger. pczip has
  no entropy coding stage, so a lower ratio is expected.
- pczip has the lowest ratio of all the tools on all three datasets.

**Two cores (pczip `-workers 2`).** These rows are not like-for-like with
the gzip rows: gzip is single-threaded, and pigz was not installed, so
multi-core gzip was not measured. `zstd -T0` used both cores to compress.
Every zstd and gzip decompression ran on one core.

| dataset | compress MB/s: pczip 2 cores / zstd -1 -T0 2 cores / gzip -1 1 core | decompress MB/s: pczip 2 cores / zstd -d 1 core / gzip -d 1 core |
|---|---|---|
| text | 271 / 309 / 48 | 363 / 497 / 109 |
| logs | 436 / 530 / 101 | 650 / 665 / 186 |
| mixed | 377 / 444 / 50 | 486 / 709 / 136 |

- Going from 1 to 2 workers, pczip compression gains 1.79–1.86× and
  decompression gains 1.64–1.78×.
- zstd on 2 cores still compresses faster than pczip on 2 cores on all
  three datasets. Even on one core, zstd decompresses `text` and `mixed`
  faster than pczip does on two. On `logs` they are about equal: 650 vs
  665 MB/s, and the ranges overlap.

**Machine and caveats:**

- **Machine:** an AWS EC2 t4g.medium, which has 2 vCPUs (Graviton2, arm64).
  It is a burstable instance.
- **Throttling:** sustained probe runs before the final measurements held
  steady within ±3%.
- **Core count:** scaling beyond 2 cores was not measured. Nothing here says
  how pczip behaves on 4, 8 or more cores.
- **Data:** the datasets are synthetic: English-like text, JSON logs, and a
  mix of text, random bytes and zeros. They show how the tools compare on
  these shapes of data, not what you will get on your own files.

[docs/benchmarks.md](docs/benchmarks.md) has every configuration, with
min–max ranges, raw wall times, dataset hashes and the reproduction
commands.

## How it works

```
                  up to Workers x 4 blocks per batch
input -> reader -> batch -> scheduler -> encode (one block per task) -> ordered writer -> output
                            static | steal       in parallel            frames in input order
```

The compressor reads up to `Workers × 4` blocks (1 MiB each by default) into
a batch. A scheduler hands the block indices to the worker goroutines. Each
worker encodes its blocks and computes their checksums into per-index slots.
The writer then emits the frames in input order. Decompression has the same
shape: it reads a batch of frames, decodes and checks them in parallel, and
writes the results in order.

**Codec.** The codec is LZ4-style: a 4-byte minimum match, a 16K-entry hash
table, offsets up to 64 KiB, and LZ4's skip acceleration on runs of misses.
Each block is encoded on its own. The codec is LZ4-style but not
LZ4-compatible, so lz4 tools cannot read its output.

### Container format (PCZ3)

All integers are little-endian.

- **Header:** magic `"PCZ3"` (4 bytes), version `u8 = 3`, flags `u8 = 0`,
  block size `u32`.
- **Frames:** one per block: `rawLen u32`, `compLen u32`, `crc32 u32` (IEEE,
  over the raw bytes), then `compLen` payload bytes.
- **End frame:** the stream ends with a frame where `rawLen = 0` and
  `compLen = 0` (crc 0, no payload).
- **Payload:** the first byte is the mode. `0x00` means an LZ4-style
  sequence stream follows; `0xFF` means raw bytes follow.
- **Block size:** the default is 1 MiB. The valid range is 4 KiB to 64 MiB,
  and a reader rejects any value outside it.

**The decoder never** panics, and when it sizes a buffer from numbers it
read from the stream, no single allocation exceeds `blockSize + 64` bytes.
It rejects all of the following:

- `rawLen > blockSize`;
- `compLen > blockSize + 64`;
- a CRC mismatch;
- a truncated stream, a missing end frame, or trailing bytes after the end
  frame.

## Design decisions and trade-offs

**Independent blocks.** No block refers to another block's bytes. LZ4
frames in block-independent mode and `pigz -i` work the same way. Plain
pigz and `zstd -T` also compress in parallel, but they chain each block to
the data before it, so decompression is serial. Because pczip's blocks are
independent, they also decompress in parallel. Independence also makes the
output byte-identical for every worker count and scheduler; the tests check
this, and so did every benchmark run. The cost is ratio: no match can reach
into another block, and every block starts with an empty hash table.

**Raw fallback.** A block is stored raw (mode `0xFF`) unless the LZ form is
strictly smaller. The encoder also gives up early once its output plus
pending literals reaches the raw size, so random data costs little CPU. As a
result, incompressible input grows by at most 13 bytes per block (a 12-byte
frame header plus the mode byte) plus 22 bytes per stream (the 10-byte
header and the 12-byte end frame). A test checks this bound. The cost is one
byte per block, even for blocks that compress.

**A CRC per block.** Each frame carries a CRC-32 of its raw bytes, which is
checked after decoding. A corrupt block is reported where it occurs, and
the check runs in parallel with the decoding. The costs are 4 bytes per
block and a CRC pass over all the data on both sides. CRC-32 detects
accidental corruption; it is not a defense against deliberate tampering.

**A bounded in-flight window.** At most `Workers × 4` blocks are in memory
at once, whatever the input size, and buffers are reused from one batch to
the next. A test compresses 64 MiB and checks that the heap stays bounded.
The cost is that batches run in lockstep. The reader waits while a batch
encodes, and the workers wait while it is written, so I/O does not overlap
with compression.

**A worker cap.** `-workers` accepts 1 to 1024, and the default is the
smaller of the CPU count and 8. The cap exists because a stream's header
picks the block size: a hostile stream that declares 64 MiB blocks makes
each in-flight slot 64 MiB. With the default of at most 8 workers, that is
at most 32 such blocks in flight. The cost is that a machine with more than
8 cores needs an explicit `-workers N` to use them all.

**Work stealing vs static scheduling.** Both schedulers are kept and can be
chosen with `-sched`:

- `static` gives each worker one contiguous range of blocks.
- `steal` deals the blocks round-robin into one bounded Chase–Lev deque per
  worker. Idle workers then steal from the other workers' deques.

With 2 workers, stealing showed no gain. In-process compression measured
396 MB/s for static, 376 MB/s for steal, and 393 MB/s for a plain
atomic-counter baseline, which exists only in the benchmark tests (median
of 5 runs). That puts steal about 5% below static. The reason is that all
the tasks are dealt out before any worker starts, so stealing only helps
when block costs vary a lot or there are many workers. Neither case was
measured here. `steal` is the default because it is designed for uneven
blocks, and on even blocks it measured only a few percent slower. That
choice is a design judgment, not a measured win.

**Testing.**

- **Fuzzing.** Three fuzz targets:
  - `FuzzDecodeBlock` feeds arbitrary payloads to the block decoder.
  - `FuzzReadStream` feeds arbitrary bytes to the container reader and
    checks that every accepted frame is within its bounds.
  - `FuzzBlockRoundTrip` checks that any input survives encoding and
    decoding.
- **Race detector.** `go test -race` runs on the deque, the schedulers and
  the full pipeline. The deque's concurrent test has 4 thieves and an owner
  move 100,000 tasks, and it checks that each task is taken exactly once.
- **Mutation checks.** Several tests were checked by breaking the code on
  purpose and confirming that they fail:
  - **Deque:** removing the CAS from `Steal`, removing it from `Pop`'s
    last-element path, and reading `top` before reserving `bottom` in `Pop`.
    The exactly-once test fails ("task N taken 2 times") for each.
  - **Bounded-memory test:** it fails when the batch limit is raised from 4
    to 1000 blocks per worker.
  - **Skip-after-error test:** it fails when workers ignore an earlier
    error.
- **Error paths.** Corrupt, truncated and bad-magic streams, and streams
  with no end frame, all exit with status 1 and leave no output file. Usage
  errors exit with 2. If the input and output are the same file, even
  through a symlink or a hard link, pczip refuses with exit 2 and leaves the
  input untouched. An unwritable output directory gives exit 1. SIGINT and
  SIGTERM remove the temp file.

## What I'd do next

- **An entropy stage** (Huffman or FSE) after the match finder, to close
  part of the ratio gap with zstd.
- **Streaming dictionaries:** prime each block with the tail of the previous
  block, as plain pigz does. This gains ratio, but decompression would
  become serial.
- **Measure on 8 or more cores,** with uneven block costs, to find out
  whether work stealing earns its place.

## Usage

Install the CLI. This needs Go 1.22 or later:

```
go install github.com/rutvijjoshi26/parallel-compressor-go/cmd/pczip@latest
```

The command line is:

```
pczip [-d] [-workers N] [-sched static|steal] [-block SIZE] IN OUT
```

Flags go before `IN` and `OUT`. Either one may be `-`, for stdin or stdout.

```
pczip data.log data.log.pcz                 # compress a file
pczip -d data.log.pcz data.log              # decompress it
pczip -workers 4 -block 4MiB big.bin big.pcz
pczip - - < data.log > data.log.pcz         # stdin to stdout
tar cf - dir | pczip - backup.tar.pcz       # compress a stream
pczip -d backup.tar.pcz - | tar xf -        # decompress a stream
```

| flag | default | meaning |
|---|---|---|
| `-d` | off | decompress instead of compress |
| `-workers N` | the CPU count, at most 8 | worker goroutines, 1 to 1024 |
| `-sched` | `steal` | block scheduler: `static` or `steal` |
| `-block SIZE` | `1MiB` | block size for compression, 4KiB to 64MiB, in bytes or with a `KiB`/`MiB` suffix; with `-d`, the block size comes from the stream header and this flag is ignored |

A file output is written to `OUT.tmp-<pid>` in the same directory, synced,
and then renamed to `OUT` only if everything succeeded. So a failed run
leaves no output file.

**Exit codes:**

| code | meaning |
|---|---|
| 0 | success |
| 1 | runtime error (I/O, or a corrupt or truncated input); a file OUT is not created |
| 2 | usage error (bad flag or value, wrong number of arguments, IN and OUT the same file) |
| 130 | interrupted by SIGINT or SIGTERM while writing a file; the temp file is removed |

Errors go to stderr, so stdout carries only data.

**Windows.** pczip is a plain CLI with no Windows-specific code, and it is
expected to work the same way there. CI runs the test suite on Linux, macOS
and Windows. Some tests are skipped on Windows:

- the two signal tests, so Ctrl-C cleanup is tested only on Linux and macOS;
- the unwritable-directory test, because Windows does not use Unix
  permission bits;
- the symlink and hard-link cases of the same-file test, but only if
  Windows refuses to create the link.

## Testing and benchmarks

```
go vet ./...
go test -race ./...

# fuzz one target at a time (-fuzz accepts one package)
go test -run='^$' -fuzz='^FuzzDecodeBlock$'    -fuzztime=30s ./pcz/
go test -run='^$' -fuzz='^FuzzReadStream$'     -fuzztime=30s ./pcz/
go test -run='^$' -fuzz='^FuzzBlockRoundTrip$' -fuzztime=30s ./pcz/

# in-process scheduler benchmarks (static, steal, atomic-counter)
go test -run='^$' -bench=. -benchtime=3x -count=5 ./pcz/

# CLI benchmarks against gzip and zstd (needs ~2 GB free in BENCH_DIR)
export BENCH_DIR=$HOME/bench-data
bash scripts/bench.sh
rm -rf "$BENCH_DIR"
```

`scripts/bench.sh` builds pczip and `cmd/gendata` and generates the
datasets. It times every tool with the same stdin-to-file I/O and checks
every output with `cmp`. Any tool that is not installed is skipped.

## Limitations

- **The codec is LZ4-class, not zstd-class.** It has no entropy stage and no
  dictionary, so its ratio trails both gzip and zstd on every dataset
  measured.
- **Old files are not readable.** The format is PCZ3, which is not
  compatible with `.pcz` files from earlier versions of this repository
  (PCZ2).
- **The benchmarks are synthetic,** measured on one 2-vCPU burstable
  machine, and do not cover more than 2 cores.
- **Durability is partial.** The output file is fsynced before the rename,
  but its directory is not.

## License

MIT. See [LICENSE](LICENSE).

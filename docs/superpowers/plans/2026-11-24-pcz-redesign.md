# pcz Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild parallel-compressor-go as a correct, tested, streaming block compressor. It has one pipeline with two schedulers (static, and Chase–Lev work stealing), an LZ4-style codec, a validated and checksummed container, and a CLI that fails loudly.

**Architecture:**
- **Package `pcz`** holds the block codec (`block.go`), the container framing (`frame.go`), the scheduler (`sched.go`) and the streaming `Compress`/`Decompress` (`pcz.go`).
- **`internal/chaselev`** holds the work-stealing deque.
- **`cmd/pczip`** is the CLI.
- **`cmd/gendata`** writes benchmark datasets.
- **Replaced and deleted:** the old `core/`, `main.go` and `benchmark.py`.

**Tech Stack:** Go 1.22+ standard library only (`hash/crc32`, `sync/atomic`, `testing` with fuzzing). Benchmarks compare against `gzip` and `zstd`.

**Spec:**
- The audits in `docs/refactor-audit-2026-11-24/` (deep-refactor worklog and repros).
- The simplify survey at `/tmp/claude-1000/-home-ec2-user/53eaa5bf-eb58-4a19-ab41-a561cb7c3708/scratchpad/pcg-simplify-survey.md`.
- The user's choice: "Also redesign": fix all bugs, merge the three pipelines, and use an LZ4-style token format; the file format may change.

## Global Constraints

- Module path `github.com/rutvijjoshi26/parallel-compressor-go`, `go 1.22` in go.mod, and no third-party dependencies.
- **Container format `PCZ3`, all integers little-endian.**
  - Header: magic `"PCZ3"` (4 bytes), version `u8 = 3`, flags `u8 = 0`, block size `u32`.
  - Then one frame per block: `rawLen u32`, `compLen u32`, `crc32 u32` (IEEE, over the raw bytes), then `compLen` payload bytes.
  - The stream ends with a frame where `rawLen = 0` and `compLen = 0` (crc 0, no payload).
- **Payload:** the first byte is the mode. `0x00` means an LZ4-style sequence stream follows; `0xFF` means raw bytes follow.
- **Block size** defaults to `1 MiB`. Valid range is `[4 KiB, 64 MiB]`. A reader rejects any value outside that range.
- **A decoder must never:**
  - panic;
  - allocate more than `blockSize + 64` bytes per block from header-supplied numbers;
  - accept `rawLen > blockSize`, `compLen > blockSize + 64`, a CRC mismatch, a truncated stream or a missing end frame.
- **Compressed output is byte-identical** for every `Workers` and `Sched` value, given the same input and block size.
- **CLI exit codes:** `0` on success, `1` on a runtime error, `2` on a usage error. Errors go to stderr. A failed run leaves no output file. Output is written to `<out>.tmp-<pid>` in the same directory and then renamed.
- **Memory:** at most `Workers × 4` blocks in flight, whatever the input size.
- **Commits:** local branch `redesign` only, never pushed by implementers. Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. A corrupt, truncated or hostile `.pcz` (bad magic, huge `rawLen`, flipped payload byte, missing end frame) gives an error, exit 1, no output file, no panic and no OOM. Test owners: Task 2 (fuzz plus cases), Task 5, Task 6.
2. An empty input, a 1-byte input, an input exactly `k × blockSize` long, and an input of `k × blockSize + 1` all round-trip. Test owner: Task 5.
3. `-workers 0`, a negative value, and more workers than blocks: zero or negative is a usage error (exit 2), and more workers than blocks works. Test owners: Task 5 and Task 6.
4. Incompressible (random) data is stored raw, and the output is at most `input + 13 × blocks + 22` bytes. Test owner: Task 5.
5. `-in` and `-out` naming the same file is refused with exit 2 and the input left untouched. An unwritable output directory gives exit 1. Test owner: Task 6.

---

### Task 1: Module and LZ4-style block codec

**Files:**
- Modify: `go.mod` (module path and go version)
- Create: `pcz/block.go`
- Test: `pcz/block_test.go`

**Interfaces:**
- Produces:
  - `func EncodeBlock(dst, src []byte) []byte`: appends the payload (mode byte plus body) to `dst`. It uses raw mode (`0xFF`) when the LZ form would not be strictly smaller.
  - `func DecodeBlock(dst, payload []byte, rawLen int) ([]byte, error)`: appends exactly `rawLen` bytes to `dst`, or returns an error.

**Sequence format:**
- **Token byte:** the high nibble is the literal length and the low nibble is `matchLen − 4`. A nibble value of 15 means the length continues in extra bytes, each adding 255 until a byte below 255 ends it.
- Then the literals, then the offset as `u16` (1..65535).
- **Last sequence:** literals only, with no offset.
- **Matching:** minimum match 4, a hash table of 4-byte prefixes (`1<<14` entries), and a 64 KiB window inside the block only (blocks stay independent).
- **The decoder rejects:** offset 0, an offset past the produced output, output overrunning `rawLen`, a truncated token, literals or offset, and output short of `rawLen`.

- [ ] **Step 1:** Run `go mod edit -module github.com/rutvijjoshi26/parallel-compressor-go -go=1.22`. Write the failing tests:
  - `TestBlockRoundTrip`, as a table: empty, `"a"`, 4 bytes, `"abcabcabcabc…"` (64 KiB), 1 MiB of English-like text, 1 MiB of random bytes, and 1 MiB of zeros. Each one decodes back to its input.
  - `TestRandomStoredRaw`: 1 MiB of random bytes gives `payload[0] == 0xFF` and `len(payload) == len(src)+1`.
  - `TestZerosCompress`: 1 MiB of zeros gives `len(payload) < 5000`.
  - `TestDecodeRejects`, as a table: offset 0, an offset larger than the produced output, an overrun of rawLen, a truncated literal, an unknown mode byte `0x42`, an empty payload. Each returns a non-nil error.
  - `FuzzDecodeBlock(payload []byte, rawLen uint16)`: must not panic.
  - `FuzzBlockRoundTrip(src []byte)`: encode then decode equals `src`.
- [ ] **Step 2:** Run `go test ./pcz/`. Expected: FAIL (undefined).
- [ ] **Step 3:** Implement `EncodeBlock` and `DecodeBlock` in `pcz/block.go`.
- [ ] **Step 4:** Run `go test ./pcz/ && go test ./pcz/ -run=^$ -fuzz=FuzzDecodeBlock -fuzztime=20s && go test ./pcz/ -run=^$ -fuzz=FuzzBlockRoundTrip -fuzztime=20s`. Expected: PASS.
- [ ] **Step 5:** Commit with `feat(pcz): LZ4-style block codec with fuzz tests`.

### Task 2: Container framing

**Files:**
- Create: `pcz/frame.go`
- Test: `pcz/frame_test.go`

**Interfaces:**
- Consumes: nothing from Task 1. Framing is independent of the payload.
- Produces:
  - `const DefaultBlockSize = 1 << 20`, `MinBlockSize = 4 << 10`, `MaxBlockSize = 64 << 20`
  - `func writeHeader(w io.Writer, blockSize int) error`
  - `func readHeader(r io.Reader) (blockSize int, err error)`
  - `type frame struct { rawLen int; crc uint32; payload []byte }`
  - `func writeFrame(w io.Writer, f frame) error` and `func writeEnd(w io.Writer) error`
  - `func readFrame(r io.Reader, blockSize int) (f frame, end bool, err error)`
  - Exported error values: `ErrFormat` (wrap it with `fmt.Errorf("%w: …")` for every malformed-input error) and `ErrChecksum`.

- [ ] **Step 1:** Write the failing tests:
  - `TestHeaderRoundTrip` for block sizes 4 KiB, 1 MiB and 64 MiB.
  - `TestHeaderRejects`: bad magic, version 2, block size 1 KiB, block size 128 MiB, a truncated header. Each gives `errors.Is(err, ErrFormat)`.
  - `TestFrameRoundTrip`: three frames, then the end frame, are read back.
  - `TestFrameRejects`:
    - `rawLen > blockSize`, `compLen > blockSize+64`, and `compLen == 0` with `rawLen > 0` give ErrFormat;
    - a truncated frame header and a truncated payload give ErrFormat;
    - a stream with no end frame (EOF after a frame) gives ErrFormat from the next `readFrame`.
  - `TestHugeLengthsNoAlloc`: a frame header claiming `rawLen = 0xFFFFFFFF` returns ErrFormat without allocating (check `testing.AllocsPerRun` ≤ 2 on the reject path).
  - `FuzzReadStream(data []byte)`: readHeader then readFrame in a loop must not panic.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3:** Implement. The checksum is verified in Task 5, after decode, so `readFrame` only bounds-checks.
- [ ] **Step 4:** Run `go test ./pcz/` plus `-fuzz=FuzzReadStream -fuzztime=20s`. Expected: PASS.
- [ ] **Step 5:** Commit with `feat(pcz): validated PCZ3 container framing`.

### Task 3: Chase–Lev deque

**Files:**
- Create: `internal/chaselev/deque.go`
- Test: `internal/chaselev/deque_test.go`

**Interfaces:**
- Produces:
  - `type Deque struct`
  - `func New(capacity int) *Deque` (capacity is rounded up to a power of two, minimum 1)
  - `func (d *Deque) Push(task int) bool`: owner only. Returns false when full; never overwrites.
  - `func (d *Deque) Pop() (int, bool)`: owner, LIFO from the bottom.
  - `func (d *Deque) Steal() (int, bool)`: any goroutine, FIFO from the top.
- Keep the existing algorithm from `core/wsdeque.go` (atomic top/bottom, CAS on the last element, cache-line padding), and add the capacity check.

- [ ] **Step 1:** Write the failing tests:
  - `TestPushFull`: `New(2)`, then push 1, 2, 3 gives true, true, false. Draining with Pop gives `[2 1]` (the old bug returned `[3 2 3]`).
  - `TestOwnerLIFO`.
  - `TestStealFIFO`.
  - `TestConcurrentExactlyOnce`: with `-race`, the owner pushes 0..99999 in batches that fit the capacity, interleaved with Pops, while 4 thieves Steal. Every task is taken exactly once (count with a `[]atomic.Int32`).
- [ ] **Step 2:** Run `go test -race ./internal/chaselev/`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `go test -race -count=3 ./internal/chaselev/`. Expected: PASS.
- [ ] **Step 5:** Commit with `feat(chaselev): bounded Chase–Lev deque with race tests`.

### Task 4: Block scheduler

**Files:**
- Create: `pcz/sched.go`
- Test: `pcz/sched_test.go`

**Interfaces:**
- Consumes: `chaselev.New/Push/Pop/Steal` (Task 3).
- Produces:
  - `type Sched int` with `const ( Static Sched = iota; Steal )`
  - `func (s Sched) String() string` returning `"static"` / `"steal"`
  - `func ParseSched(s string) (Sched, error)`
  - `func forEachBlock(n, workers int, sched Sched, fn func(i int) error) error`. It runs `fn` once for each `i` in `[0,n)` using at most `workers` goroutines and returns the first error.
- **Static:** contiguous chunks.
- **Steal:** indices are dealt round-robin into one deque per worker; workers Pop their own, then Steal from others in random order; a worker stops when every deque is empty.
- **On error:** remaining work is skipped, and the function never blocks (there is no barrier). With `workers <= 1` or `n <= 1`, it runs inline on the caller's goroutine.

- [ ] **Step 1:** Write the failing tests:
  - `TestEachIndexOnce`: both scheds, with workers 1, 2, 3, 8 and 64, and n 0, 1, 7 and 1000. Every index runs exactly once (`-race`).
  - `TestErrorNoDeadlock`: `fn` fails for `i == 3` in both scheds with workers 4. It returns that error within 2 s (guarded by `time.After`).
  - `TestPanicFreeOnZeroWorkers`: workers 0 behaves like 1.
  - `TestParseSched`: `"static"` and `"steal"` parse; `"bsp"` gives an error.
- [ ] **Step 2:** Run `go test -race ./pcz/ -run Sched|Each|Error`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `go test -race -count=3 ./pcz/`. Expected: PASS.
- [ ] **Step 5:** Commit with `feat(pcz): static and work-stealing block scheduler`.

### Task 5: Streaming Compress and Decompress

**Files:**
- Create: `pcz/pcz.go`
- Test: `pcz/pcz_test.go`

**Interfaces:**
- Consumes: Tasks 1, 2 and 4.
- Produces:
  - `type Options struct { Workers int; Sched Sched; BlockSize int }`. A zero `BlockSize` means `DefaultBlockSize`. `Workers < 1` gives an error wrapping `ErrOptions`.
  - `var ErrOptions = errors.New("pcz: invalid options")`
  - `func Compress(dst io.Writer, src io.Reader, opt Options) error`
  - `func Decompress(dst io.Writer, src io.Reader, opt Options) error` (the block size comes from the header; `opt.BlockSize` is ignored)
- **Pipeline:**
  - read up to `Workers×4` blocks into a batch;
  - process them with `forEachBlock` (encode, or decode plus a CRC check that fails with `ErrChecksum`);
  - write them in order;
  - repeat until EOF;
  - Compress writes the end frame.

- [ ] **Step 1:** Write the failing tests:
  - `TestRoundTripMatrix`. Inputs: empty, 1 byte, `4096` bytes, `3×4096`, `3×4096+1`, 2 MiB text and 2 MiB random, using `BlockSize: 4096` for the small ones. Run against every `{Static, Steal} × {1,2,3,8}` for compress and decompress. Each decompress equals the input.
  - `TestIdenticalAcrossModes`: the compressed bytes are equal for every sched and worker combination.
  - `TestRandomOverheadBound`: 1 MiB random with `BlockSize: 64 KiB` gives `len(out) <= len(in) + 13*16 + 22`.
  - `TestDecompressRejects`: a flipped payload byte gives ErrChecksum or ErrFormat; truncation at every 97th byte offset, a missing end frame and trailing garbage after the end frame each give ErrFormat. All are run with workers 3 and Steal, with a 2 s deadlock guard.
  - `TestBoundedMemory`: compressing 64 MiB from a generator reader with `Workers: 2`, `BlockSize: 1 MiB` gives `runtime.MemStats.HeapInuse` growth below 64 MiB. Measure the peak by sampling in a goroutine.
  - `TestWorkersInvalid`: `Workers: 0` gives `errors.Is(err, ErrOptions)`.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `go test -race ./pcz/`. Expected: PASS.
- [ ] **Step 5:** Commit with `feat(pcz): streaming parallel Compress/Decompress`.

### Task 6: CLI, and removal of the old code

**Files:**
- Create: `cmd/pczip/main.go`
- Test: `cmd/pczip/main_test.go`
- Delete: `core/`, `main.go`

**Interfaces:**
- Consumes: `pcz.Compress`, `pcz.Decompress`, `pcz.Options`, `pcz.ParseSched` (Task 5).
- **Usage:** `pczip [-d] [-workers N] [-sched static|steal] [-block SIZE] IN OUT`.
  - `-d` means decompress. `-workers` defaults to `runtime.NumCPU()`, `-sched` to `steal`, and `-block` to `1MiB` (it accepts `64KiB`, `1MiB` and so on).
  - `IN` and `OUT` may be `-` for stdin/stdout.
- `func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int` returns the exit code, so it can be tested without exec. `main` calls `os.Exit(run(...))`.
- Output goes to `<OUT>.tmp-<pid>` in OUT's directory and is renamed on success; on failure the temp file is removed.

- [ ] **Step 1:** Write the failing tests against `run`, in temp dirs:
  - `TestCLIRoundTrip`: exit 0 and the files are equal.
  - `TestCLIUsageErrors`: no args, `-workers 0`, `-sched bsp`, `-block 1KiB`. Each gives exit 2 with a stderr line starting `pczip: `.
  - `TestCLIMissingInput`: exit 1.
  - `TestCLICorruptNoOutput`: decompressing a corrupt file gives exit 1, `OUT` doesn't exist, and no `*.tmp-*` remains.
  - `TestCLISameInOut`: exit 2, and the input is unchanged (compare bytes).
  - `TestCLIUnwritableDir`: exit 1.
  - `TestCLIStdio`: `- -` works through the provided readers and writers.
- [ ] **Step 2:** Run `go test ./cmd/pczip/`. Expected: FAIL.
- [ ] **Step 3:** Implement, then run `git rm -r core main.go`.
- [ ] **Step 4:** Run `go vet ./... && go test -race ./...`. Expected: PASS.
- [ ] **Step 5:** Commit with `feat(cli): pczip with exit codes and atomic output; remove old core`.

### Task 7: Benchmarks and measured results

**Files:**
- Create: `cmd/gendata/main.go`, `pcz/bench_test.go`, `scripts/bench.sh`, `docs/benchmarks.md`
- Delete: `benchmark.py`

**Interfaces:**
- `gendata -kind text|logs|mixed|random -size SIZE -out PATH` is deterministic (seed 1).
  - `text` is generated English-like words.
  - `logs` is JSON log lines with repeating keys.
  - `mixed` is 20% random, 60% text, 20% zeros.
- `pcz/bench_test.go`: `BenchmarkCompress/{sched}/{workers}` and `BenchmarkDecompress/...` on 32 MiB of `mixed`, with `b.SetBytes`. It also includes an `atomic-counter` baseline scheduler local to the test file, for the "is stealing worth it" comparison.
- `scripts/bench.sh` builds pczip and generates 256 MiB each of text, logs and mixed into `$BENCH_DIR` (default `/tmp`). For each dataset it times, with the median of 3:
  - pczip compress for `{static,steal}×{1,2,max}` workers, and pczip decompress for `steal×max`;
  - `gzip -1`, `gzip -6` and `zstd -1 -T0` (skipping any tool that isn't installed);
  - and it verifies every round trip with `cmp`.

  It prints a markdown table: dataset, tool/config, ratio, MB/s compress, MB/s decompress.

- [ ] **Step 1:** Implement gendata and the benchmarks.
- [ ] **Step 2:** Run `go test -run=^$ -bench=. -benchtime=3x ./pcz/` and `bash scripts/bench.sh`.
- [ ] **Step 3:** Write `docs/benchmarks.md` with the machine (CPU model, cores, RAM, OS and Go version), the exact commands and the raw tables. Write findings only as the numbers show them, including where pcz loses to zstd.
- [ ] **Step 4:** Run `git rm benchmark.py` and commit with `bench: reproducible benchmarks vs gzip and zstd`.

### Task 8: CI, LICENSE, .gitignore and README

**Files:**
- Create: `.github/workflows/ci.yml`, `LICENSE` (MIT, `Copyright (c) 2025 Rutvij Joshi`), `.gitignore` (`/pczip`, `*.pcz`, `*.tmp-*`, `/bench-out/`)
- Modify: `README.md` (full rewrite)

**ci.yml:**
- Runs on push and pull_request, with a matrix of ubuntu-latest, macos-latest and windows-latest, and Go `stable`.
- Steps: `go vet ./...` and `go test -race ./...` (on Windows `-race` needs cgo, so use plain `go test ./...` there). On ubuntu only, a fuzz smoke test of `FuzzDecodeBlock`, `FuzzReadStream` and `FuzzBlockRoundTrip` for 15 s each.
- `permissions: contents: read`, and concurrency cancel-in-progress.

**README contents:**
- **Title:** `pczip: a parallel block compressor in Go`.
- **Opening:** a one-paragraph pitch.
- **Results first:** the table from `docs/benchmarks.md` (ratio and MB/s vs gzip and zstd, with the machine noted).
- **How it works:**
  - a block diagram in text: reader → batch → scheduler (static | Chase–Lev stealing) → encode → ordered writer;
  - the format spec copied from Global Constraints;
  - why independent blocks (as in pigz, zstd -T and LZ4 frames).
- **Design decisions and trade-offs:** each a short paragraph.
  - Why both schedulers stay, with the benchmark comparison from Task 7, including the atomic-counter baseline, stated honestly.
  - Raw fallback.
  - CRC per block.
  - Bounded memory.
  - Fuzzing and race tests.
- **Usage:** install with `go install github.com/rutvijjoshi26/parallel-compressor-go/cmd/pczip@latest`, examples, and exit codes.
- **Testing:** the commands.
- **Limitations,** all true: the codec is LZ4-class, not zstd-class; there is no dictionary or entropy stage; the format is not compatible with older `.pcz` files.
- **License.**
- No AI chat leftovers, no absolute personal paths, and no claim that the measurements don't support.

- [ ] **Step 1:** Write the files.
- [ ] **Step 2:** Run `go vet ./... && go test -race ./...` and validate the YAML with python3.
- [ ] **Step 3:** Commit with `docs: README with measured results; CI; MIT license`.

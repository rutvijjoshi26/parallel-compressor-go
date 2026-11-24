# Worklog — parallel-compressor-go audit (2026-11-24)

## Goal

Critically audit the whole repo (main.go, core/, benchmark.py, README.md) to decide which architecture,
modes, boundaries, names, abstractions and hand-rolled machinery should be kept, simplified, deleted or
redesigned, and to judge concurrency and format correctness. Audience: the owner, who shows this repo to
startup founders as a systems-depth piece.

Core questions: Why does each mode/primitive exist? Does it pay rent? Is the deque/barrier justified? Does
the format round-trip and do parallel modes match sequential? What would a clean design look like today?

Non-goals: no edits to implementation files; no commits; no scaling claims beyond 2 cores.

## Environment

- Go 1.27.4 at ~/.local/go-sdk/go/bin (go.mod says go 1.19). 2 vCPU arm64, 3 GB RAM.
- Scratch: /tmp/claude-1000/-home-ec2-user/53eaa5bf-eb58-4a19-ab41-a561cb7c3708/scratchpad/pcg-audit/
  - `pczip` built from the repo (`go build -o $S/pczip .`)
  - `in/` generated inputs (empty, 1 B, 3 B, 1 MiB+1 random, 5 MiB zeros, 11.7 MB text-ish, 2 MiB exact-multiple, 40 MiB mixed)
  - `mod/` copy of core/ + go.mod with scratch tests (copies saved in `repro/*.go.txt` here)

## Log

1. Read all source (≈1000 lines Go, 298 Python, 175 README). Two commits only (`4bb2151` initial, `47079fb` README).
2. `go vet ./...` clean. Builds.
3. Round-trip matrix: 8 inputs × compress{seq, bsp t=1,2,3,8, ws t=1,2,3,8} × decompress{seq,bsp,ws}×{1,3}
   → all round-trip, and every compressed file is byte-identical to the seq output. (fail=0)
4. Ratio vs prior art (bytes): text 11.7 MB → pcz 7.35 MB, gzip -1 3.60 MB, zstd -1 3.34 MB.
   mixed 40 MiB → pcz 23.4 MB, gzip -1 19.0 MB, zstd -1 18.6 MB. Wall: pcz bsp t=2 0.18 s vs zstd -1 -T2 0.07 s.
5. Error-path repros (see README.md "Correctness bugs"): exit code 0 on every failure; truncated .pcz →
   partial output + exit 0; corrupt OriginalSize → panic; huge NumBlocks → fatal OOM; BSP decompress
   deadlock when one worker errors after the others reached the barrier (2/3 runs).
6. `go test -race` on in-process round trip with 4 KiB blocks (768 blocks, 3 workers, heavy stealing): clean.
   Deque exactly-once stress (owner push/pop interleaved with 3 thieves, 200 rounds × 20k): clean.
   Ring-wrap stress (cap 4, 200k tasks, 1 thief): 0 lost/duplicated, race detector silent — the
   plain-slot-read race in Steal stays theoretical/unreproduced.
7. Scheduler comparison in-process, 40 × 1 MiB blocks, 2 workers, best of 5: sequential 221 ms,
   atomic counter 126 ms, static chunks 127 ms, Chase-Lev 127 ms. (A critic subagent may have been using
   CPU concurrently; the point is equivalence, not absolute numbers.)
8. Spawned 2 critics (concurrency adversary; deletion/portfolio skeptic). Outputs in reports/.

## Key insights to keep after compaction

- Parallel correctness of the happy path is solid: blocks are independent, per-block encoding is
  deterministic, so all modes emit identical bytes. The bugs are all on error paths and untrusted input.
- The deque is used as a pre-filled read-only queue: all PushBottom calls happen before workers start
  (worksteal.go:73-75, 245-247). Chase-Lev's raison d'être (owner pushes during execution) never happens.
- The barrier is redundant with the WaitGroup (single superstep, Wait is the last statement before Done)
  and it is the cause of the deadlock.
- ~60% of Go lines are three copies of the same file driver differing only in scheduling.
- README contains AI-chat residue and an absolute Mac path; module is named `proj3`.

## Critic results (2 subagents, 2026-11-24)

- Concurrency critic (scratch: pcg-audit/critic-concurrency/): independently reproduced the BSP deadlock
  (corrupt block 1, 3 blocks, -threads 2; in-process hang 3/3), exit-0 behaviour, and header panics:
  numBlocks=2^62 → makeslice at format.go:96; block sizes [2^64-1,2] wrap the uint64 sum (bsp.go:176-178,
  worksteal.go:221-223) → slice-bounds panic; orig=10/bs=4096/n=2 → panic inside a ws worker goroutine
  (worksteal.go:339); OriginalSize=2^62 → panic at bsp.go:191 / worksteal.go:236.
  Reproduced PushBottom overflow (wsdeque.go:49-53): NewWSDeque(2), push 1,2,3, drain → [3 2 3]. Latent:
  worksteal.go sizes deques to ceil(N/T) and only pushes before workers start.
  Judged CORRECT: deque memory ordering (seq-cst atomics), last-element CAS, no ABA, Barrier itself,
  BSP compress, ws error propagation, all 9 compress×decompress pairings under -race.
- Design critic (scratch: pcg-audit/critic-design/): prototyped a ~15-line atomic-counter scheduler,
  byte-identical to ws and within noise on timing; zstd -1 single-threaded beats both parallel modes on
  speed and ratio (real system files 60 MB: pcz ratio 1.31 vs zstd 3.3). Noted sequential.go:145 creates
  output before validating (truncates input if -in == -out); README.md:147 claims streaming sequential mode
  but sequential.go:72/102 buffers all compressed blocks; benchmark `fragmented` dataset is symmetric so it
  cannot show BSP imbalance at T=2.

## Note on line numbers

Early `cat -n` calls concatenated files, so line numbers for lz.go, format.go, barrier.go, wsdeque.go and
bsp.go in my scratch notes were offset; all citations in the final report were re-checked with per-file grep.
The harness refused writing a separate findings.md from this subagent, so the findings live in the
handback report; this worklog and repro/*.go.txt are the on-disk artifacts.

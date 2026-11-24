package pcz

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rutvijjoshi26/parallel-compressor-go/internal/gendata"
)

// benchSize is the size of the benchmark input: 32 MiB of gendata "mixed"
// (seed 1), the same generator cmd/gendata and scripts/bench.sh use.
const benchSize = 32 << 20

var benchInput = sync.OnceValue(func() []byte {
	r, err := gendata.New("mixed", 1)
	if err != nil {
		panic(err)
	}
	b, err := io.ReadAll(io.LimitReader(r, benchSize))
	if err != nil {
		panic(err)
	}
	return b
})

// benchWorkers is {1, 2, NumCPU}, deduplicated and in order.
func benchWorkers() []int {
	ws := []int{1, 2}
	if n := runtime.NumCPU(); n > 2 {
		ws = append(ws, n)
	}
	return ws
}

// runAtomicCounter is a baseline scheduler for comparison only: workers
// claim the next index from one shared atomic counter. It is the simplest
// dynamic scheduler, and the question the benchmark answers is whether
// work stealing does better than it.
func runAtomicCounter(n, workers int, fn func(i int) error) error {
	if workers > n {
		workers = n
	}
	if workers <= 1 {
		for i := 0; i < n; i++ {
			if err := fn(i); err != nil {
				return err
			}
		}
		return nil
	}
	var next atomic.Int64
	var failed atomic.Bool
	var once sync.Once
	var first error
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !failed.Load() {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				if err := fn(i); err != nil {
					once.Do(func() { first = err; failed.Store(true) })
					return
				}
			}
		}()
	}
	wg.Wait()
	return first
}

// compressAtomicCounter is Compress with forEachBlock replaced by
// runAtomicCounter. It must stay a line-for-line copy of Compress apart
// from that call; TestCompressAtomicCounterMatches checks its output.
func compressAtomicCounter(dst io.Writer, src io.Reader, workers, bs int) error {
	if err := writeHeader(dst, bs); err != nil {
		return err
	}
	batch := workers * blocksPerWorker
	var raws [][]byte
	var frames []frame
	for eof := false; !eof; {
		n := 0
		for n < batch {
			if n == len(raws) {
				raws = append(raws, make([]byte, bs))
				frames = append(frames, frame{})
			}
			m, err := io.ReadFull(src, raws[n][:bs])
			if m > 0 {
				raws[n] = raws[n][:m]
				n++
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				eof = true
				break
			}
			if err != nil {
				return err
			}
		}
		err := runAtomicCounter(n, workers, func(i int) error {
			raw := raws[i]
			frames[i] = frame{
				rawLen:  len(raw),
				crc:     crc32.ChecksumIEEE(raw),
				payload: EncodeBlock(frames[i].payload[:0], raw),
			}
			return nil
		})
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := writeFrame(dst, frames[i]); err != nil {
				return err
			}
		}
	}
	return writeEnd(dst)
}

func TestCompressAtomicCounterMatches(t *testing.T) {
	in := benchInput()[:5<<20]
	var want, got bytes.Buffer
	if err := Compress(&want, bytes.NewReader(in), Options{Workers: 2, Sched: Steal}); err != nil {
		t.Fatal(err)
	}
	if err := compressAtomicCounter(&got, bytes.NewReader(in), 2, DefaultBlockSize); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want.Bytes()) {
		t.Fatal("compressAtomicCounter output differs from Compress")
	}
}

// BenchmarkCompress reports throughput in input bytes per second, as
// b.SetBytes counts the uncompressed size.
func BenchmarkCompress(b *testing.B) {
	in := benchInput()
	for _, sched := range []string{"static", "steal", "atomic-counter"} {
		for _, w := range benchWorkers() {
			b.Run(fmt.Sprintf("%s/%d", sched, w), func(b *testing.B) {
				b.SetBytes(int64(len(in)))
				b.ReportAllocs()
				var out bytes.Buffer
				out.Grow(len(in) + len(in)/8)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out.Reset()
					var err error
					if sched == "atomic-counter" {
						err = compressAtomicCounter(&out, bytes.NewReader(in), w, DefaultBlockSize)
					} else {
						s, _ := ParseSched(sched)
						err = Compress(&out, bytes.NewReader(in), Options{Workers: w, Sched: s})
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(len(in))/float64(out.Len()), "ratio")
			})
		}
	}
}

// BenchmarkDecompress reports throughput in decompressed bytes per second.
func BenchmarkDecompress(b *testing.B) {
	in := benchInput()
	var comp bytes.Buffer
	if err := Compress(&comp, bytes.NewReader(in), Options{Workers: 1}); err != nil {
		b.Fatal(err)
	}
	for _, sched := range []Sched{Static, Steal} {
		for _, w := range benchWorkers() {
			b.Run(fmt.Sprintf("%s/%d", sched, w), func(b *testing.B) {
				b.SetBytes(int64(len(in)))
				b.ReportAllocs()
				var out bytes.Buffer
				out.Grow(len(in))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out.Reset()
					if err := Decompress(&out, bytes.NewReader(comp.Bytes()), Options{Workers: w, Sched: sched}); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if !bytes.Equal(out.Bytes(), in) {
					b.Fatal("round trip mismatch")
				}
			})
		}
	}
}

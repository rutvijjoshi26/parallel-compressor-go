package pcz

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

var allWorkers = []int{1, 2, 3, 8}

type pipelineInput struct {
	name      string
	data      []byte
	blockSize int
}

func pipelineInputs() []pipelineInput {
	const bs = 4096
	return []pipelineInput{
		{"empty", nil, bs},
		{"1byte", []byte{'x'}, bs},
		{"4096", englishText(bs), bs},
		{"3x4096", englishText(3 * bs), bs},
		{"3x4096+1", englishText(3*bs + 1), bs},
		{"2MiB-text", englishText(2 * mib), 0},
		{"2MiB-random", randomBytes(2 * mib), 0},
	}
}

func compressBytes(t testing.TB, src []byte, opt Options) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := Compress(&out, bytes.NewReader(src), opt); err != nil {
		t.Fatalf("Compress(%+v): %v", opt, err)
	}
	return out.Bytes()
}

func TestRoundTripMatrix(t *testing.T) {
	for _, in := range pipelineInputs() {
		for _, sched := range allScheds {
			for _, w := range allWorkers {
				t.Run(fmt.Sprintf("%s/%s/w%d", in.name, sched, w), func(t *testing.T) {
					opt := Options{Workers: w, Sched: sched, BlockSize: in.blockSize}
					comp := compressBytes(t, in.data, opt)
					var got bytes.Buffer
					if err := Decompress(&got, bytes.NewReader(comp), opt); err != nil {
						t.Fatalf("Decompress: %v", err)
					}
					if !bytes.Equal(got.Bytes(), in.data) {
						t.Fatalf("round trip mismatch: got %d bytes, want %d", got.Len(), len(in.data))
					}
				})
			}
		}
	}
}

func TestIdenticalAcrossModes(t *testing.T) {
	inputs := append(pipelineInputs(), pipelineInput{"2MiB-text-bs4096", englishText(2 * mib), 4096})
	for _, in := range inputs {
		t.Run(in.name, func(t *testing.T) {
			ref := compressBytes(t, in.data, Options{Workers: 1, Sched: Static, BlockSize: in.blockSize})
			for _, sched := range allScheds {
				for _, w := range allWorkers {
					got := compressBytes(t, in.data, Options{Workers: w, Sched: sched, BlockSize: in.blockSize})
					if !bytes.Equal(got, ref) {
						t.Fatalf("%s/w%d: compressed bytes differ from static/w1", sched, w)
					}
				}
			}
		})
	}
}

func TestRandomOverheadBound(t *testing.T) {
	in := randomBytes(1 * mib)
	out := compressBytes(t, in, Options{Workers: 2, Sched: Steal, BlockSize: 64 << 10})
	blocks := len(in) / (64 << 10)
	if limit := len(in) + 13*blocks + 22; len(out) > limit {
		t.Fatalf("compressed %d random bytes to %d, want <= %d", len(in), len(out), limit)
	}
}

// decompressGuarded runs Decompress with workers 3 and Steal, failing the
// test if it does not return within 2 s.
func decompressGuarded(t *testing.T, data []byte) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- Decompress(io.Discard, bytes.NewReader(data), Options{Workers: 3, Sched: Steal})
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Decompress did not return within 2s")
		return nil
	}
}

func TestDecompressRejects(t *testing.T) {
	// 12 blocks of 4 KiB plus a partial block: several batches with workers 3.
	comp := compressBytes(t, englishText(50000), Options{Workers: 3, Sched: Steal, BlockSize: 4096})

	t.Run("flipped payload byte", func(t *testing.T) {
		bad := bytes.Clone(comp)
		bad[headerSize+frameHeaderSize+40] ^= 0x01
		err := decompressGuarded(t, bad)
		if !errors.Is(err, ErrChecksum) && !errors.Is(err, ErrFormat) {
			t.Fatalf("err = %v, want ErrChecksum or ErrFormat", err)
		}
	})
	t.Run("truncated", func(t *testing.T) {
		for off := 0; off < len(comp); off += 97 {
			if err := decompressGuarded(t, comp[:off]); !errors.Is(err, ErrFormat) {
				t.Fatalf("truncated at %d of %d: err = %v, want ErrFormat", off, len(comp), err)
			}
		}
	})
	t.Run("missing end frame", func(t *testing.T) {
		if err := decompressGuarded(t, comp[:len(comp)-frameHeaderSize]); !errors.Is(err, ErrFormat) {
			t.Fatalf("err = %v, want ErrFormat", err)
		}
	})
	t.Run("trailing garbage", func(t *testing.T) {
		if err := decompressGuarded(t, append(bytes.Clone(comp), 0)); !errors.Is(err, ErrFormat) {
			t.Fatalf("err = %v, want ErrFormat", err)
		}
	})
}

// A raw frame whose bytes are intact but whose stored CRC is wrong must give
// ErrChecksum specifically.
func TestDecompressCRCMismatch(t *testing.T) {
	comp := compressBytes(t, randomBytes(100), Options{Workers: 1, BlockSize: 4096})
	comp[headerSize+8] ^= 0xFF // first byte of the first frame's crc
	if err := decompressGuarded(t, comp); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

// genReader yields n bytes of cheap, mildly compressible data without
// holding them in memory.
type genReader struct {
	n, off int
}

func (g *genReader) Read(p []byte) (int, error) {
	if g.off >= g.n {
		return 0, io.EOF
	}
	if len(p) > g.n-g.off {
		p = p[:g.n-g.off]
	}
	for i := range p {
		x := uint32(g.off + i)
		x ^= x >> 7
		x *= 0x9E3779B1
		p[i] = byte(x>>24) & 0x3F
	}
	g.off += len(p)
	return len(p), nil
}

func TestBoundedMemory(t *testing.T) {
	const input = 64 * mib
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base := ms.HeapInuse

	var peak atomic.Uint64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		var s runtime.MemStats
		for {
			runtime.ReadMemStats(&s)
			if s.HeapInuse > peak.Load() {
				peak.Store(s.HeapInuse)
			}
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
	}()

	err := Compress(io.Discard, &genReader{n: input}, Options{Workers: 2, Sched: Steal, BlockSize: 1 * mib})
	close(stop)
	<-sampled
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	growth := int64(peak.Load()) - int64(base)
	t.Logf("HeapInuse growth peak: %.1f MiB", float64(growth)/mib)
	if growth >= 64*mib {
		t.Fatalf("HeapInuse grew by %d bytes while compressing %d bytes, want < 64 MiB", growth, input)
	}
}

func TestWorkersInvalid(t *testing.T) {
	for _, w := range []int{0, -1} {
		opt := Options{Workers: w}
		if err := Compress(io.Discard, bytes.NewReader([]byte("x")), opt); !errors.Is(err, ErrOptions) {
			t.Fatalf("Compress workers %d: err = %v, want ErrOptions", w, err)
		}
		if err := Decompress(io.Discard, bytes.NewReader(nil), opt); !errors.Is(err, ErrOptions) {
			t.Fatalf("Decompress workers %d: err = %v, want ErrOptions", w, err)
		}
	}
}

func TestOptionsInvalid(t *testing.T) {
	for _, opt := range []Options{
		{Workers: 1, BlockSize: 100},
		{Workers: 1, BlockSize: MaxBlockSize + 1},
		{Workers: 1, Sched: Sched(9)},
	} {
		if err := Compress(io.Discard, bytes.NewReader(nil), opt); !errors.Is(err, ErrOptions) {
			t.Fatalf("Compress(%+v): err = %v, want ErrOptions", opt, err)
		}
	}
}

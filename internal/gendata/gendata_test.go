package gendata

import (
	"bytes"
	"io"
	"testing"
)

func read(t *testing.T, kind string, seed uint64, n int64) []byte {
	t.Helper()
	r, err := New(kind, seed)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(io.LimitReader(r, n))
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(b)) != n {
		t.Fatalf("%s: got %d bytes, want %d", kind, len(b), n)
	}
	return b
}

func TestDeterministic(t *testing.T) {
	for _, k := range Kinds {
		a := read(t, k, 1, 3<<20)
		b := read(t, k, 1, 3<<20)
		if !bytes.Equal(a, b) {
			t.Errorf("%s: two runs with seed 1 differ", k)
		}
		c := read(t, k, 2, 3<<20)
		if bytes.Equal(a, c) {
			t.Errorf("%s: seeds 1 and 2 give the same bytes", k)
		}
	}
}

// TestReadSizeIndependent checks that the bytes do not depend on how the
// caller sizes its reads.
func TestReadSizeIndependent(t *testing.T) {
	for _, k := range Kinds {
		want := read(t, k, 1, 1<<20)
		r, _ := New(k, 1)
		var got []byte
		buf := make([]byte, 7777)
		for len(got) < len(want) {
			n, _ := r.Read(buf[:min(len(buf), len(want)-len(got))])
			got = append(got, buf[:n]...)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s: output depends on read size", k)
		}
	}
}

func TestMixedProportions(t *testing.T) {
	b := read(t, "mixed", 1, 5*segmentSize*20)
	zeroSegs := 0
	for i := 0; i < len(b); i += segmentSize {
		if bytes.Count(b[i:i+segmentSize], []byte{0}) == segmentSize {
			zeroSegs++
		}
	}
	if zeroSegs != 20 {
		t.Fatalf("zero segments = %d, want 20 (one per group of five)", zeroSegs)
	}
}

func TestUnknownKind(t *testing.T) {
	if _, err := New("nope", 1); err == nil {
		t.Fatal("no error for unknown kind")
	}
}

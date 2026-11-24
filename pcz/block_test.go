package pcz

import (
	"bytes"
	"math/rand"
	"strings"
	"testing"
)

const mib = 1 << 20

func englishText(n int) []byte {
	words := strings.Fields("the quick brown fox jumps over a lazy dog while parallel " +
		"compressors split input into blocks and workers encode each block independently " +
		"so that output stays deterministic for every schedule of goroutines and channels")
	r := rand.New(rand.NewSource(1))
	var b bytes.Buffer
	for b.Len() < n {
		b.WriteString(words[r.Intn(len(words))])
		if r.Intn(12) == 0 {
			b.WriteString(".\n")
		} else {
			b.WriteByte(' ')
		}
	}
	return b.Bytes()[:n]
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(2)).Read(b)
	return b
}

func roundTrip(t testing.TB, src []byte) {
	t.Helper()
	payload := EncodeBlock(nil, src)
	got, err := DecodeBlock(nil, payload, len(src))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("round trip mismatch: len got %d want %d", len(got), len(src))
	}
}

func TestBlockRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		src  []byte
	}{
		{"empty", []byte{}},
		{"one byte", []byte("a")},
		{"four bytes", []byte("abcd")},
		{"abc repeated 64KiB", bytes.Repeat([]byte("abc"), 64*1024/3+1)[:64*1024]},
		{"english 1MiB", englishText(mib)},
		{"random 1MiB", randomBytes(mib)},
		{"zeros 1MiB", make([]byte, mib)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { roundTrip(t, c.src) })
	}
}

func TestBlockAppendsToDst(t *testing.T) {
	src := englishText(10000)
	prefix := []byte("prefix")
	payload := EncodeBlock(append([]byte{}, prefix...), src)
	if !bytes.HasPrefix(payload, prefix) {
		t.Fatal("EncodeBlock did not append to dst")
	}
	got, err := DecodeBlock(append([]byte{}, prefix...), payload[len(prefix):], len(src))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, append(append([]byte{}, prefix...), src...)) {
		t.Fatal("DecodeBlock did not append to dst")
	}
}

func TestEnglishCompresses(t *testing.T) {
	src := englishText(mib)
	payload := EncodeBlock(nil, src)
	if payload[0] != 0x00 || len(payload) >= len(src)/2 {
		t.Fatalf("english text: mode %#x, len %d of %d", payload[0], len(payload), len(src))
	}
}

func TestRandomStoredRaw(t *testing.T) {
	src := randomBytes(mib)
	payload := EncodeBlock(nil, src)
	if payload[0] != 0xFF {
		t.Fatalf("mode = %#x, want 0xFF", payload[0])
	}
	if len(payload) != len(src)+1 {
		t.Fatalf("len(payload) = %d, want %d", len(payload), len(src)+1)
	}
}

func TestZerosCompress(t *testing.T) {
	payload := EncodeBlock(nil, make([]byte, mib))
	if len(payload) >= 5000 {
		t.Fatalf("len(payload) = %d, want < 5000", len(payload))
	}
}

func TestDecodeRejects(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		rawLen  int
	}{
		{"offset zero", []byte{0x00, 0x10, 'a', 0x00, 0x00}, 5},
		{"offset past output", []byte{0x00, 0x10, 'a', 0x02, 0x00}, 5},
		{"match overruns rawLen", []byte{0x00, 0x10, 'a', 0x01, 0x00, 0x00}, 3},
		{"literals overrun rawLen", []byte{0x00, 0x30, 'a', 'b', 'c'}, 2},
		{"truncated literal", []byte{0x00, 0x50, 'a', 'b'}, 5},
		{"truncated offset", []byte{0x00, 0x10, 'a', 0x01}, 5},
		{"truncated token length", []byte{0x00, 0xF0, 0xFF}, 300},
		{"missing last sequence", []byte{0x00, 0x10, 'a', 0x01, 0x00}, 5},
		{"empty LZ body", []byte{0x00}, 0},
		{"output short of rawLen", []byte{0x00, 0x20, 'a', 'b'}, 3},
		{"raw too long", []byte{0xFF, 'a', 'b'}, 1},
		{"raw too short", []byte{0xFF, 'a'}, 2},
		{"unknown mode", []byte{0x42, 'a'}, 1},
		{"empty payload", []byte{}, 0},
		{"negative rawLen", []byte{0xFF}, -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := DecodeBlock(nil, c.payload, c.rawLen); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func FuzzDecodeBlock(f *testing.F) {
	f.Add([]byte{0x00, 0x10, 'a', 0x01, 0x00, 0x00}, uint16(5))
	f.Add([]byte{0xFF, 'x'}, uint16(1))
	f.Add(EncodeBlock(nil, englishText(2000)), uint16(2000))
	f.Fuzz(func(t *testing.T, payload []byte, rawLen uint16) {
		out, err := DecodeBlock(nil, payload, int(rawLen))
		if err == nil && len(out) != int(rawLen) {
			t.Fatalf("no error but len(out) = %d, rawLen = %d", len(out), rawLen)
		}
	})
}

func FuzzBlockRoundTrip(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("a"))
	f.Add([]byte("abcabcabcabcabcabcabc"))
	f.Add(englishText(3000))
	f.Fuzz(func(t *testing.T, src []byte) { roundTrip(t, src) })
}

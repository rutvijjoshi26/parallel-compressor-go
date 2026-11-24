package pcz

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func headerBytes(magic string, version, flags byte, blockSize uint32) []byte {
	b := append([]byte(magic), version, flags)
	return binary.LittleEndian.AppendUint32(b, blockSize)
}

func frameHeaderBytes(rawLen, compLen, crc uint32) []byte {
	b := binary.LittleEndian.AppendUint32(nil, rawLen)
	b = binary.LittleEndian.AppendUint32(b, compLen)
	return binary.LittleEndian.AppendUint32(b, crc)
}

func TestHeaderRoundTrip(t *testing.T) {
	for _, bs := range []int{4 << 10, 1 << 20, 64 << 20} {
		var buf bytes.Buffer
		if err := writeHeader(&buf, bs); err != nil {
			t.Fatalf("writeHeader(%d): %v", bs, err)
		}
		if want := headerBytes("PCZ3", 3, 0, uint32(bs)); !bytes.Equal(buf.Bytes(), want) {
			t.Fatalf("header bytes = %x, want %x", buf.Bytes(), want)
		}
		got, err := readHeader(&buf)
		if err != nil {
			t.Fatalf("readHeader(%d): %v", bs, err)
		}
		if got != bs {
			t.Fatalf("block size = %d, want %d", got, bs)
		}
	}
}

func TestWriteHeaderRejectsBadBlockSize(t *testing.T) {
	for _, bs := range []int{0, -1, MinBlockSize - 1, MaxBlockSize + 1} {
		if err := writeHeader(&bytes.Buffer{}, bs); err == nil {
			t.Errorf("writeHeader(%d): expected error", bs)
		}
	}
}

func TestHeaderRejects(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"bad magic", headerBytes("PCZ2", 3, 0, 1<<20)},
		{"version 2", headerBytes("PCZ3", 2, 0, 1<<20)},
		{"nonzero flags", headerBytes("PCZ3", 3, 1, 1<<20)},
		{"block size 1KiB", headerBytes("PCZ3", 3, 0, 1<<10)},
		{"block size 128MiB", headerBytes("PCZ3", 3, 0, 128<<20)},
		{"truncated header", headerBytes("PCZ3", 3, 0, 1<<20)[:7]},
		{"empty input", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := readHeader(bytes.NewReader(c.data))
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("err = %v, want ErrFormat", err)
			}
		})
	}
}

func TestFrameRoundTrip(t *testing.T) {
	const bs = 4 << 10
	frames := []frame{
		{rawLen: 3, crc: 0x11111111, payload: []byte{0xFF, 'a', 'b', 'c'}},
		{rawLen: bs, crc: 0x22222222, payload: bytes.Repeat([]byte{0x5A}, bs+64)},
		{rawLen: 1, crc: 0xDEADBEEF, payload: []byte{0xFF, 'z'}},
	}
	var buf bytes.Buffer
	for _, f := range frames {
		if err := writeFrame(&buf, f); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeEnd(&buf); err != nil {
		t.Fatal(err)
	}
	if tail := buf.Bytes()[buf.Len()-12:]; !bytes.Equal(tail, make([]byte, 12)) {
		t.Fatalf("end frame = %x, want 12 zero bytes", tail)
	}
	r := bytes.NewReader(buf.Bytes())
	for i, want := range frames {
		got, end, err := readFrame(r, bs)
		if err != nil || end {
			t.Fatalf("frame %d: end=%v err=%v", i, end, err)
		}
		if got.rawLen != want.rawLen || got.crc != want.crc || !bytes.Equal(got.payload, want.payload) {
			t.Fatalf("frame %d mismatch: got rawLen %d crc %#x len %d", i, got.rawLen, got.crc, len(got.payload))
		}
	}
	if _, end, err := readFrame(r, bs); err != nil || !end {
		t.Fatalf("end frame: end=%v err=%v", end, err)
	}
	if r.Len() != 0 {
		t.Fatalf("%d bytes left after end frame", r.Len())
	}
}

func TestWriteFrameRejects(t *testing.T) {
	cases := []struct {
		name string
		f    frame
	}{
		{"looks like end frame", frame{rawLen: 0, payload: nil}},
		{"negative rawLen", frame{rawLen: -1, payload: []byte{0xFF}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeFrame(&buf, c.f); err == nil {
				t.Fatal("expected error")
			}
			if buf.Len() != 0 {
				t.Fatalf("wrote %d bytes on error", buf.Len())
			}
		})
	}
}

func TestFrameRejects(t *testing.T) {
	const bs = 4 << 10
	good := append(frameHeaderBytes(2, 3, 0), 0xFF, 'h', 'i')
	cases := []struct {
		name string
		data []byte
	}{
		{"rawLen > blockSize", frameHeaderBytes(bs+1, 10, 0)},
		{"compLen > blockSize+64", frameHeaderBytes(10, bs+65, 0)},
		{"compLen 0 with rawLen > 0", frameHeaderBytes(10, 0, 0)},
		{"end frame with nonzero crc", frameHeaderBytes(0, 0, 1)},
		{"truncated frame header", frameHeaderBytes(2, 3, 0)[:11]},
		{"truncated payload", good[:len(good)-1]},
		{"no frame at all", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := readFrame(bytes.NewReader(c.data), bs)
			if !errors.Is(err, ErrFormat) {
				t.Fatalf("err = %v, want ErrFormat", err)
			}
		})
	}

	t.Run("missing end frame", func(t *testing.T) {
		r := bytes.NewReader(good)
		if _, end, err := readFrame(r, bs); err != nil || end {
			t.Fatalf("first frame: end=%v err=%v", end, err)
		}
		_, _, err := readFrame(r, bs)
		if !errors.Is(err, ErrFormat) {
			t.Fatalf("err = %v, want ErrFormat", err)
		}
	})
}

func TestHugeLengthsNoAlloc(t *testing.T) {
	data := frameHeaderBytes(0xFFFFFFFF, 0xFFFFFFFF, 0)
	r := bytes.NewReader(data)
	allocs := testing.AllocsPerRun(100, func() {
		r.Reset(data)
		if _, _, err := readFrame(r, MaxBlockSize); !errors.Is(err, ErrFormat) {
			t.Fatalf("err = %v, want ErrFormat", err)
		}
	})
	if allocs > 2 {
		t.Fatalf("reject path allocated %.1f times per run, want <= 2", allocs)
	}
	// compLen alone huge, rawLen in range.
	data2 := frameHeaderBytes(1, 0xFFFFFFFF, 0)
	allocs = testing.AllocsPerRun(100, func() {
		r.Reset(data2)
		if _, _, err := readFrame(r, MaxBlockSize); !errors.Is(err, ErrFormat) {
			t.Fatalf("err = %v, want ErrFormat", err)
		}
	})
	if allocs > 2 {
		t.Fatalf("compLen reject path allocated %.1f times per run, want <= 2", allocs)
	}
}

func FuzzReadStream(f *testing.F) {
	var ok bytes.Buffer
	_ = writeHeader(&ok, MinBlockSize)
	_ = writeFrame(&ok, frame{rawLen: 2, payload: []byte{0xFF, 'h', 'i'}})
	_ = writeEnd(&ok)
	f.Add(ok.Bytes())
	f.Add(ok.Bytes()[:ok.Len()-5])
	f.Add(append(headerBytes("PCZ3", 3, 0, MinBlockSize), frameHeaderBytes(0xFFFFFFFF, 0xFFFFFFFF, 0)...))
	f.Add([]byte("PCZ3"))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		bs, err := readHeader(r)
		if err != nil {
			return
		}
		if bs < MinBlockSize || bs > MaxBlockSize {
			t.Fatalf("readHeader accepted block size %d", bs)
		}
		for {
			fr, end, err := readFrame(r, bs)
			if err != nil {
				if !errors.Is(err, ErrFormat) {
					t.Fatalf("non-format error from in-memory reader: %v", err)
				}
				return
			}
			if end {
				return
			}
			if fr.rawLen < 0 || fr.rawLen > bs || len(fr.payload) == 0 || len(fr.payload) > bs+64 {
				t.Fatalf("readFrame accepted rawLen %d compLen %d", fr.rawLen, len(fr.payload))
			}
		}
	})
}

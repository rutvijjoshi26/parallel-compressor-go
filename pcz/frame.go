package pcz

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Block size limits for the PCZ3 container.
const (
	DefaultBlockSize = 1 << 20
	MinBlockSize     = 4 << 10
	MaxBlockSize     = 64 << 20
)

const (
	formatVersion   = 3
	headerSize      = 10 // magic(4) version(1) flags(1) blockSize(4)
	frameHeaderSize = 12 // rawLen(4) compLen(4) crc32(4)
	maxPayloadSlack = 64 // compLen may exceed blockSize by at most this much
)

var magic = [4]byte{'P', 'C', 'Z', '3'}

var (
	// ErrFormat reports a malformed, truncated or hostile container.
	ErrFormat = errors.New("pcz: invalid format")
	// ErrChecksum reports a block whose decoded bytes fail the CRC-32 check.
	ErrChecksum = errors.New("pcz: checksum mismatch")
)

// Static reject errors, so the reject paths do not allocate.
var (
	errTruncHeader   = fmt.Errorf("%w: truncated header", ErrFormat)
	errBadMagic      = fmt.Errorf("%w: bad magic", ErrFormat)
	errBadVersion    = fmt.Errorf("%w: unsupported version", ErrFormat)
	errBadFlags      = fmt.Errorf("%w: unknown flags", ErrFormat)
	errBadBlockSize  = fmt.Errorf("%w: block size outside [%d, %d]", ErrFormat, MinBlockSize, MaxBlockSize)
	errNoEnd         = fmt.Errorf("%w: missing end frame", ErrFormat)
	errTruncFrame    = fmt.Errorf("%w: truncated frame header", ErrFormat)
	errTruncPayload  = fmt.Errorf("%w: truncated frame payload", ErrFormat)
	errRawTooLong    = fmt.Errorf("%w: frame raw length exceeds block size", ErrFormat)
	errCompTooLong   = fmt.Errorf("%w: frame payload length exceeds block size + %d", ErrFormat, maxPayloadSlack)
	errEmptyFrame    = fmt.Errorf("%w: frame has raw bytes but no payload", ErrFormat)
	errPayloadNoRaw  = fmt.Errorf("%w: frame has a payload but no raw bytes", ErrFormat)
	errEndFrameCRC   = fmt.Errorf("%w: end frame has nonzero crc", ErrFormat)
	errWriteEndFrame = errors.New("pcz: frame with no raw bytes and no payload would read as the end frame")
)

// frame is one block in the container: its raw length, the CRC-32 (IEEE)
// of its raw bytes, and its encoded payload.
type frame struct {
	rawLen  int
	crc     uint32
	payload []byte
}

func validBlockSize(n int) bool {
	return n >= MinBlockSize && n <= MaxBlockSize
}

// writeHeader writes the PCZ3 stream header.
func writeHeader(w io.Writer, blockSize int) error {
	if !validBlockSize(blockSize) {
		return fmt.Errorf("pcz: block size %d outside [%d, %d]", blockSize, MinBlockSize, MaxBlockSize)
	}
	var b [headerSize]byte
	copy(b[:4], magic[:])
	b[4] = formatVersion
	b[5] = 0 // flags
	binary.LittleEndian.PutUint32(b[6:], uint32(blockSize))
	_, err := w.Write(b[:])
	return err
}

// readHeader reads and validates the PCZ3 stream header.
func readHeader(r io.Reader) (blockSize int, err error) {
	var b [headerSize]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, errTruncHeader
		}
		return 0, err
	}
	switch {
	case [4]byte(b[:4]) != magic:
		return 0, errBadMagic
	case b[4] != formatVersion:
		return 0, errBadVersion
	case b[5] != 0:
		return 0, errBadFlags
	}
	n := binary.LittleEndian.Uint32(b[6:])
	if n < MinBlockSize || n > MaxBlockSize {
		return 0, errBadBlockSize
	}
	return int(n), nil
}

// writeFrame writes one block frame. compLen is len(f.payload).
func writeFrame(w io.Writer, f frame) error {
	if f.rawLen < 0 || uint64(f.rawLen) > 0xFFFFFFFF || uint64(len(f.payload)) > 0xFFFFFFFF {
		return fmt.Errorf("pcz: frame lengths out of range (raw %d, payload %d)", f.rawLen, len(f.payload))
	}
	if f.rawLen == 0 && len(f.payload) == 0 {
		return errWriteEndFrame
	}
	var b [frameHeaderSize]byte
	binary.LittleEndian.PutUint32(b[0:], uint32(f.rawLen))
	binary.LittleEndian.PutUint32(b[4:], uint32(len(f.payload)))
	binary.LittleEndian.PutUint32(b[8:], f.crc)
	if _, err := w.Write(b[:]); err != nil {
		return err
	}
	_, err := w.Write(f.payload)
	return err
}

// writeEnd writes the end frame: rawLen 0, compLen 0, crc 0.
func writeEnd(w io.Writer) error {
	var b [frameHeaderSize]byte
	_, err := w.Write(b[:])
	return err
}

// readFrame reads the next frame. It returns end=true for the end frame.
// It bounds-checks the lengths before allocating; the CRC is verified by
// the caller after decoding.
func readFrame(r io.Reader, blockSize int) (f frame, end bool, err error) {
	return readFrameBuf(r, blockSize, nil)
}

// payloadChunk caps the initial payload allocation, so a truncated stream
// that claims a large compLen cannot force a large allocation up front.
const payloadChunk = 64 << 10

// readFrameBuf is readFrame, reading the payload into buf's storage when its
// capacity suffices (the returned payload may alias buf).
func readFrameBuf(r io.Reader, blockSize int, buf []byte) (f frame, end bool, err error) {
	var b [frameHeaderSize]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		switch err {
		case io.EOF:
			return frame{}, false, errNoEnd
		case io.ErrUnexpectedEOF:
			return frame{}, false, errTruncFrame
		}
		return frame{}, false, err
	}
	rawLen := uint64(binary.LittleEndian.Uint32(b[0:]))
	compLen := uint64(binary.LittleEndian.Uint32(b[4:]))
	crc := binary.LittleEndian.Uint32(b[8:])
	switch {
	case rawLen == 0 && compLen == 0:
		if crc != 0 {
			return frame{}, false, errEndFrameCRC
		}
		return frame{}, true, nil
	case rawLen == 0:
		return frame{}, false, errPayloadNoRaw
	case rawLen > uint64(blockSize):
		return frame{}, false, errRawTooLong
	case compLen > uint64(blockSize)+maxPayloadSlack:
		return frame{}, false, errCompTooLong
	case compLen == 0:
		return frame{}, false, errEmptyFrame
	}
	payload, err := readPayload(r, int(compLen), buf)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return frame{}, false, errTruncPayload
		}
		return frame{}, false, err
	}
	return frame{rawLen: int(rawLen), crc: crc, payload: payload}, false, nil
}

// readPayload reads exactly n bytes. It reuses buf when cap(buf) >= n.
// Otherwise it starts at min(n, payloadChunk) bytes and doubles (capped at
// n) only as bytes actually arrive, so the allocation tracks real input.
func readPayload(r io.Reader, n int, buf []byte) ([]byte, error) {
	if cap(buf) >= n {
		p := buf[:n]
		_, err := io.ReadFull(r, p)
		return p, err
	}
	p := make([]byte, 0, min(n, payloadChunk))
	for len(p) < n {
		if len(p) == cap(p) {
			q := make([]byte, len(p), min(2*cap(p), n))
			copy(q, p)
			p = q
		}
		m, err := io.ReadFull(r, p[len(p):cap(p)])
		p = p[:len(p)+m]
		if err != nil {
			if err == io.EOF && len(p) > 0 {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
	return p, nil
}

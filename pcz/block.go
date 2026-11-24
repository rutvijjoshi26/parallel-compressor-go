// Package pcz implements the PCZ3 parallel block compressor.
package pcz

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

// Payload mode bytes.
const (
	modeLZ  byte = 0x00
	modeRaw byte = 0xFF
)

const (
	minMatch   = 4
	hashLog    = 14
	hashSize   = 1 << hashLog
	maxOffset  = 1<<16 - 1
	nibbleMore = 15
)

var (
	errEmptyPayload = errors.New("pcz: empty block payload")
	errTruncated    = errors.New("pcz: truncated block payload")
	errBadOffset    = errors.New("pcz: invalid match offset")
	errOverrun      = errors.New("pcz: block output overruns raw length")
	errShort        = errors.New("pcz: block output shorter than raw length")
)

func hash4(v uint32) uint32 {
	return (v * 2654435761) >> (32 - hashLog)
}

// appendLen appends the continuation bytes for a length whose nibble is 15.
func appendLen(dst []byte, n int) []byte {
	for n >= 255 {
		dst = append(dst, 255)
		n -= 255
	}
	return append(dst, byte(n))
}

func nibble(n int) byte {
	if n >= nibbleMore {
		return nibbleMore
	}
	return byte(n)
}

// appendSequence appends one sequence. matchLen == 0 marks the last
// sequence, which carries literals only and no offset.
func appendSequence(dst, lits []byte, offset, matchLen int) []byte {
	ml := 0
	if matchLen > 0 {
		ml = matchLen - minMatch
	}
	dst = append(dst, nibble(len(lits))<<4|nibble(ml))
	if len(lits) >= nibbleMore {
		dst = appendLen(dst, len(lits)-nibbleMore)
	}
	dst = append(dst, lits...)
	if matchLen == 0 {
		return dst
	}
	dst = binary.LittleEndian.AppendUint16(dst, uint16(offset))
	if ml >= nibbleMore {
		dst = appendLen(dst, ml-nibbleMore)
	}
	return dst
}

// EncodeBlock appends the payload for src (mode byte plus body) to dst.
// It stores src raw (mode 0xFF) unless the LZ form is strictly smaller.
func EncodeBlock(dst, src []byte) []byte {
	start := len(dst)
	dst = append(dst, modeLZ)
	dst = encodeLZ(dst, src, start+1+len(src))
	if len(dst)-start < 1+len(src) {
		return dst
	}
	dst = append(dst[:start], modeRaw)
	return append(dst, src...)
}

// encodeLZ appends the sequence stream for src to dst. It stops early once
// the output reaches limit bytes, since raw mode will win anyway.
func encodeLZ(dst, src []byte, limit int) []byte {
	var table [hashSize]int32 // position+1; 0 means empty
	anchor := 0
	i := 0
	for i+minMatch <= len(src) {
		if len(dst) >= limit {
			return dst
		}
		v := binary.LittleEndian.Uint32(src[i:])
		h := hash4(v)
		cand := int(table[h]) - 1
		table[h] = int32(i + 1)
		if cand < 0 || i-cand > maxOffset || binary.LittleEndian.Uint32(src[cand:]) != v {
			i++
			continue
		}
		n := minMatch
		for i+n < len(src) && src[cand+n] == src[i+n] {
			n++
		}
		dst = appendSequence(dst, src[anchor:i], i-cand, n)
		i += n
		anchor = i
	}
	return appendSequence(dst, src[anchor:], 0, 0)
}

// readLen reads continuation bytes for a nibble of 15, adding them to n.
func readLen(p []byte, pos, n, max int) (int, int, error) {
	for {
		if pos >= len(p) {
			return 0, 0, errTruncated
		}
		b := p[pos]
		pos++
		n += int(b)
		if n > max {
			return 0, 0, errOverrun
		}
		if b < 255 {
			return n, pos, nil
		}
	}
}

// DecodeBlock appends exactly rawLen decoded bytes to dst, or returns an error.
func DecodeBlock(dst, payload []byte, rawLen int) ([]byte, error) {
	if rawLen < 0 {
		return dst, fmt.Errorf("pcz: negative raw length %d", rawLen)
	}
	if len(payload) == 0 {
		return dst, errEmptyPayload
	}
	body := payload[1:]
	switch payload[0] {
	case modeRaw:
		if len(body) != rawLen {
			return dst, fmt.Errorf("pcz: raw block has %d bytes, want %d", len(body), rawLen)
		}
		return append(dst, body...), nil
	case modeLZ:
		return decodeLZ(dst, body, rawLen)
	default:
		return dst, fmt.Errorf("pcz: unknown block mode %#x", payload[0])
	}
}

func decodeLZ(dst, p []byte, rawLen int) ([]byte, error) {
	base := len(dst)
	orig := dst
	dst = slices.Grow(dst, rawLen) // rawLen is bounded by the caller (≤ blockSize)
	pos := 0
	for {
		if pos >= len(p) {
			return orig, errTruncated
		}
		token := p[pos]
		pos++
		room := rawLen - (len(dst) - base)

		lit := int(token >> 4)
		if lit == nibbleMore {
			var err error
			if lit, pos, err = readLen(p, pos, lit, room); err != nil {
				return orig, err
			}
		}
		if lit > room {
			return orig, errOverrun
		}
		if lit > len(p)-pos {
			return orig, errTruncated
		}
		dst = append(dst, p[pos:pos+lit]...)
		pos += lit
		room -= lit
		if pos == len(p) {
			break // last sequence: literals only
		}

		if len(p)-pos < 2 {
			return orig, errTruncated
		}
		offset := int(binary.LittleEndian.Uint16(p[pos:]))
		pos += 2
		if offset == 0 || offset > len(dst)-base {
			return orig, errBadOffset
		}
		ml := int(token & 0x0F)
		if ml == nibbleMore {
			var err error
			if ml, pos, err = readLen(p, pos, ml, room); err != nil {
				return orig, err
			}
		}
		ml += minMatch
		if ml > room {
			return orig, errOverrun
		}
		from := len(dst) - offset
		for k := 0; k < ml; k++ { // byte-wise: overlapping copies are valid
			dst = append(dst, dst[from+k])
		}
	}
	if len(dst)-base != rawLen {
		return orig, errShort
	}
	return dst, nil
}

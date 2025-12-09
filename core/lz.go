package core

import (
	"fmt"
)

const (
	lzWindowSize = 65535 // 64KB sliding window
	lzMinMatch   = 4     // Minimum match length
	lzMaxMatch   = 255   // Maximum match length (1 byte to store length)
	hashBits     = 14    // 16K entries
	hashSize     = 1 << hashBits
)

// lzCompressTokens uses a Hash-based LZ77 implementation.
func lzCompressTokens(input []byte) []byte {
	if len(input) == 0 {
		return nil
	}

	out := make([]byte, 0, len(input))

	// Hash table stores the index of the last occurrence of a 4-byte sequence.
	// -1 indicates no history.
	table := make([]int, hashSize)
	for i := range table {
		table[i] = -1
	}

	i := 0
	for i < len(input) {
		if i+lzMinMatch > len(input) {
			out = append(out, 0x00, input[i])
			i++
			continue
		}

		// 1. Hash the next 4 bytes
		h := ((uint32(input[i]) << 24) ^ (uint32(input[i+1]) << 16) ^ (uint32(input[i+2]) << 8) ^ uint32(input[i+3]))
		h = (h * 0x1e35a7bd) >> (32 - hashBits)

		candidate := table[h]
		table[h] = i

		// 2. Check if candidate is valid match
		// - Must be within window
		// - Must actually match (hash collision check)
		if candidate != -1 && (i-candidate) < lzWindowSize && i-candidate > 0 {
			if input[candidate] == input[i] &&
				input[candidate+1] == input[i+1] &&
				input[candidate+2] == input[i+2] &&
				input[candidate+3] == input[i+3] {

				matchLen := 4
				for i+matchLen < len(input) &&
					candidate+matchLen < len(input) &&
					matchLen < lzMaxMatch &&
					input[candidate+matchLen] == input[i+matchLen] {
					matchLen++
				}

				// Emit Match Token
				offset := i - candidate
				out = append(out, 0x01, byte(offset&0xFF), byte(offset>>8), byte(matchLen))

				i += matchLen
				continue
			}
		}

		// No match found, emit literal
		out = append(out, 0x00, input[i])
		i++
	}

	return out
}

// lzDecompressTokens decompresses LZ77 tokens back to original data.
func lzDecompressTokens(tokens []byte, expectedSize int) ([]byte, error) {
	if len(tokens) == 0 && expectedSize == 0 {
		return nil, nil
	}

	out := make([]byte, 0, expectedSize)
	i := 0

	for i < len(tokens) {
		flag := tokens[i]
		i++

		switch flag {
		case 0x00:
			if i >= len(tokens) {
				return nil, fmt.Errorf("truncated literal")
			}
			out = append(out, tokens[i])
			i++

		case 0x01:
			if i+3 > len(tokens) {
				return nil, fmt.Errorf("truncated match")
			}
			offset := int(tokens[i]) | int(tokens[i+1])<<8
			length := int(tokens[i+2])
			i += 3

			if offset <= 0 || offset > len(out) {
				return nil, fmt.Errorf("invalid match offset %d (out len %d)", offset, len(out))
			}

			start := len(out) - offset
			for j := 0; j < length; j++ {
				out = append(out, out[start+j])
			}

		default:
			return nil, fmt.Errorf("invalid token flag 0x%02x", flag)
		}
	}

	if len(out) != expectedSize {
		return nil, fmt.Errorf("size mismatch: got %d, expected %d", len(out), expectedSize)
	}
	return out, nil
}

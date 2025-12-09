// internal/core/sequential.go
package core

import (
	"fmt"
	"io"
	"os"
)

// DefaultBlockSize is the global block size for sequential compression.
var DefaultBlockSize uint32 = 1024 * 1024

func SetBlockSizeBytes(n uint32) {
	if n < 4*1024 {
		n = 4 * 1024
	}
	if n > 4*1024*1024 {
		n = 4 * 1024 * 1024
	}
	DefaultBlockSize = n
}

// SequentialCompressFile:
//   - opens inputPath
//   - splits into blocks (DefaultBlockSize)
//   - per block: try LZ tokens (0x00), else raw (0xFF)
//   - writes header + block table + blocks
func SequentialCompressFile(inputPath, outputPath string) error {
	in, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return fmt.Errorf("stat input: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("input is not a regular file")
	}

	originalSize := info.Size()
	if originalSize < 0 {
		return fmt.Errorf("invalid file size: %d", originalSize)
	}

	// Empty file edge case.
	if originalSize == 0 {
		out, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("create output: %w", err)
		}
		defer out.Close()

		header := &FileHeader{
			Filename:       info.Name(),
			OriginalSize:   0,
			BlockSize:      DefaultBlockSize,
			NumBlocks:      0,
			BlockCompSizes: nil,
		}
		if err := WriteHeader(out, header); err != nil {
			return fmt.Errorf("write header: %w", err)
		}
		return nil
	}

	blockSize := int(DefaultBlockSize)
	numBlocks := (originalSize + int64(blockSize) - 1) / int64(blockSize)

	compressedBlocks := make([][]byte, numBlocks)
	blockCompSizes := make([]uint64, numBlocks)

	for blockIndex := int64(0); blockIndex < numBlocks; blockIndex++ {
		var thisBlockSize int
		if blockIndex < numBlocks-1 {
			thisBlockSize = blockSize
		} else {
			remaining := originalSize - int64(blockSize)*(numBlocks-1)
			thisBlockSize = int(remaining)
		}

		buf := make([]byte, thisBlockSize)
		if _, err := io.ReadFull(in, buf); err != nil {
			return fmt.Errorf("read block %d: %w", blockIndex, err)
		}

		tokens := lzCompressTokens(buf)

		var encoded []byte
		if len(tokens)+1 >= len(buf)+1 {
			encoded = make([]byte, 1+len(buf))
			encoded[0] = 0xFF
			copy(encoded[1:], buf)
		} else {
			encoded = make([]byte, 1+len(tokens))
			encoded[0] = 0x00
			copy(encoded[1:], tokens)
		}

		compressedBlocks[blockIndex] = encoded
		blockCompSizes[blockIndex] = uint64(len(encoded))
	}

	out, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer func() { _ = out.Close() }()

	header := &FileHeader{
		Filename:       info.Name(),
		OriginalSize:   uint64(originalSize),
		BlockSize:      uint32(blockSize),
		NumBlocks:      uint64(numBlocks),
		BlockCompSizes: blockCompSizes,
	}
	if err := WriteHeader(out, header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	for i := int64(0); i < numBlocks; i++ {
		if _, err := out.Write(compressedBlocks[i]); err != nil {
			return fmt.Errorf("write block %d: %w", i, err)
		}
	}
	return nil
}

// SequentialDecompressFile:
//   - reads header, then per block: 0xFF (raw) or 0x00 (LZ tokens)
func SequentialDecompressFile(compressedPath, outputPath string) error {
	in, err := os.Open(compressedPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer in.Close()

	header, err := ReadHeader(in)
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	out, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer func() { _ = out.Close() }()

	if header.OriginalSize == 0 || header.NumBlocks == 0 {
		return nil
	}

	blockSize := int(header.BlockSize)
	numBlocks := int(header.NumBlocks)
	originalSize := int64(header.OriginalSize)

	for blockIndex := 0; blockIndex < numBlocks; blockIndex++ {
		compSize := header.BlockCompSizes[blockIndex]
		if compSize == 0 {
			return fmt.Errorf("invalid compressed size for block %d", blockIndex)
		}

		compBuf := make([]byte, compSize)
		if _, err := io.ReadFull(in, compBuf); err != nil {
			return fmt.Errorf("read compressed block %d: %w", blockIndex, err)
		}
		if len(compBuf) == 0 {
			return fmt.Errorf("empty block %d", blockIndex)
		}

		var expectedOrigSize int
		if blockIndex < numBlocks-1 {
			expectedOrigSize = blockSize
		} else {
			fullBlocksSize := int64(blockSize) * int64(numBlocks-1)
			remaining := originalSize - fullBlocksSize
			expectedOrigSize = int(remaining)
		}

		mode := compBuf[0]
		data := compBuf[1:]

		switch mode {
		case 0xFF:
			if len(data) != expectedOrigSize {
				return fmt.Errorf("raw block %d size mismatch: got %d, expected %d",
					blockIndex, len(data), expectedOrigSize)
			}
			if _, err := out.Write(data); err != nil {
				return fmt.Errorf("write raw block %d: %w", blockIndex, err)
			}

		case 0x00:
			decompressed, err := lzDecompressTokens(data, expectedOrigSize)
			if err != nil {
				return fmt.Errorf("decompress block %d: %w", blockIndex, err)
			}
			if _, err := out.Write(decompressed); err != nil {
				return fmt.Errorf("write block %d: %w", blockIndex, err)
			}

		default:
			return fmt.Errorf("unknown block mode 0x%02x in block %d", mode, blockIndex)
		}
	}
	return nil
}

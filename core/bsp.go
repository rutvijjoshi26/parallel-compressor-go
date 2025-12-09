package core

import (
	"fmt"
	"io"
	"os"
	"sync"
)

// BSPCompressFile:
// Splits work into contiguous partitions of blocks.
// Thread 0 takes first N/T blocks, Thread 1 takes next N/T, etc.
func BSPCompressFile(inputPath, outputPath string, threads int) error {
	if threads <= 0 {
		threads = 1
	}

	info, err := os.Stat(inputPath)
	if err != nil {
		return fmt.Errorf("stat input: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("input is not a regular file")
	}

	if info.Size() == 0 {
		// Handle empty file
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

	data, err := os.ReadFile(inputPath)
	if err != nil {
		return fmt.Errorf("read input: %w", err)
	}
	originalSize := len(data)

	blockSize := int(DefaultBlockSize)
	numBlocks := (originalSize + blockSize - 1) / blockSize

	blocks := make([][]byte, numBlocks)
	for i := 0; i < numBlocks; i++ {
		s := i * blockSize
		e := s + blockSize
		if e > originalSize {
			e = originalSize
		}
		blocks[i] = data[s:e]
	}

	compressedBlocks := make([][]byte, numBlocks)
	blockCompSizes := make([]uint64, numBlocks)

	if threads > numBlocks {
		threads = numBlocks
	}
	barrier := NewBarrier(threads)
	var wg sync.WaitGroup
	wg.Add(threads)

	// Calculate partition size (N / T)
	chunkSize := numBlocks / threads
	if numBlocks%threads != 0 {
		chunkSize++
	}

	for id := 0; id < threads; id++ {
		go func(id int) {
			defer wg.Done()

			start := id * chunkSize
			end := start + chunkSize
			if start >= numBlocks {
				// This thread has no work (can happen if threads > blocks)
				start = 0
				end = 0
			}
			if end > numBlocks {
				end = numBlocks
			}

			for idx := start; idx < end; idx++ {
				buf := blocks[idx]

				tokens := lzCompressTokens(buf)
				var enc []byte
				if len(tokens)+1 >= len(buf)+1 {
					enc = make([]byte, 1+len(buf))
					enc[0] = 0xFF
					copy(enc[1:], buf)
				} else {
					enc = make([]byte, 1+len(tokens))
					enc[0] = 0x00
					copy(enc[1:], tokens)
				}

				compressedBlocks[idx] = enc
				blockCompSizes[idx] = uint64(len(enc))
			}
			barrier.Wait()
		}(id)
	}
	wg.Wait()

	out, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	header := &FileHeader{
		Filename:       info.Name(),
		OriginalSize:   uint64(originalSize),
		BlockSize:      DefaultBlockSize,
		NumBlocks:      uint64(numBlocks),
		BlockCompSizes: blockCompSizes,
	}
	if err := WriteHeader(out, header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	for i := 0; i < numBlocks; i++ {
		if _, err := out.Write(compressedBlocks[i]); err != nil {
			return fmt.Errorf("write block %d: %w", i, err)
		}
	}
	return nil
}

// BSPDecompressFile :
// Splits work into contiguous partitions of blocks.
// Thread 0 takes first N/T blocks, Thread 1 takes next N/T, etc.
func BSPDecompressFile(compressedPath, outputPath string, threads int) error {
	if threads <= 0 {
		threads = 1
	}

	in, err := os.Open(compressedPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer in.Close()

	header, err := ReadHeader(in)
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	if header.OriginalSize == 0 || header.NumBlocks == 0 {
		out, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("create output: %w", err)
		}
		defer out.Close()
		return nil
	}

	numBlocks := int(header.NumBlocks)
	blockSize := int(header.BlockSize)
	originalSize := int(header.OriginalSize)

	total := uint64(0)
	for _, s := range header.BlockCompSizes {
		total += s
	}
	compData := make([]byte, total)
	if _, err := io.ReadFull(in, compData); err != nil {
		return fmt.Errorf("read compressed payload: %w", err)
	}

	offs := make([]uint64, numBlocks)
	cur := uint64(0)
	for i := 0; i < numBlocks; i++ {
		offs[i] = cur
		cur += header.BlockCompSizes[i]
	}

	outBuf := make([]byte, originalSize)

	if threads > numBlocks {
		threads = numBlocks
	}
	barrier := NewBarrier(threads)
	var wg sync.WaitGroup
	wg.Add(threads)

	var firstErr error
	var mu sync.Mutex

	// Calculate partition size (N / T)
	chunkSize := numBlocks / threads
	if numBlocks%threads != 0 {
		chunkSize++
	}

	for id := 0; id < threads; id++ {
		go func(id int) {
			defer wg.Done()

			start := id * chunkSize
			end := start + chunkSize
			if start >= numBlocks {
				start = 0
				end = 0
			}
			if end > numBlocks {
				end = numBlocks
			}

			for idx := start; idx < end; idx++ {
				mu.Lock()
				if firstErr != nil {
					mu.Unlock()
					return
				}
				mu.Unlock()

				comp := compData[offs[idx] : offs[idx]+header.BlockCompSizes[idx]]
				if len(comp) == 0 {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("empty compressed block %d", idx)
					}
					mu.Unlock()
					return
				}

				exp := blockSize
				if idx == numBlocks-1 {
					exp = originalSize - blockSize*(numBlocks-1)
				}

				mode := comp[0]
				data := comp[1:]
				switch mode {
				case 0xFF:
					if len(data) != exp {
						mu.Lock()
						if firstErr == nil {
							firstErr = fmt.Errorf("raw block %d size mismatch: got %d, expected %d", idx, len(data), exp)
						}
						mu.Unlock()
						return
					}
					copy(outBuf[idx*blockSize:idx*blockSize+exp], data)
				case 0x00:
					dec, err := lzDecompressTokens(data, exp)
					if err != nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = fmt.Errorf("decompress block %d: %w", idx, err)
						}
						mu.Unlock()
						return
					}
					copy(outBuf[idx*blockSize:idx*blockSize+exp], dec)
				default:
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("unknown block mode 0x%02x in block %d", mode, idx)
					}
					mu.Unlock()
					return
				}
			}
			barrier.Wait()
		}(id)
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}

	out, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()
	if _, err := out.Write(outBuf); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

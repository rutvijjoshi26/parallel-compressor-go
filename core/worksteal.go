// internal/core/worksteal.go
package core

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"
)

// WorkStealingCompressFile: tasks = blocks; owner pops bottom; thieves steal top.
func WorkStealingCompressFile(inputPath, outputPath string, threads int) error {
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

	if threads > numBlocks {
		threads = numBlocks
	}
	deques := make([]*WSDeque, threads)
	for i := 0; i < threads; i++ {
		deques[i] = NewWSDeque((numBlocks + threads - 1) / threads)
	}
	for idx := 0; idx < numBlocks; idx++ {
		deques[idx%threads].PushBottom(idx)
	}

	compressedBlocks := make([][]byte, numBlocks)
	blockCompSizes := make([]uint64, numBlocks)

	var wg sync.WaitGroup
	wg.Add(threads)

	type rngState uint32
	xorshift := func(r *rngState) int {
		x := uint32(*r)
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		*r = rngState(x)
		return int(x)
	}

	const stealTries = 10

	for wid := 0; wid < threads; wid++ {
		go func(id int) {
			defer wg.Done()
			dq := deques[id]

			rs := rngState(uint32(time.Now().UnixNano()) ^ uint32(id))

			for {
				task, ok := dq.PopBottom()
				if !ok {
					// Stealing Strategy
					// 1. Fast Spin
					for t := 0; t < stealTries; t++ {
						// Fast random victim
						victimID := xorshift(&rs) % threads
						if victimID == id {
							continue
						}
						if val, stolen := deques[victimID].Steal(); stolen {
							task = val
							ok = true
							break
						}
					}

					// 2. Yield and retry if still empty
					if !ok {
						runtime.Gosched()
						for t := 0; t < stealTries; t++ {
							victimID := xorshift(&rs) % threads
							if victimID == id {
								continue
							}
							if val, stolen := deques[victimID].Steal(); stolen {
								task = val
								ok = true
								break
							}
						}
					}

					// 3. Give up
					if !ok {
						return
					}
				}

				// Process Task
				idx := task
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
		}(wid)
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

// WorkStealingDecompressFile: tasks = blocks; owner pops bottom; thieves steal top.
func WorkStealingDecompressFile(compressedPath, outputPath string, threads int) error {
	if threads <= 0 {
		threads = 1
	}

	in, err := os.Open(compressedPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer in.Close()

	h, err := ReadHeader(in)
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	if h.OriginalSize == 0 || h.NumBlocks == 0 {
		out, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("create output: %w", err)
		}
		defer out.Close()
		return nil
	}

	numBlocks := int(h.NumBlocks)
	blockSize := int(h.BlockSize)
	originalSize := int(h.OriginalSize)

	total := uint64(0)
	for _, s := range h.BlockCompSizes {
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
		cur += h.BlockCompSizes[i]
	}

	outBuf := make([]byte, originalSize)

	if threads > numBlocks {
		threads = numBlocks
	}
	deques := make([]*WSDeque, threads)
	for i := 0; i < threads; i++ {
		deques[i] = NewWSDeque((numBlocks + threads - 1) / threads)
	}
	for idx := 0; idx < numBlocks; idx++ {
		deques[idx%threads].PushBottom(idx)
	}

	var wg sync.WaitGroup
	wg.Add(threads)

	var firstErr error
	var mu sync.Mutex
	const stealTries = 6

	for wid := 0; wid < threads; wid++ {
		go func(id int) {
			defer wg.Done()
			dq := deques[id]
			r := rand.New(rand.NewSource(int64(id) ^ time.Now().UnixNano()))
			for {
				mu.Lock()
				if firstErr != nil {
					mu.Unlock()
					return
				}
				mu.Unlock()

				task, ok := dq.PopBottom()
				if !ok {
					for t := 0; t < stealTries && !ok; t++ {
						v := r.Intn(threads)
						if v == id {
							continue
						}
						if tt, ok2 := deques[v].Steal(); ok2 {
							task, ok = tt, true
						}
					}
					if !ok {
						runtime.Gosched()
						for t := 0; t < stealTries && !ok; t++ {
							v := r.Intn(threads)
							if v == id {
								continue
							}
							if tt, ok2 := deques[v].Steal(); ok2 {
								task, ok = tt, true
							}
						}
					}
					if !ok {
						return
					}
				}

				idx := task
				exp := blockSize
				if idx == numBlocks-1 {
					exp = originalSize - blockSize*(numBlocks-1)
				}

				comp := compData[offs[idx] : offs[idx]+h.BlockCompSizes[idx]]
				if len(comp) == 0 {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("empty compressed block %d", idx)
					}
					mu.Unlock()
					return
				}

				mode := comp[0]
				data := comp[1:]
				var outBlock []byte
				var derr error

				switch mode {
				case 0xFF:
					if len(data) != exp {
						derr = fmt.Errorf("raw size mismatch: got %d want %d", len(data), exp)
					} else {
						outBlock = data
					}
				case 0x00:
					outBlock, derr = lzDecompressTokens(data, exp)
				default:
					derr = fmt.Errorf("unknown mode 0x%02x", mode)
				}

				if derr != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("block %d: %w", idx, derr)
					}
					mu.Unlock()
					return
				}
				copy(outBuf[idx*blockSize:idx*blockSize+exp], outBlock)
			}
		}(wid)
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

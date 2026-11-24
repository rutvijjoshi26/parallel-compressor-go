package pcz

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// Options configures Compress and Decompress.
type Options struct {
	// Workers is the number of goroutines encoding or decoding blocks. It
	// must be at least 1.
	Workers int
	// Sched selects how blocks are distributed to workers.
	Sched Sched
	// BlockSize is the raw block size for Compress; 0 means
	// DefaultBlockSize. Decompress ignores it and uses the stream header.
	BlockSize int
}

// ErrOptions reports invalid Options.
var ErrOptions = errors.New("pcz: invalid options")

// MaxWorkers is the largest accepted Options.Workers.
const MaxWorkers = 1024

// blocksPerWorker bounds memory: at most Workers*blocksPerWorker blocks are
// in flight at once, whatever the input size.
const blocksPerWorker = 4

func (o Options) check() error {
	if o.Workers < 1 || o.Workers > MaxWorkers {
		return fmt.Errorf("%w: workers %d outside [1, %d]", ErrOptions, o.Workers, MaxWorkers)
	}
	if o.Sched != Static && o.Sched != Steal {
		return fmt.Errorf("%w: unknown scheduler %v", ErrOptions, o.Sched)
	}
	return nil
}

// Compress reads src to EOF and writes it to dst as a PCZ3 stream. The
// output is byte-identical for every Workers and Sched value. On error, dst
// may already hold the header and the frames of earlier blocks.
func Compress(dst io.Writer, src io.Reader, opt Options) error {
	if err := opt.check(); err != nil {
		return err
	}
	bs := opt.BlockSize
	if bs == 0 {
		bs = DefaultBlockSize
	}
	if !validBlockSize(bs) {
		return fmt.Errorf("%w: block size %d outside [%d, %d]", ErrOptions, bs, MinBlockSize, MaxBlockSize)
	}
	if err := writeHeader(dst, bs); err != nil {
		return err
	}

	batch := opt.Workers * blocksPerWorker
	// Grown on demand up to batch and reused across batches, so a small
	// input never allocates for workers it does not need.
	var raws [][]byte
	var frames []frame
	for eof := false; !eof; {
		// Read up to batch full blocks. io.ReadFull makes block boundaries
		// independent of how src chunks its reads.
		n := 0
		for n < batch {
			if n == len(raws) {
				raws = append(raws, make([]byte, bs))
				frames = append(frames, frame{})
			}
			m, err := io.ReadFull(src, raws[n][:bs])
			if m > 0 {
				raws[n] = raws[n][:m]
				n++
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				eof = true
				break
			}
			if err != nil {
				return err
			}
		}

		err := forEachBlock(n, opt.Workers, opt.Sched, func(i int) error {
			raw := raws[i]
			frames[i] = frame{
				rawLen:  len(raw),
				crc:     crc32.ChecksumIEEE(raw),
				payload: EncodeBlock(frames[i].payload[:0], raw),
			}
			return nil
		})
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := writeFrame(dst, frames[i]); err != nil {
				return err
			}
		}
	}
	return writeEnd(dst)
}

// Decompress reads a PCZ3 stream from src and writes the decoded bytes to
// dst. It fails with an error wrapping ErrFormat for a malformed, truncated
// or trailing-garbage stream, and ErrChecksum for a CRC mismatch. On error,
// dst may already hold the output of earlier blocks.
func Decompress(dst io.Writer, src io.Reader, opt Options) error {
	if err := opt.check(); err != nil {
		return err
	}
	bs, err := readHeader(src)
	if err != nil {
		return err
	}

	batch := opt.Workers * blocksPerWorker
	// Grown on demand up to batch; payload and output buffers are reused
	// across batches.
	var frames []frame
	var outs [][]byte
	for end := false; !end; {
		n := 0
		for n < batch {
			if n == len(frames) {
				frames = append(frames, frame{})
				outs = append(outs, nil)
			}
			f, isEnd, err := readFrameBuf(src, bs, frames[n].payload[:0])
			if err != nil {
				return err
			}
			if isEnd {
				end = true
				break
			}
			frames[n] = f
			n++
		}

		err := forEachBlock(n, opt.Workers, opt.Sched, func(i int) error {
			f := frames[i]
			out, err := DecodeBlock(outs[i][:0], f.payload, f.rawLen)
			if err != nil {
				return fmt.Errorf("%w: %w", ErrFormat, err)
			}
			outs[i] = out
			if crc32.ChecksumIEEE(out) != f.crc {
				return ErrChecksum
			}
			return nil
		})
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if _, err := dst.Write(outs[i]); err != nil {
				return err
			}
		}
	}

	// Nothing may follow the end frame.
	var b [1]byte
	m, err := io.ReadFull(src, b[:])
	if m > 0 {
		return fmt.Errorf("%w: trailing data after end frame", ErrFormat)
	}
	if err != io.EOF {
		return err
	}
	return nil
}

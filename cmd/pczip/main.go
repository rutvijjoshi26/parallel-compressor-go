// Command pczip compresses and decompresses PCZ3 streams.
//
// Usage:
//
//	pczip [-d] [-workers N] [-sched static|steal] [-block SIZE] IN OUT
//
// IN and OUT may be "-" for stdin and stdout. A file OUT is written to
// "<OUT>.tmp-<pid>" in the same directory and renamed into place only on
// success, so a failed run leaves no output file.
//
// Exit codes: 0 on success, 1 on a runtime error, 2 on a usage error.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/rutvijjoshi26/parallel-compressor-go/pcz"
)

const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

// maxDefaultWorkers caps the default -workers value. A hostile stream with
// 64 MiB blocks costs Workers × 4 × 64 MiB of memory, so the default does
// not grow with the core count past this.
const maxDefaultWorkers = 8

const usageLine = "usage: pczip [-d] [-workers N] [-sched static|steal] [-block SIZE] IN OUT"

// ioBufSize is the buffer size for the file and stdio streams.
const ioBufSize = 256 << 10

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// usageError marks an error that should exit with status 2.
type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

func usagef(format string, a ...any) error {
	return usageError{fmt.Sprintf(format, a...)}
}

// run executes pczip with args (excluding the program name) and returns the
// process exit code.
func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	err := runErr(args, stdin, stdout, stderr)
	switch {
	case err == nil:
		return exitOK
	case errors.Is(err, flag.ErrHelp):
		return exitOK
	}
	fmt.Fprintf(stderr, "pczip: %v\n", err)
	var ue usageError
	if errors.As(err, &ue) || errors.Is(err, pcz.ErrOptions) {
		fmt.Fprintln(stderr, usageLine)
		return exitUsage
	}
	return exitError
}

func runErr(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("pczip", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	decompress := fs.Bool("d", false, "decompress IN to OUT")
	workers := fs.Int("workers", min(runtime.NumCPU(), maxDefaultWorkers), "worker goroutines, 1 to 1024")
	schedName := fs.String("sched", "steal", "block scheduler: static or steal")
	blockStr := fs.String("block", "1MiB", "block size for compression, 4KiB to 64MiB")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(stderr, usageLine)
			fs.SetOutput(stderr)
			fs.PrintDefaults()
			return err
		}
		return usageError{err.Error()}
	}
	if fs.NArg() != 2 {
		return usagef("expected IN and OUT, got %d arguments", fs.NArg())
	}
	inPath, outPath := fs.Arg(0), fs.Arg(1)

	if *workers < 1 || *workers > pcz.MaxWorkers {
		return usagef("-workers %d outside [1, %d]", *workers, pcz.MaxWorkers)
	}
	sched, err := pcz.ParseSched(*schedName)
	if err != nil {
		return usagef("-sched: %v", err)
	}
	blockSize, err := parseSize(*blockStr)
	if err != nil {
		return usagef("-block: %v", err)
	}
	if blockSize < pcz.MinBlockSize || blockSize > pcz.MaxBlockSize {
		return usagef("-block %s outside [4KiB, 64MiB]", *blockStr)
	}
	if err := checkSameFile(inPath, outPath); err != nil {
		return err
	}
	opt := pcz.Options{Workers: *workers, Sched: sched, BlockSize: blockSize}

	var src io.Reader = stdin
	if inPath != "-" {
		f, err := os.Open(inPath)
		if err != nil {
			return err
		}
		defer f.Close()
		src = f
	}
	src = bufio.NewReaderSize(src, ioBufSize)

	process := func(dst io.Writer) error {
		bw := bufio.NewWriterSize(dst, ioBufSize)
		var err error
		if *decompress {
			err = pcz.Decompress(bw, src, opt)
		} else {
			err = pcz.Compress(bw, src, opt)
		}
		if err != nil {
			return err
		}
		return bw.Flush()
	}

	if outPath == "-" {
		return process(stdout)
	}
	return writeAtomic(outPath, process)
}

// checkSameFile refuses an OUT that names the same file as IN, by cleaned
// absolute path or, when both exist, by os.SameFile (which also catches
// symlinks and hard links).
func checkSameFile(inPath, outPath string) error {
	if inPath == "-" || outPath == "-" {
		return nil
	}
	inAbs, err1 := filepath.Abs(inPath)
	outAbs, err2 := filepath.Abs(outPath)
	if err1 == nil && err2 == nil && inAbs == outAbs {
		return usagef("IN and OUT are the same file: %s", inPath)
	}
	inInfo, err1 := os.Stat(inPath)
	outInfo, err2 := os.Stat(outPath)
	if err1 == nil && err2 == nil && os.SameFile(inInfo, outInfo) {
		return usagef("IN and OUT are the same file: %s and %s", inPath, outPath)
	}
	return nil
}

// writeAtomic runs fill on a temp file "<path>.tmp-<pid>" in path's
// directory and renames it to path only if fill, Sync and Close all
// succeed. On any error the temp file is removed.
func writeAtomic(path string, fill func(io.Writer) error) (err error) {
	tmp := fmt.Sprintf("%s.tmp-%d", path, os.Getpid())
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmp)
		}
	}()
	if err = fill(f); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// parseSize parses a byte count such as "65536", "64KiB" or "1MiB".
func parseSize(s string) (int, error) {
	num, mult := s, 1
	switch {
	case strings.HasSuffix(s, "KiB"):
		num, mult = strings.TrimSuffix(s, "KiB"), 1<<10
	case strings.HasSuffix(s, "MiB"):
		num, mult = strings.TrimSuffix(s, "MiB"), 1<<20
	}
	n, err := strconv.ParseUint(num, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q (want bytes, or a number with KiB or MiB)", s)
	}
	// n < 2^32 and mult <= 2^20, so v cannot overflow uint64; rejecting
	// anything above MaxBlockSize keeps int(v) exact on 32-bit platforms.
	v := uint64(n) * uint64(mult)
	if v > uint64(pcz.MaxBlockSize) {
		return 0, fmt.Errorf("size %q exceeds %d bytes", s, pcz.MaxBlockSize)
	}
	return int(v), nil
}

// Command gendata writes deterministic synthetic benchmark input.
//
// Usage:
//
//	gendata -kind text|logs|mixed|random -size SIZE -out PATH
//
// SIZE is a byte count with an optional KiB, MiB or GiB suffix. The data
// is seeded with 1, so the same kind and size always give the same file.
// See package internal/gendata for what each kind contains.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/rutvijjoshi26/parallel-compressor-go/internal/gendata"
)

const seed = 1

func main() {
	kind := flag.String("kind", "", "text, logs, mixed or random")
	sizeStr := flag.String("size", "", "output size, e.g. 256MiB")
	out := flag.String("out", "", "output path")
	flag.Parse()
	if *kind == "" || *sizeStr == "" || *out == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: gendata -kind text|logs|mixed|random -size SIZE -out PATH")
		os.Exit(2)
	}
	size, err := parseSize(*sizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gendata: %v\n", err)
		os.Exit(2)
	}
	src, err := gendata.New(*kind, seed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gendata: %v\n", err)
		os.Exit(2)
	}
	if err := write(*out, io.LimitReader(src, size)); err != nil {
		fmt.Fprintf(os.Stderr, "gendata: %v\n", err)
		os.Exit(1)
	}
}

func write(path string, src io.Reader) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	if _, err := io.Copy(bw, src); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	return f.Close()
}

func parseSize(s string) (int64, error) {
	num, mult := s, int64(1)
	for _, u := range []struct {
		suf string
		m   int64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}} {
		if strings.HasSuffix(s, u.suf) {
			num, mult = strings.TrimSuffix(s, u.suf), u.m
			break
		}
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil || n < 0 || n > (1<<40)/mult {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return n * mult, nil
}

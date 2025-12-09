package main

import (
	"flag"
	"os"

	"proj3/core"
)

func main() {
	mode := flag.String("mode", "", "Mode: compress or decompress")
	inPath := flag.String("in", "", "Input file path")
	outPath := flag.String("out", "", "Output file path")
	impl := flag.String("impl", "seq", "Implementation: seq, bsp, or ws")
	threads := flag.Int("threads", 4, "Number of worker threads for parallel implementations")

	flag.Parse()

	if *mode == "" || *inPath == "" || *outPath == "" {
		os.Exit(1)
	}

	switch *mode {
	case "compress":
		switch *impl {
		case "seq":
			core.SequentialCompressFile(*inPath, *outPath)
		case "bsp":
			core.BSPCompressFile(*inPath, *outPath, *threads)
		case "ws":
			core.WorkStealingCompressFile(*inPath, *outPath, *threads)
		default:
			os.Exit(1)
		}

	case "decompress":
		switch *impl {
		case "seq":
			core.SequentialDecompressFile(*inPath, *outPath)
		case "bsp":
			core.BSPDecompressFile(*inPath, *outPath, *threads)
		case "ws":
			core.WorkStealingDecompressFile(*inPath, *outPath, *threads)
		default:
			os.Exit(1)
		}

	default:
		os.Exit(1)
	}

}

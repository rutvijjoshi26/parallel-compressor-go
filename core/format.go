package core

import (
	"encoding/binary"
	"fmt"
	"io"
)

var magic = [4]byte{'P', 'C', 'Z', '2'}

type FileHeader struct {
	Filename       string
	OriginalSize   uint64
	BlockSize      uint32
	NumBlocks      uint64
	BlockCompSizes []uint64
}

// WriteHeader writes the custom header (including block table) to w.
func WriteHeader(w io.Writer, h *FileHeader) error {
	if _, err := w.Write(magic[:]); err != nil {
		return err
	}

	nameBytes := []byte(h.Filename)
	if len(nameBytes) > 0xFFFF {
		return fmt.Errorf("filename too long")
	}
	if err := binary.Write(w, binary.LittleEndian, uint16(len(nameBytes))); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, h.OriginalSize); err != nil {
		return err
	}

	if _, err := w.Write(nameBytes); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, h.BlockSize); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, h.NumBlocks); err != nil {
		return err
	}

	if uint64(len(h.BlockCompSizes)) != h.NumBlocks {
		return fmt.Errorf("block count mismatch")
	}
	for i := uint64(0); i < h.NumBlocks; i++ {
		if err := binary.Write(w, binary.LittleEndian, h.BlockCompSizes[i]); err != nil {
			return err
		}
	}

	return nil
}

// ReadHeader reads and validates the header (including block table).
func ReadHeader(r io.Reader) (*FileHeader, error) {
	var m [4]byte
	if _, err := io.ReadFull(r, m[:]); err != nil {
		return nil, err
	}
	if m != magic {
		return nil, fmt.Errorf("invalid magic")
	}

	var nameLen uint16
	if err := binary.Read(r, binary.LittleEndian, &nameLen); err != nil {
		return nil, err
	}

	var originalSize uint64
	if err := binary.Read(r, binary.LittleEndian, &originalSize); err != nil {
		return nil, err
	}

	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(r, nameBytes); err != nil {
		return nil, err
	}

	var blockSize uint32
	if err := binary.Read(r, binary.LittleEndian, &blockSize); err != nil {
		return nil, err
	}

	var numBlocks uint64
	if err := binary.Read(r, binary.LittleEndian, &numBlocks); err != nil {
		return nil, err
	}

	blockSizes := make([]uint64, numBlocks)
	for i := uint64(0); i < numBlocks; i++ {
		if err := binary.Read(r, binary.LittleEndian, &blockSizes[i]); err != nil {
			return nil, err
		}
	}

	return &FileHeader{
		Filename:       string(nameBytes),
		OriginalSize:   originalSize,
		BlockSize:      blockSize,
		NumBlocks:      numBlocks,
		BlockCompSizes: blockSizes,
	}, nil
}

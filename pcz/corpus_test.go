package pcz

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestCorpusRoundTrip round-trips already-compressed system files, which
// are near-incompressible and exercise the encoder's raw fallback. It skips
// in short mode and when the directories are absent.
func TestCorpusRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("corpus test skipped in short mode")
	}
	const maxFiles = 200
	var files []string
	for _, pat := range []string{"/usr/share/info/*.gz", "/usr/share/man/man1/*.gz"} {
		m, _ := filepath.Glob(pat)
		files = append(files, m...)
	}
	if len(files) > maxFiles {
		files = files[:maxFiles]
	}
	if len(files) == 0 {
		t.Skip("no *.gz files under /usr/share/info or /usr/share/man/man1")
	}
	n := 0
	for _, path := range files {
		src, err := os.ReadFile(path)
		if err != nil {
			continue // unreadable or a dangling link: not our concern
		}
		n++
		for _, bs := range []int{4 << 10, mib} {
			opt := Options{Workers: 2, BlockSize: bs}
			packed := compressBytes(t, src, opt)
			var out bytes.Buffer
			if err := Decompress(&out, bytes.NewReader(packed), Options{Workers: 2}); err != nil {
				t.Errorf("%s (block %d): decompress: %v", path, bs, err)
				continue
			}
			if !bytes.Equal(out.Bytes(), src) {
				t.Errorf("%s (block %d): round trip mismatch", path, bs)
			}
		}
	}
	t.Logf("round-tripped %d files at block sizes 4 KiB and 1 MiB", n)
}

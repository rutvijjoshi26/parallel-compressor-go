package main

import (
	"bytes"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testInput returns n bytes that are partly compressible and partly random,
// so both payload modes are exercised.
func testInput(n int) []byte {
	r := rand.New(rand.NewPCG(1, 2))
	b := make([]byte, n)
	for i := range b {
		if (i/5000)%2 == 0 {
			b[i] = byte("the quick brown fox "[i%20])
		} else {
			b[i] = byte(r.Uint32())
		}
	}
	return b
}

// runCLI calls run with the given args and returns the exit code and the
// captured stdout and stderr.
func runCLI(t *testing.T, stdin []byte, args ...string) (int, []byte, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(args, bytes.NewReader(stdin), &out, &errb)
	return code, out.Bytes(), errb.String()
}

func writeFile(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// assertNoTemp fails if any *.tmp-* file remains in dir.
func assertNoTemp(t *testing.T, dir string) {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "*.tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 0 {
		t.Fatalf("temp files left behind: %v", m)
	}
}

func assertStderrPrefixed(t *testing.T, stderr string) {
	t.Helper()
	for _, line := range strings.Split(stderr, "\n") {
		if strings.HasPrefix(line, "pczip: ") {
			return
		}
	}
	t.Fatalf("stderr has no line starting %q:\n%s", "pczip: ", stderr)
}

func TestCLIRoundTrip(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.bin")
	pcz := filepath.Join(dir, "in.pcz")
	back := filepath.Join(dir, "back.bin")
	data := testInput(300_000)
	writeFile(t, in, data)

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"defaults", nil},
		{"static-4KiB", []string{"-sched", "static", "-workers", "3", "-block", "4KiB"}},
		{"steal-64KiB", []string{"-sched", "steal", "-workers", "16", "-block", "65536"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if code, _, stderr := runCLI(t, nil, append(tc.args, in, pcz)...); code != 0 {
				t.Fatalf("compress exit %d, stderr: %s", code, stderr)
			}
			if code, _, stderr := runCLI(t, nil, append(append([]string{"-d"}, tc.args...), pcz, back)...); code != 0 {
				t.Fatalf("decompress exit %d, stderr: %s", code, stderr)
			}
			if !bytes.Equal(readFile(t, back), data) {
				t.Fatal("round trip mismatch")
			}
			assertNoTemp(t, dir)
		})
	}
}

func TestCLIOverwritesExistingOutput(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.bin")
	pcz := filepath.Join(dir, "in.pcz")
	back := filepath.Join(dir, "back.bin")
	data := testInput(10_000)
	writeFile(t, in, data)
	writeFile(t, back, bytes.Repeat([]byte("stale"), 100_000))

	if code, _, stderr := runCLI(t, nil, in, pcz); code != 0 {
		t.Fatalf("compress exit %d, stderr: %s", code, stderr)
	}
	if code, _, stderr := runCLI(t, nil, "-d", pcz, back); code != 0 {
		t.Fatalf("decompress exit %d, stderr: %s", code, stderr)
	}
	if !bytes.Equal(readFile(t, back), data) {
		t.Fatal("existing output not replaced")
	}
}

func TestCLIUsageErrors(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.bin")
	out := filepath.Join(dir, "out.pcz")
	writeFile(t, in, []byte("hello"))

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"no-args", nil},
		{"one-arg", []string{in}},
		{"three-args", []string{in, out, out}},
		{"workers-0", []string{"-workers", "0", in, out}},
		{"workers-negative", []string{"-workers", "-3", in, out}},
		{"workers-too-many", []string{"-workers", "1025", in, out}},
		{"workers-not-a-number", []string{"-workers", "x", in, out}},
		{"sched-bsp", []string{"-sched", "bsp", in, out}},
		{"block-1KiB", []string{"-block", "1KiB", in, out}},
		{"block-128MiB", []string{"-block", "128MiB", in, out}},
		{"block-garbage", []string{"-block", "1.5MiB", in, out}},
		{"block-negative", []string{"-block", "-4096", in, out}},
		{"block-huge", []string{"-block", "99999999999999999999MiB", in, out}},
		{"unknown-flag", []string{"-z", in, out}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, _, stderr := runCLI(t, nil, tc.args...)
			if code != 2 {
				t.Fatalf("exit %d, want 2; stderr: %s", code, stderr)
			}
			assertStderrPrefixed(t, stderr)
			if _, err := os.Stat(out); !os.IsNotExist(err) {
				t.Fatalf("output exists after usage error (stat err %v)", err)
			}
			assertNoTemp(t, dir)
		})
	}
}

func TestCLIBlockSizes(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int
	}{
		{"4KiB", 4 << 10},
		{"64KiB", 64 << 10},
		{"1MiB", 1 << 20},
		{"64MiB", 64 << 20},
		{"4096", 4096},
		{"100000", 100000},
	} {
		got, err := parseSize(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", tc.in, got, err, tc.want)
		}
	}
}

func TestCLIMissingInput(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out.pcz")
	code, _, stderr := runCLI(t, nil, filepath.Join(dir, "nope.bin"), out)
	if code != 1 {
		t.Fatalf("exit %d, want 1; stderr: %s", code, stderr)
	}
	assertStderrPrefixed(t, stderr)
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("output exists (stat err %v)", err)
	}
	assertNoTemp(t, dir)
}

func TestCLICorruptNoOutput(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.bin")
	pcz := filepath.Join(dir, "in.pcz")
	data := testInput(200_000)
	writeFile(t, in, data)
	if code, _, stderr := runCLI(t, nil, "-block", "4KiB", in, pcz); code != 0 {
		t.Fatalf("compress exit %d, stderr: %s", code, stderr)
	}
	good := readFile(t, pcz)

	flip := bytes.Clone(good)
	flip[len(flip)/2] ^= 0x40
	cases := map[string][]byte{
		"flipped-byte": flip,
		"truncated":    good[:len(good)-5],
		"no-end-frame": good[:len(good)-12],
		"bad-magic":    append([]byte("PCZ2"), good[4:]...),
		"empty":        {},
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			bad := filepath.Join(dir, name+".pcz")
			out := filepath.Join(dir, name+".out")
			writeFile(t, bad, b)
			code, _, stderr := runCLI(t, nil, "-d", bad, out)
			if code != 1 {
				t.Fatalf("exit %d, want 1; stderr: %s", code, stderr)
			}
			assertStderrPrefixed(t, stderr)
			if _, err := os.Stat(out); !os.IsNotExist(err) {
				t.Fatalf("output exists after failed decompress (stat err %v)", err)
			}
			assertNoTemp(t, dir)
		})
	}
}

func TestCLISameInOut(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.bin")
	data := testInput(50_000)
	writeFile(t, in, data)
	link := filepath.Join(dir, "link.bin")
	if err := os.Symlink(in, link); err != nil {
		t.Fatal(err)
	}
	hard := filepath.Join(dir, "hard.bin")
	if err := os.Link(in, hard); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"same-path", []string{in, in}},
		{"unclean-path", []string{in, filepath.Join(dir, ".", "sub", "..", "in.bin")}},
		{"symlink", []string{in, link}},
		{"hardlink", []string{in, hard}},
		{"decompress", []string{"-d", in, in}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, _, stderr := runCLI(t, nil, tc.args...)
			if code != 2 {
				t.Fatalf("exit %d, want 2; stderr: %s", code, stderr)
			}
			assertStderrPrefixed(t, stderr)
			if !bytes.Equal(readFile(t, in), data) {
				t.Fatal("input modified")
			}
			assertNoTemp(t, dir)
		})
	}
}

func TestCLIUnwritableDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	in := filepath.Join(dir, "in.bin")
	writeFile(t, in, testInput(10_000))
	ro := filepath.Join(dir, "ro")
	if err := os.Mkdir(ro, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(ro, 0o700) })
	out := filepath.Join(ro, "out.pcz")

	code, _, stderr := runCLI(t, nil, in, out)
	if code != 1 {
		t.Fatalf("exit %d, want 1; stderr: %s", code, stderr)
	}
	assertStderrPrefixed(t, stderr)
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("output exists (stat err %v)", err)
	}
	assertNoTemp(t, ro)
}

func TestCLIStdio(t *testing.T) {
	data := testInput(150_000)
	code, compressed, stderr := runCLI(t, data, "-block", "64KiB", "-", "-")
	if code != 0 {
		t.Fatalf("compress exit %d, stderr: %s", code, stderr)
	}
	if !bytes.HasPrefix(compressed, []byte("PCZ3")) {
		t.Fatalf("stdout does not start with PCZ3 magic")
	}
	code, back, stderr := runCLI(t, compressed, "-d", "-", "-")
	if code != 0 {
		t.Fatalf("decompress exit %d, stderr: %s", code, stderr)
	}
	if !bytes.Equal(back, data) {
		t.Fatal("stdio round trip mismatch")
	}

	// A corrupt stream on stdin is still a runtime error.
	bad := bytes.Clone(compressed)
	bad[len(bad)/2] ^= 0x01
	if code, _, stderr := runCLI(t, bad, "-d", "-", "-"); code != 1 {
		t.Fatalf("corrupt stdin: exit %d, want 1; stderr: %s", code, stderr)
	}
}

func TestCLIStdioMixedWithFiles(t *testing.T) {
	dir := t.TempDir()
	pcz := filepath.Join(dir, "x.pcz")
	data := testInput(20_000)
	if code, _, stderr := runCLI(t, data, "-", pcz); code != 0 {
		t.Fatalf("compress exit %d, stderr: %s", code, stderr)
	}
	code, back, stderr := runCLI(t, nil, "-d", pcz, "-")
	if code != 0 {
		t.Fatalf("decompress exit %d, stderr: %s", code, stderr)
	}
	if !bytes.Equal(back, data) {
		t.Fatal("round trip mismatch")
	}
}

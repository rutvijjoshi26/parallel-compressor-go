package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestMain lets the test binary act as pczip when PCZIP_TEST_MAIN is set,
// so the signal test can run the real main in a child process without a
// separate go build.
func TestMain(m *testing.M) {
	if os.Getenv("PCZIP_TEST_MAIN") == "1" {
		main()
		return
	}
	os.Exit(m.Run())
}

// TestCLIInterruptRemovesTemp starts pczip writing a file from a pipe that
// never closes, waits for the temp file to appear, sends the signal, and
// checks for exit 130 with neither the temp file nor the output left.
func TestCLIInterruptRemovesTemp(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no SIGINT delivery to a child on windows")
	}
	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			dir := t.TempDir()
			out := filepath.Join(dir, "out.pcz")

			// TestMain runs main before the testing package parses flags,
			// so the arguments go straight to pczip.
			cmd := exec.Command(os.Args[0], "-workers", "2", "-", out)
			cmd.Env = append(os.Environ(), "PCZIP_TEST_MAIN=1")
			// Our own pipe, so we control when data flows and can keep it
			// open (pczip then blocks reading stdin mid-write).
			pr, pw, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer pw.Close()
			cmd.Stdin = pr
			var stderr strings.Builder
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			pr.Close()
			// Feed 3 MiB so at least the header and some blocks are being
			// produced; the pipe then stays open, so pczip blocks reading.
			if _, err := pw.Write(testInput(3 << 20)); err != nil {
				t.Fatal(err)
			}

			tmp := fmt.Sprintf("%s.tmp-%d", out, cmd.Process.Pid)
			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err := os.Stat(tmp); err == nil {
					break
				}
				if time.Now().After(deadline) {
					cmd.Process.Kill()
					cmd.Wait()
					t.Fatalf("temp file %s never appeared; stderr: %s", tmp, stderr.String())
				}
				time.Sleep(5 * time.Millisecond)
			}

			if err := cmd.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			err = cmd.Wait()
			var ee *exec.ExitError
			if !errors.As(err, &ee) || ee.ExitCode() != 130 {
				t.Fatalf("wait: %v, want exit status 130; stderr: %s", err, stderr.String())
			}
			assertNoTemp(t, dir)
			if _, err := os.Stat(out); !os.IsNotExist(err) {
				t.Fatalf("output exists after interrupt (stat err %v)", err)
			}
		})
	}
}

// TestTempGuardInterruptAfterRelease checks that a signal arriving after
// the rename does not delete the renamed output's former temp path or the
// output itself.
func TestTempGuardInterruptAfterRelease(t *testing.T) {
	dir := t.TempDir()
	tmp := filepath.Join(dir, "x.tmp-1")
	out := filepath.Join(dir, "x")
	writeFile(t, tmp, []byte("data"))
	g := &tempGuard{path: tmp}
	if err := g.release(func() error { return os.Rename(tmp, out) }); err != nil {
		t.Fatal(err)
	}
	// A new file at the old temp path must survive: interrupt is disarmed.
	writeFile(t, tmp, []byte("other"))
	g.interrupt()
	if _, err := os.Stat(tmp); err != nil {
		t.Fatalf("interrupt after release removed %s: %v", tmp, err)
	}
	if got := string(readFile(t, out)); got != "data" {
		t.Fatalf("output = %q", got)
	}
}

func TestTempGuardInterruptRemoves(t *testing.T) {
	dir := t.TempDir()
	tmp := filepath.Join(dir, "x.tmp-1")
	writeFile(t, tmp, []byte("partial"))
	g := &tempGuard{path: tmp}
	g.interrupt()
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temp file still exists (stat err %v)", err)
	}
}

func TestCLIHelp(t *testing.T) {
	code, out, stderr := runCLI(t, nil, "-h")
	if code != 0 {
		t.Fatalf("-h exit %d, want 0", code)
	}
	if len(out) != 0 {
		t.Fatalf("-h wrote to stdout: %q", out)
	}
	for _, want := range []string{
		usageLine,
		`"-" for stdin and stdout`,
		"0    success",
		"1    runtime error",
		"2    usage error",
		"130  interrupted",
		"-workers",
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("-h output lacks %q:\n%s", want, stderr)
		}
	}
}

func TestCLIDefaultWorkers(t *testing.T) {
	want := min(runtime.NumCPU(), 8)
	_, _, stderr := runCLI(t, nil, "-h")
	// The flag package prints "(default N)" on the line after "-workers".
	i := strings.Index(stderr, "\n  -workers")
	if i < 0 {
		t.Fatalf("no -workers in help:\n%s", stderr)
	}
	rest := stderr[i+1:]
	if j := strings.Index(rest, "\n  -"); j >= 0 {
		rest = rest[:j]
	}
	if !strings.Contains(rest, fmt.Sprintf("(default %d)", want)) {
		t.Fatalf("-workers default not %d:\n%s", want, rest)
	}
}

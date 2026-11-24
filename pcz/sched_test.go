package pcz

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

var allScheds = []Sched{Static, Steal}

func TestEachIndexOnce(t *testing.T) {
	for _, sched := range allScheds {
		for _, workers := range []int{1, 2, 3, 8, 64} {
			for _, n := range []int{0, 1, 7, 1000} {
				name := fmt.Sprintf("%s/w%d/n%d", sched, workers, n)
				t.Run(name, func(t *testing.T) {
					counts := make([]atomic.Int32, n)
					err := forEachBlock(n, workers, sched, func(i int) error {
						counts[i].Add(1)
						return nil
					})
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					for i := range counts {
						if c := counts[i].Load(); c != 1 {
							t.Fatalf("index %d ran %d times, want 1", i, c)
						}
					}
				})
			}
		}
	}
}

func TestErrorNoDeadlock(t *testing.T) {
	for _, sched := range allScheds {
		t.Run(sched.String(), func(t *testing.T) {
			want := errors.New("block 3 failed")
			done := make(chan error, 1)
			go func() {
				done <- forEachBlock(1000, 4, sched, func(i int) error {
					if i == 3 {
						return want
					}
					return nil
				})
			}()
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("got error %v, want %v", err, want)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("forEachBlock did not return within 2s after an error")
			}
		})
	}
}

func TestPanicFreeOnZeroWorkers(t *testing.T) {
	for _, sched := range allScheds {
		t.Run(sched.String(), func(t *testing.T) {
			const n = 10
			counts := make([]int, n) // no atomics: workers 0 must run inline
			err := forEachBlock(n, 0, sched, func(i int) error {
				counts[i]++
				return nil
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for i, c := range counts {
				if c != 1 {
					t.Fatalf("index %d ran %d times, want 1", i, c)
				}
			}
		})
	}
}

func TestParseSched(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Sched
	}{{"static", Static}, {"steal", Steal}} {
		got, err := ParseSched(tc.in)
		if err != nil {
			t.Fatalf("ParseSched(%q): unexpected error %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("ParseSched(%q) = %v, want %v", tc.in, got, tc.want)
		}
		if got.String() != tc.in {
			t.Fatalf("%v.String() = %q, want %q", got, got.String(), tc.in)
		}
	}
	if _, err := ParseSched("bsp"); err == nil {
		t.Fatal(`ParseSched("bsp"): expected an error`)
	}
}

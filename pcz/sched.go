package pcz

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/rutvijjoshi26/parallel-compressor-go/internal/chaselev"
)

// Sched selects how forEachBlock distributes block indices to workers.
type Sched int

const (
	// Static gives each worker one contiguous chunk of indices.
	Static Sched = iota
	// Steal deals indices round-robin into per-worker Chase–Lev deques;
	// idle workers steal from the others.
	Steal
)

// String returns "static" or "steal".
func (s Sched) String() string {
	switch s {
	case Static:
		return "static"
	case Steal:
		return "steal"
	default:
		return fmt.Sprintf("Sched(%d)", int(s))
	}
}

// ParseSched parses "static" or "steal".
func ParseSched(s string) (Sched, error) {
	switch s {
	case "static":
		return Static, nil
	case "steal":
		return Steal, nil
	default:
		return 0, fmt.Errorf("pcz: unknown scheduler %q (want static or steal)", s)
	}
}

// errState records the first error from any worker. Once set, workers stop
// taking new work. There is no barrier: workers simply return, and the caller
// waits on a WaitGroup, so an early return can never deadlock the others.
type errState struct {
	failed atomic.Bool
	once   sync.Once
	err    error
	// onFail, when non-nil, runs once right after the first error is
	// recorded. It is a test seam (nil in production) that lets tests order
	// events deterministically around the failure.
	onFail func()
}

func (e *errState) set(err error) {
	e.once.Do(func() {
		e.err = err
		e.failed.Store(true)
		if e.onFail != nil {
			e.onFail()
		}
	})
}

// forEachBlock runs fn once for each i in [0,n) using at most workers
// goroutines and returns the first error. After an error, work not yet
// started is skipped. With workers <= 1 or n <= 1, fn runs inline on the
// caller's goroutine.
func forEachBlock(n, workers int, sched Sched, fn func(i int) error) error {
	return forEachBlockHook(n, workers, sched, fn, nil)
}

// forEachBlockHook is forEachBlock with a test seam: onFail, when non-nil,
// runs once right after the first worker error is recorded (parallel paths
// only).
func forEachBlockHook(n, workers int, sched Sched, fn func(i int) error, onFail func()) error {
	if sched != Static && sched != Steal {
		return fmt.Errorf("pcz: unknown scheduler %v", sched)
	}
	if n <= 0 {
		return nil
	}
	if workers > n {
		workers = n
	}
	if workers <= 1 || n <= 1 {
		for i := 0; i < n; i++ {
			if err := fn(i); err != nil {
				return err
			}
		}
		return nil
	}
	if sched == Static {
		return runStatic(n, workers, fn, onFail)
	}
	return runSteal(n, workers, fn, onFail)
}

// runStatic splits [0,n) into workers contiguous chunks whose sizes differ
// by at most one.
func runStatic(n, workers int, fn func(i int) error, onFail func()) error {
	es := errState{onFail: onFail}
	var wg sync.WaitGroup
	base, extra := n/workers, n%workers
	lo := 0
	for w := 0; w < workers; w++ {
		hi := lo + base
		if w < extra {
			hi++
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				if es.failed.Load() {
					return
				}
				if err := fn(i); err != nil {
					es.set(err)
					return
				}
			}
		}(lo, hi)
		lo = hi
	}
	wg.Wait()
	return es.err
}

// runSteal deals [0,n) round-robin into one deque per worker. Each worker
// pops its own deque, then steals from the others in random order.
//
// A failed Steal does not mean the victim is empty: it can also lose the CAS
// race for the top task. So workers count taken tasks in a shared remaining
// counter and exit only when it reaches zero (or an error is recorded),
// yielding between fruitless steal rounds.
func runSteal(n, workers int, fn func(i int) error, onFail func()) error {
	perWorker := (n + workers - 1) / workers
	deques := make([]*chaselev.Deque, workers)
	for w := range deques {
		deques[w] = chaselev.New(perWorker)
	}
	// Deal before any goroutine starts, so Push (owner-only) is not concurrent
	// with the owner's Pop.
	for i := 0; i < n; i++ {
		if !deques[i%workers].Push(i) {
			return fmt.Errorf("pcz: scheduler deque %d full at task %d (capacity %d)", i%workers, i, perWorker)
		}
	}

	es := errState{onFail: onFail}
	var remaining atomic.Int64
	remaining.Store(int64(n))
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(self int) {
			defer wg.Done()
			victims := make([]int, 0, workers-1)
			for v := 0; v < workers; v++ {
				if v != self {
					victims = append(victims, v)
				}
			}
			run := func(i int) bool {
				remaining.Add(-1)
				if es.failed.Load() {
					return false
				}
				if err := fn(i); err != nil {
					es.set(err)
					return false
				}
				return true
			}
			for {
				if es.failed.Load() {
					return
				}
				if i, ok := deques[self].Pop(); ok {
					if !run(i) {
						return
					}
					continue
				}
				stole := false
				rand.Shuffle(len(victims), func(a, b int) {
					victims[a], victims[b] = victims[b], victims[a]
				})
				for _, v := range victims {
					if i, ok := deques[v].Steal(); ok {
						if !run(i) {
							return
						}
						stole = true
						break
					}
				}
				if stole {
					continue
				}
				if remaining.Load() == 0 {
					return
				}
				runtime.Gosched()
			}
		}(w)
	}
	wg.Wait()
	return es.err
}

package chaselev

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

func drainPop(d *Deque) []int {
	var out []int
	for {
		v, ok := d.Pop()
		if !ok {
			return out
		}
		out = append(out, v)
	}
}

func TestPushFull(t *testing.T) {
	d := New(2)
	got := []bool{d.Push(1), d.Push(2), d.Push(3)}
	if want := []bool{true, true, false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Push results = %v, want %v", got, want)
	}
	if got, want := drainPop(d), []int{2, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("drain = %v, want %v", got, want)
	}
}

func TestCapacityRounding(t *testing.T) {
	for _, tc := range []struct{ req, want int }{{-5, 1}, {0, 1}, {1, 1}, {3, 4}, {4, 4}, {5, 8}} {
		d := New(tc.req)
		n := 0
		for d.Push(n) {
			n++
		}
		if n != tc.want {
			t.Errorf("New(%d) holds %d, want %d", tc.req, n, tc.want)
		}
	}
}

func TestOwnerLIFO(t *testing.T) {
	d := New(8)
	for i := 1; i <= 5; i++ {
		if !d.Push(i) {
			t.Fatalf("Push(%d) = false", i)
		}
	}
	if got, want := drainPop(d), []int{5, 4, 3, 2, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Pop order = %v, want %v", got, want)
	}
	if _, ok := d.Pop(); ok {
		t.Fatal("Pop on empty deque returned ok")
	}
}

func TestStealFIFO(t *testing.T) {
	d := New(8)
	for i := 1; i <= 5; i++ {
		d.Push(i)
	}
	var got []int
	for {
		v, ok := d.Steal()
		if !ok {
			break
		}
		got = append(got, v)
	}
	if want := []int{1, 2, 3, 4, 5}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Steal order = %v, want %v", got, want)
	}
}

// TestWrapAround checks that slots are reused correctly once top advances.
func TestWrapAround(t *testing.T) {
	d := New(4)
	next := 0
	for round := 0; round < 10; round++ {
		for d.Push(next) {
			next++
		}
		v, ok := d.Steal()
		if !ok || v != next-4 {
			t.Fatalf("round %d: Steal = %d,%v want %d", round, v, ok, next-4)
		}
		v, ok = d.Pop()
		if !ok || v != next-1 {
			t.Fatalf("round %d: Pop = %d,%v want %d", round, v, ok, next-1)
		}
		drainPop(d)
	}
}

func TestConcurrentExactlyOnce(t *testing.T) {
	const (
		total    = 100000
		capacity = 64
		thieves  = 4
	)
	d := New(capacity)
	counts := make([]atomic.Int32, total)
	var done atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < thieves; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if v, ok := d.Steal(); ok {
					counts[v].Add(1)
					continue
				}
				if done.Load() {
					// Final drain: the owner has stopped pushing.
					for {
						v, ok := d.Steal()
						if !ok {
							return
						}
						counts[v].Add(1)
					}
				}
			}
		}()
	}

	next := 0
	for next < total {
		// Push a batch that fits the capacity, spinning on Pop if full.
		for b := 0; b < capacity/2 && next < total; {
			if d.Push(next) {
				next++
				b++
			} else if v, ok := d.Pop(); ok {
				counts[v].Add(1)
			}
		}
		// Interleave some owner Pops.
		for k := 0; k < capacity/4; k++ {
			if v, ok := d.Pop(); ok {
				counts[v].Add(1)
			}
		}
	}
	for {
		v, ok := d.Pop()
		if !ok {
			break
		}
		counts[v].Add(1)
	}
	done.Store(true)
	wg.Wait()

	if v, ok := d.Pop(); ok {
		t.Fatalf("deque not empty at end: popped %d", v)
	}
	for i := range counts {
		if c := counts[i].Load(); c != 1 {
			t.Fatalf("task %d taken %d times, want 1", i, c)
		}
	}
}

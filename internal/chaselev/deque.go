// Package chaselev implements a bounded Chase–Lev work-stealing deque.
//
// One goroutine (the owner) pushes and pops at the bottom; any number of
// other goroutines (thieves) steal from the top. All shared state goes
// through sync/atomic, which is sequentially consistent in Go, as the
// Chase–Lev algorithm requires.
package chaselev

import "sync/atomic"

const cacheLineSize = 64

// Deque is a fixed-capacity Chase–Lev deque of int tasks.
type Deque struct {
	// Slots are atomic so that a thief's speculative read of a slot that the
	// owner is reusing is not a data race. The CAS on top decides who wins.
	tasks []atomic.Int64
	mask  int64

	_   [cacheLineSize]byte // keep top off the tasks/mask cache line
	top atomic.Int64

	_      [cacheLineSize]byte // keep bottom off top's cache line
	bottom atomic.Int64
}

// New returns a deque whose capacity is capacity rounded up to a power of
// two, with a minimum of 1.
func New(capacity int) *Deque {
	size := 1
	for size < capacity {
		size <<= 1
	}
	return &Deque{
		tasks: make([]atomic.Int64, size),
		mask:  int64(size - 1),
	}
}

// Push appends task at the bottom. Owner only. It returns false, and leaves
// the deque unchanged, when the deque is full; it never overwrites a task.
func (d *Deque) Push(task int) bool {
	b := d.bottom.Load()
	t := d.top.Load()
	if b-t >= int64(len(d.tasks)) {
		return false
	}
	d.tasks[b&d.mask].Store(int64(task))
	d.bottom.Store(b + 1)
	return true
}

// Pop removes and returns the most recently pushed task (LIFO). Owner only.
func (d *Deque) Pop() (int, bool) {
	// Decrement bottom before reading top, so a concurrent thief either sees
	// the reservation or loses the CAS race on the last element.
	b := d.bottom.Load() - 1
	d.bottom.Store(b)
	t := d.top.Load()
	if t > b {
		// Empty: restore bottom.
		d.bottom.Store(b + 1)
		return 0, false
	}
	task := int(d.tasks[b&d.mask].Load())
	if t == b {
		// Last element: race thieves for it with a CAS on top.
		won := d.top.CompareAndSwap(t, t+1)
		d.bottom.Store(b + 1)
		if !won {
			return 0, false
		}
	}
	return task, true
}

// Steal removes and returns the oldest task (FIFO). Safe from any goroutine.
// It reports false when the deque is empty or it lost a race.
func (d *Deque) Steal() (int, bool) {
	t := d.top.Load()
	b := d.bottom.Load()
	if t >= b {
		return 0, false
	}
	// Read the task before the CAS; it is only valid if the CAS succeeds.
	task := int(d.tasks[t&d.mask].Load())
	if !d.top.CompareAndSwap(t, t+1) {
		return 0, false
	}
	return task, true
}

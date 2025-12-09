package core

import (
	"sync/atomic"
)

const cacheLineSize = 64

// WSDeque is a lock-free Chase–Lev work-stealing deque.
type WSDeque struct {
	tasks []int
	mask  uint64

	// Padding ensures 'top' is on its own cache line, separate from 'tasks'
	_ [cacheLineSize]byte

	top atomic.Uint64

	// Padding ensures 'bottom' is on its own cache line, separate from 'top'
	_ [cacheLineSize]byte

	bottom atomic.Uint64
}

func nextPow2(n int) int {
	x := uint64(n - 1)
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	return int(x + 1)
}

// NewWSDeque allocates a deque with capacity >= requested.
func NewWSDeque(capacity int) *WSDeque {
	if capacity <= 0 {
		capacity = 1
	}
	size := nextPow2(capacity)
	return &WSDeque{
		tasks: make([]int, size),
		mask:  uint64(size - 1),
	}
}

// PushBottom: owner-only; append at bottom.
func (d *WSDeque) PushBottom(task int) {
	b := d.bottom.Load()
	d.tasks[b&d.mask] = task
	d.bottom.Store(b + 1)
}

// PopBottom: owner-only pop. Resolves last-item race with thieves via CAS on top.
func (d *WSDeque) PopBottom() (int, bool) {
	b := d.bottom.Load()
	if b == 0 {
		return 0, false
	}
	b = b - 1
	d.bottom.Store(b)

	t := d.top.Load()
	if t <= b {
		task := d.tasks[b&d.mask]
		if t == b {
			if !d.top.CompareAndSwap(t, t+1) {
				d.bottom.Store(b + 1)
				return 0, false
			}
			d.bottom.Store(b + 1)
		}
		return task, true
	}

	d.bottom.Store(b + 1)
	return 0, false
}

// Steal: thieves take from top using CAS; owner unaffected.
func (d *WSDeque) Steal() (int, bool) {
	t := d.top.Load()
	b := d.bottom.Load()
	if t >= b {
		return 0, false
	}
	task := d.tasks[t&d.mask]
	if !d.top.CompareAndSwap(t, t+1) {
		return 0, false
	}
	return task, true
}

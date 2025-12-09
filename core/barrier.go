// In this file, we implement a simple barrier synchronization primitive using sync.Cond.
package core

import "sync"

// Barrier uses a condition variable (sync.Cond) to synchronize a set of goroutines.
type Barrier struct {
	mu    sync.Mutex
	cond  *sync.Cond
	total int
	count int
}

func NewBarrier(total int) *Barrier {
	if total <= 0 {
		panic("barrier total must be > 0")
	}
	b := &Barrier{total: total}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *Barrier) Wait() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.count++
	if b.count == b.total {
		// Last goroutine to arrive: reset and wake everyone.
		b.count = 0
		b.cond.Broadcast()
	} else {
		// Wait until the last goroutine reaches the barrier.
		b.cond.Wait()
	}
}

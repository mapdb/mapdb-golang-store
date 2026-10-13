package queue

import (
	"runtime"
	"sync"
	"time"
)

// qgoid identifies the calling goroutine for the callback re-entry
// tripwire (same parsing as the btree node-lock tripwire — kept local to
// the queue package so the packages share no internals).
func qgoid() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	if n < 10 {
		return 0
	}
	var id uint64
	for i := 10; i < n && buf[i] >= '0' && buf[i] <= '9'; i++ {
		id = id*10 + uint64(buf[i]-'0')
	}
	return id
}

// chCond is a channel-backed condition variable supporting both untimed
// and timed waits (Go's sync.Cond has no timeout). Each waiter registers
// its own buffered channel under the caller's mutex BEFORE releasing it,
// so registration and notification are mutex-ordered: a signal always
// reaches a live waiter — a single shared buffered token could drop a
// second signal between a waiter's unlock and its receive, stranding it
// with work queued (Java Condition.await registers the wait under the
// lock before releasing it; this mirrors that ordering).
//
// signal pops and wakes the OLDEST waiter (Java signal/notify_one);
// broadcast closes every registered channel (signalAll). All methods must
// be called with the associated mutex held; waiters re-check their
// predicate under the mutex after every wakeup, so extra wakeups are
// harmless.
type chCond struct {
	waiters []chan struct{}
}

func newChCond() *chCond { return &chCond{} }

// register adds a per-waiter buffered channel; caller must hold mu.
func (c *chCond) register() chan struct{} {
	ch := make(chan struct{}, 1)
	c.waiters = append(c.waiters, ch)
	return ch
}

// deregister removes a still-registered waiter channel (the timeout
// path); caller must hold mu. A signal already delivered removes the
// channel first, making this a no-op.
func (c *chCond) deregister(ch chan struct{}) {
	for i, w := range c.waiters {
		if w == ch {
			c.waiters = append(c.waiters[:i], c.waiters[i+1:]...)
			return
		}
	}
}

// wait releases mu, blocks for a wakeup, and re-acquires mu.
func (c *chCond) wait(mu *sync.Mutex) {
	ch := c.register()
	mu.Unlock()
	<-ch
	mu.Lock()
}

// waitTimeout is wait with a deadline; false return = timeout elapsed.
// The mutex is always re-acquired before returning, and a timed-out
// waiter deregisters itself so a later signal cannot be absorbed by a
// dead channel (a lost wakeup).
func (c *chCond) waitTimeout(mu *sync.Mutex, d time.Duration) bool {
	ch := c.register()
	mu.Unlock()
	if d > 0 {
		t := time.NewTimer(d)
		select {
		case <-ch:
			t.Stop()
			mu.Lock()
			return true
		case <-t.C:
		}
	}
	mu.Lock()
	// signal pops the channel and posts the token under mu, so a signal
	// that raced the timeout (delivered while this waiter re-acquired mu)
	// is now visible: honour it rather than drop it.
	select {
	case <-ch:
		return true
	default:
	}
	c.deregister(ch)
	return false
}

// signal wakes the OLDEST waiter (Java Condition.signal/notify_one). The
// waiter's channel is popped BEFORE the token is posted, so the channel
// can never absorb a second signal meant for another waiter. Each wait
// channel is signaled at most once, so the send can never block.
func (c *chCond) signal() {
	if len(c.waiters) == 0 {
		return
	}
	ch := c.waiters[0]
	c.waiters = c.waiters[1:]
	ch <- struct{}{}
}

// broadcast wakes every current waiter (Java signalAll).
func (c *chCond) broadcast() {
	for _, ch := range c.waiters {
		close(ch)
	}
	c.waiters = nil
}

// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package zapcore

import (
	"sync"
	"time"

	"go.uber.org/atomic"
)

func newCounters() *counters {
	return &counters{
		counts: make(map[string]*atomic.Uint64),
	}
}

func newCounters2() *counters2 {
	return &counters2{}
}

type counters struct {
	sync.RWMutex
	counts map[string]*atomic.Uint64
}

func (c *counters) Inc(key string) uint64 {
	c.RLock()
	count, ok := c.counts[key]
	c.RUnlock()
	if ok {
		return count.Inc()
	}

	c.Lock()
	count, ok = c.counts[key]
	if ok {
		c.Unlock()
		return count.Inc()
	}

	c.counts[key] = atomic.NewUint64(1)
	c.Unlock()
	return 1
}

func (c *counters) Reset(key string) {
	c.Lock()
	count := c.counts[key]
	c.Unlock()
	count.Store(0)
}

// TODO: replace counters with counters2 once proven
const (
	// how many counters are under each lock; 8 uint64s fit in a 64-byte cache
	// line.
	_counters2PerLock    = 8
	_counters2BucketMask = _counters2PerLock - 1

	// target using 64KiB of memory
	_counters2Width = 8192

	// how many locks we need given those goals
	_counters2Locks = _counters2Width / _counters2PerLock
)

type counters2 [_counters2Width]bucket2

type bucket2 struct {
	sync.Mutex
	counts [_counters2PerLock]uint64
}

func (b *bucket2) inc(i uint32, key string) uint64 {
	var n uint64
	b.Lock()
	n = b.counts[i]
	n++
	b.counts[i] = n
	b.Unlock()
	return n
}

func (b *bucket2) reset(i uint32, key string) {
	b.Lock()
	b.counts[i] = 0
	b.Unlock()
}

func (c *counters2) Inc(key string) uint64 {
	i := c.hash(key)
	return c[i/_counters2PerLock].inc(i&_counters2BucketMask, key)
}

func (c *counters2) Reset(key string) {
	i := c.hash(key)
	c[i/_counters2PerLock].reset(i&_counters2BucketMask, key)
}

// hash hashes the key by first xor-collapsing it into a 64-bit state, and then
// permuting that state with XSH RR (randomly rotated xorshift).
//
// TODO: engineer a custom member of the PCG permutation family that targets
// our actual needed _counters2Width = 9-bit output space; this would avoid the
// modulo.
func (c *counters2) hash(key string) uint32 {
	return xshrr(xorstring(key)) % _counters2Width
}

// xorstring converts a string into a uint64 by xoring together its
// codepoints. It works by accumulating into a 64-bit "ring" which gets
// rotated by the apparent "byte width" of each codepoint.
func xorstring(s string) uint64 {
	var n uint64
	for i := 0; i < len(s); i++ {
		n = ((n & 0xff) >> 56) | (n << 8)
		n ^= uint64(s[i])
	}
	return n
}

// xshrr computes a "randomly" rotated xorshift; this is the "XSH RR"
// transformation borrowed from the PCG famiily of random generators. It
// returns a 32-bit output from a 64-bit state.
func xshrr(n uint64) uint32 {
	xorshifted := uint32(((n >> 18) ^ n) >> 27)
	rot := uint32(n >> 59)
	return (xorshifted >> rot) | (xorshifted << ((-rot) & 31))
}

// Sample creates a facility that samples incoming entries.
func Sample(fac Facility, tick time.Duration, first, thereafter int) Facility {
	return &sampler{
		Facility:   fac,
		tick:       tick,
		counts:     newCounters(),
		first:      uint64(first),
		thereafter: uint64(thereafter),
	}
}

type sampler struct {
	Facility

	tick       time.Duration
	counts     *counters
	first      uint64
	thereafter uint64
}

func (s *sampler) With(fields []Field) Facility {
	return &sampler{
		Facility:   s.Facility.With(fields),
		tick:       s.tick,
		counts:     s.counts,
		first:      s.first,
		thereafter: s.thereafter,
	}
}

func (s *sampler) Check(ent Entry, ce *CheckedEntry) *CheckedEntry {
	if !s.Enabled(ent.Level) {
		return ce
	}
	if n := s.counts.Inc(ent.Message); n > s.first {
		if n == s.first+1 {
			time.AfterFunc(s.tick, func() { s.counts.Reset(ent.Message) })
		}
		if (n-s.first)%s.thereafter != 0 {
			return ce
		}
	}
	return s.Facility.Check(ent, ce)
}

package cache

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestCacheStatsRx(t *testing.T) {
	c := New()

	// 2048-byte KV
	key := func(i int) []byte {
		// 1024 bytes
		b := make([]byte, 1024)
		binary.BigEndian.AppendUint64(b[:0], uint64(i))
		return b
	}

	value := make([]byte, 1024)

	numWrites := []int{1, 10, 100, 1000, 10000}

	for _, i := range numWrites {
		t.Run(fmt.Sprintf("NumWrites-%d", i), func(t *testing.T) {
			c.Reset()
			for j := range i {
				c.Set(key(j), value)
			}

			s := c.Statistics()

			expected := uint64(2048 * i)
			if s.Rx != expected {
				t.Errorf("incorrect rx; expected %d, got %d", expected, s.Rx)
			}
		})
	}
}

func TestCacheStatsTx(t *testing.T) {
	c := New()

	// 2048-byte KV
	key := func(i int) []byte {
		// 1024 bytes
		b := make([]byte, 1024)
		binary.BigEndian.AppendUint64(b[:0], uint64(i))
		return b
	}

	value := make([]byte, 1024)

	numReads := []int{1, 10, 100, 1000, 10000}

	for _, i := range numReads {
		t.Run(fmt.Sprintf("NumReads-%d", i), func(t *testing.T) {
			c.Reset()

			for j := range 100 {
				c.Set(key(j), value)
			}

			for j := range i {
				c.Get(nil, key(j%100))
			}

			s := c.Statistics()

			expected := uint64(1024 * i)
			if s.Tx != expected {
				t.Errorf("incorrect tx; expected %d, got %d", expected, s.Tx)
			}
		})
	}
}

func TestCacheStatsMisses(t *testing.T) {
	c := New()

	numReads := []int{1, 10, 100, 1000, 10000}

	for _, i := range numReads {
		t.Run(fmt.Sprintf("NumReads-%d", i), func(t *testing.T) {
			c.Reset()

			for range i {
				c.Get(nil, []byte("some-key"))
			}

			s := c.Statistics()

			expected := uint64(i)
			if s.Misses != expected {
				t.Errorf("incorrect misses; expected %d, got %d", expected, s.Misses)
			}
		})
	}
}

func TestCacheStatsWraps(t *testing.T) {
	c := New(WithSize(1), WithNumRings(1)) // Single ring, 1 shard (64KB)

	numWrites := []int{1, 10, 100, 200, 400, 500, 1000, 10000}

	key := func(i int) []byte {
		b := make([]byte, 1020)
		binary.BigEndian.AppendUint64(b[:0], uint64(i))
		return b
	}

	for _, i := range numWrites {
		t.Run(fmt.Sprintf("NumWrites-%d", i), func(t *testing.T) {
			c.Reset()

			for j := range i {
				c.Set(
					key(j),
					make([]byte, 1024),
				)
			}

			s := c.Statistics()

			expected := uint64(i * 2048 / 65536)
			if s.Wraps != expected {
				t.Errorf("incorrect wraps; expected %d, got %d", expected, s.Wraps)
			}
		})
	}
}

func TestCacheStatsVacuums(t *testing.T) {
	c := New(WithSize(1), WithNumRings(1)) // Single ring, 1 shard (64KB)

	numWrites := []int{10, 100, 200, 400, 500, 1000, 10000}

	key := func(i int) []byte {
		b := make([]byte, 1020)
		binary.BigEndian.AppendUint64(b[:0], uint64(i))
		return b
	}

	value := make([]byte, 1024)

	for _, i := range numWrites {
		t.Run(fmt.Sprintf("NumWrites-%d", i), func(t *testing.T) {
			c.Reset()

			for j := range i {
				k := key(j)
				c.Set(
					k,
					value,
				)
				c.Del(k)
			}

			s := c.Statistics()

			expected := uint64((2048 * i) / 65536)
			if s.Vacuums != expected {
				t.Errorf("incorrect wraps; expected %d, got %d", expected, s.Vacuums)
			}
		})
	}
}

func TestCacheStatsAllocated(t *testing.T) {
	c := New(WithSize(10000*2048), WithNumRings(1))

	numWrites := []int{10, 100, 200, 400, 500, 1000, 1500, 2000, 5000, 10000}
	//numWrites := []int{10000}

	key := func(i int) []byte {
		b := make([]byte, 1020)
		binary.BigEndian.AppendUint64(b[:0], uint64(i))
		return b
	}

	value := make([]byte, 1024)

	for _, i := range numWrites {
		t.Run(fmt.Sprintf("NumWrites-%d", i), func(t *testing.T) {
			c.Reset()

			t.Log()

			for j := range i {
				c.Set(
					key(j),
					value,
				)
			}

			s := c.Statistics()

			expected := uint64((2048*i/shardSize + 1) * shardSize)
			if s.Allocated < expected {
				t.Errorf("incorrect allocations; expected at least %d, got %d", expected, s.Allocated)
			}
		})
	}
}

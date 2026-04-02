package cache

import (
	"github.com/zeebo/xxh3"
)

// Cache is a thread-safe, memory KV cache similar to
// https://github.com/VictoriaMetrics/fastcache, created
// to be self-educational. Use fastcache instead if aiming
// for production use.
//
// Key and value total length must not exceed (64KB - 4B).
type Cache struct {
	rings []ring

	// Config
	size     uint64
	ringSize uint64
	numRings int
}

// New() creates a new cache.
func New(opts ...func(*Cache)) *Cache {
	c := &Cache{
		numRings: 512,
		size:     1,
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.numRings <= 0 {
		c.numRings = 1
	}
	if c.size <= 0 {
		c.size = 1
	}

	numShardsPerRing := (c.size + shardSize*uint64(c.numRings) - 1) / (shardSize * uint64(c.numRings))
	c.ringSize = numShardsPerRing * shardSize
	c.size = uint64(c.numRings) * c.ringSize

	// Init rings
	c.rings = make([]ring, c.numRings)
	for i := range c.numRings {
		c.rings[i].init(c.ringSize)
	}

	return c
}

// New() options

// WithSize() sets max cache size in bytes.
// Entries will be evicted when cache size exceeds max cache size.
// Default cache size is 64KiB * NumRings if not set or lower is set.
func WithSize(size uint64) func(*Cache) {
	return func(c *Cache) {
		c.size = size
	}
}

// WithNumRings() sets number of rings used in cache.
// Defaults to 512.
// Sets to 1 if lower is set.
func WithNumRings(numRings int) func(*Cache) {
	return func(c *Cache) {
		c.numRings = numRings
	}
}

// Methods

// Set() sets k, v into the cache.
// KV pairs with total length exceeding (64KB - 4B) are silently dropped.
//
// k and v are safe to be modified after Set() returns
func (c *Cache) Set(k, v []byte) {
	h := xxh3.Hash(k)
	ringIdx := h % uint64(c.numRings)
	c.rings[ringIdx].set(k, v, h)
}

// Get() copies value for key k into dst[:len(value)].
// A new slice will be allocated if dst is nil or not long enough.
// Returns nil if not found.
func (c *Cache) Get(dst, k []byte) []byte {
	h := xxh3.Hash(k)
	ringIdx := h % uint64(c.numRings)
	dst, _ = c.rings[ringIdx].get(dst, k, h, true)
	return dst
}

// HasGet() copies value for key k into dst[:len(value)] if pair exists.
// A new slice will be allocated if dst is nil or not long enough.
// Returns nil, false if not found.
//
// HasGet() is equal to Get() in performance.
func (c *Cache) HasGet(dst, k []byte) ([]byte, bool) {
	h := xxh3.Hash(k)
	ringIdx := h % uint64(c.numRings)
	return c.rings[ringIdx].get(dst, k, h, true)
}

// Has() checks whether key k exists in cache.
//
// Has() is slightly cheaper than HasGet() and Get().
func (c *Cache) Has(k []byte) bool {
	h := xxh3.Hash(k)
	ringIdx := h % uint64(c.numRings)
	_, ok := c.rings[ringIdx].get(nil, k, h, false)
	return ok
}

// Del() deletes KV pair from cache with key k.
func (c *Cache) Del(k []byte) {
	h := xxh3.Hash(k)
	ringIdx := h % uint64(c.numRings)
	c.rings[ringIdx].del(h)
}

// Reset removes all KV pairs from cache
func (c *Cache) Reset() {
	for i := range c.numRings {
		c.rings[i].reset()
	}
}

// Iterator
type Iterator struct {
	c       *Cache
	ringIdx int
	ri      *ringIter
}

func (c *Cache) Iterator() *Iterator {
	it := &Iterator{
		c:       c,
		ringIdx: -1,
		ri: &ringIter{
			r:     &ring{},
			idxes: []uint64{}, // getNext() will return false on first call
			i:     2,
		},
	}

	return it
}

// GetNext() copies key and value of next KV pair into kDst[:len(key)] and
// vDst[:len(value)] if next pair exists.
// New slices will be allocated if is nil or not long enough.
// Returns nil, nil, false when there are no more pairs.
func (it *Iterator) GetNext(kDst, vDst []byte) ([]byte, []byte, bool) {
	for {
		if k, v, ok := it.ri.getNext(kDst, vDst); ok {
			return k, v, true
		} else {
			it.ringIdx++
			if it.ringIdx >= len(it.c.rings) { // No more rings
				return nil, nil, false
			}

			it.ri = it.c.rings[it.ringIdx].iterator()
		}
	}
}

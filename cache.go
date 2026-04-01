package cache

import "github.com/zeebo/xxh3"

type Cache struct {
	rings []ring

	// Config
	size     uint64
	ringSize uint64
	numRings int
}

func New(opts ...func(*Cache)) *Cache {
	c := &Cache{}

	for _, opt := range opts {
		opt(c)
	}

	if c.numRings <= 0 {
		panic("numRings must be positive")
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

func WithSize(size uint64) func(*Cache) {
	return func(c *Cache) {
		c.size = size
	}
}

func WithNumRings(numRings int) func(*Cache) {
	return func(c *Cache) {
		c.numRings = numRings
	}
}

// Methods

func (c *Cache) Set(k, v []byte) {
	h := xxh3.Hash(k)
	ringIdx := h % uint64(c.numRings)
	c.rings[ringIdx].set(k, v, h)
}

func (c *Cache) Get(dst, k []byte) []byte {
	h := xxh3.Hash(k)
	ringIdx := h % uint64(c.numRings)
	dst, _ = c.rings[ringIdx].get(dst, k, h, true)
	return dst
}

func (c *Cache) HasGet(dst, k []byte) ([]byte, bool) {
	h := xxh3.Hash(k)
	ringIdx := h % uint64(c.numRings)
	return c.rings[ringIdx].get(dst, k, h, true)
}

func (c *Cache) Has(k []byte) bool {
	h := xxh3.Hash(k)
	ringIdx := h % uint64(c.numRings)
	_, ok := c.rings[ringIdx].get(nil, k, h, false)
	return ok
}

func (c *Cache) Del(k []byte) {
	h := xxh3.Hash(k)
	ringIdx := h % uint64(c.numRings)
	c.rings[ringIdx].del(h)
}

func (c *Cache) Reset() {
	for i := range c.numRings {
		c.rings[i].reset()
	}
}

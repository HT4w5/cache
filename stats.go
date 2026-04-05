package cache

type Statistics struct {
	Rx          uint64
	Tx          uint64
	Misses      uint64
	Wraps       uint64
	Collisions  uint64
	Corruptions uint64
	Vacuums     uint64
	Allocated   uint64
}

func (r *ring) loadStatistics(stats *Statistics) {
	stats.Rx += r.rx.Load()
	stats.Tx += r.tx.Load()
	stats.Misses += r.misses.Load()
	stats.Wraps += r.wraps.Load()
	stats.Collisions += r.collisions.Load()
	stats.Corruptions += r.corruptions.Load()
	stats.Vacuums += r.vacuums.Load()
	alloc := r.allocated.Load()
	stats.Allocated += alloc
}

// Statistics() reports cache performance statistics
func (c *Cache) Statistics() Statistics {
	var s Statistics
	for i := range c.numRings {
		c.rings[i].loadStatistics(&s)
	}
	return s
}

package cache

import "sync"

const (
	shardSize = 1 << 16
)

var shardPool = sync.Pool{
	New: func() any {
		return make([]byte, shardSize)
	},
}

type shard struct {
	mu   sync.RWMutex
	data []byte
}

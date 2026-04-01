package cache

import (
	"bytes"
	"sync"
)

const (
	ringIdxBits       = 63
	maxRingSize       = 1 << ringIdxBits
	payloadHeaderSize = 4
)

type ring struct {
	mu     sync.RWMutex
	idxMap map[uint64]uint64
	shards []shard

	// State params
	wrapBit bool
	idx     uint64

	// Config
	size uint64
}

func (r *ring) init(size uint64) {
	if size > maxRingSize {
		panic("ring too big")
	}
	numShards := (size + shardSize - 1) / shardSize // total size is integer multiple of shardSize that is just above provided
	r.mu.Lock()
	r.size = uint64(numShards) * shardSize
	r.shards = make([]shard, numShards)
	r.idxMap = make(map[uint64]uint64)
	r.mu.Unlock()
}

func (r *ring) set(k, v []byte, h uint64) {
	payloadSize := uint64(payloadHeaderSize + len(k) + len(v))
	if payloadSize > shardSize {
		return
	}
	r.mu.Lock()
	startIdx := r.idx
	endIdx := startIdx + payloadSize
	startShardIdx := startIdx / shardSize
	endShardIdx := endIdx / shardSize
	// Check if payload spans across shards,
	// excluding the case where it ends exactly at a shard boundary
	if endShardIdx > startShardIdx && endIdx%shardSize != 0 {
		if endShardIdx >= uint64(len(r.shards)) { // Wrap ring
			startIdx = 0
			startShardIdx = 0
			endIdx = payloadSize
			r.wrapBit = !r.wrapBit
			r.vaccum()
		} else { // Move to next shard
			startIdx = endShardIdx * shardSize
			endIdx = startIdx + payloadSize
			startShardIdx = endShardIdx
		}
	}

	// Write map
	idxMapKey := startIdx
	if r.wrapBit {
		idxMapKey |= 1 << ringIdxBits
	} else {
		idxMapKey &= (1 << ringIdxBits) - 1
	}
	r.idxMap[h] = idxMapKey
	r.idx = endIdx

	// Write payload
	shard := &r.shards[startShardIdx]
	shard.mu.Lock()
	r.mu.Unlock()
	if shard.data == nil {
		shard.data = shardPool.Get().([]byte)[:shardSize:shardSize]
	}
	idx := startIdx % shardSize
	writePayload(shard.data[idx:], k, v)
	shard.mu.Unlock()
}

func writePayload(dst, k, v []byte) {
	dst[0] = byte(uint16(len(k)) >> 8)
	dst[1] = byte(len(k))
	dst[2] = byte(uint16(len(v)) >> 8)
	dst[3] = byte(len(v))
	copy(dst[4:], k)
	copy(dst[4+len(k):], v)
}

func (r *ring) get(dst, k []byte, h uint64, copyValue bool) ([]byte, bool) {
	var wrapBit bool
	r.mu.RLock()
	idx, ok := r.idxMap[h]
	if !ok {
		// No idx mapping
		r.mu.RUnlock()
		return nil, false
	}

	wrapBit = (idx >> ringIdxBits) == 1
	idx &= (1 << ringIdxBits) - 1

	if wrapBit == r.wrapBit && idx < r.idx || wrapBit != r.wrapBit && idx >= r.idx {
		// Valid mapping
		shardIdx := idx / shardSize
		if shardIdx >= uint64(len(r.shards)) {
			// Corrupt idx mapping
			r.mu.RUnlock()
			return nil, false
		}
		idx %= shardSize
		shard := &r.shards[shardIdx]
		shard.mu.RLock()
		r.mu.RUnlock()

		src := shard.data[idx:]

		kLen := (uint64(src[0]) << 8) | uint64(src[1])
		vLen := (uint64(src[2]) << 8) | uint64(src[3])
		if idx+kLen+vLen+4 > shardSize {
			// Corrupt key/value length
			shard.mu.RUnlock()
			return nil, false
		}

		idx += 4
		if !bytes.Equal(k, shard.data[idx:idx+kLen]) {
			// Collision
			shard.mu.RUnlock()
			return nil, false
		}

		idx += kLen
		if copyValue {
			if dst == nil || len(dst) < int(vLen) {
				dst = make([]byte, vLen)
			}
			copy(dst, shard.data[idx:idx+vLen])
			shard.mu.RUnlock()
			return dst[:vLen], true
		} else {
			shard.mu.RUnlock()
			return nil, true
		}
	}

	// Invalid mapping
	r.mu.RUnlock()
	return nil, false
}

func (r *ring) del(h uint64) {
	r.mu.Lock()
	delete(r.idxMap, h)
	r.mu.Unlock()
}

// Caller is responsible for (un)locking the mutex
func (r *ring) vaccum() {
	var valid int
	for _, idx := range r.idxMap {
		wrapBit := (idx >> ringIdxBits) == 1
		idx &= (1 << ringIdxBits) - 1
		if wrapBit == r.wrapBit && idx < r.idx || wrapBit != r.wrapBit && idx >= r.idx {
			// Valid
			valid++
		}
	}

	if valid < len(r.idxMap) {
		// Shrink map by re-creating
		newIdxMap := make(map[uint64]uint64)
		for h, idx := range r.idxMap {
			wrapBit := (idx >> ringIdxBits) == 1
			idx &= (1 << ringIdxBits) - 1
			if wrapBit == r.wrapBit && idx < r.idx || wrapBit != r.wrapBit && idx >= r.idx {
				// Valid
				newIdxMap[h] = idx
			}
		}
		r.idxMap = newIdxMap
	}
}

func (r *ring) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := range len(r.shards) {
		s := &r.shards[i]
		s.mu.Lock()
		if s.data != nil && cap(s.data) != shardSize {
			shardPool.Put(s.data)
		}
		s.data = nil
		s.mu.Unlock()
	}

	clear(r.idxMap)

	r.wrapBit = false
	r.idx = 0
}

// Iterator

func (r *ring) iterator() *ringIter {
	r.mu.RLock()
	defer r.mu.RUnlock()
	it := &ringIter{
		r:     r,
		idxes: make([]uint64, 0, len(r.idxMap)),
		i:     -1,
	}

	for _, idx := range r.idxMap {
		it.idxes = append(it.idxes, idx)
	}

	return it
}

type ringIter struct {
	r     *ring
	idxes []uint64
	i     int64
}

func (it *ringIter) getNext(dst []byte) ([]byte, bool) {
	for {
		it.i++
		if it.i >= int64(len(it.idxes)) {
			return nil, false
		}

		idx := it.idxes[it.i]
		wrapBit := (idx >> ringIdxBits) == 1
		idx &= (1 << ringIdxBits) - 1

		it.r.mu.RLock()
		if wrapBit == it.r.wrapBit && idx < it.r.idx || wrapBit != it.r.wrapBit && idx >= it.r.idx {
			// Valid mapping
			shardIdx := idx / shardSize
			if shardIdx >= uint64(len(it.r.shards)) {
				// Corrupt idx mapping
				it.r.mu.RUnlock()
				continue
			}
			idx %= shardSize
			shard := &it.r.shards[shardIdx]
			shard.mu.RLock()
			it.r.mu.RUnlock()

			src := shard.data[idx:]

			kLen := (uint64(src[0]) << 8) | uint64(src[1])
			vLen := (uint64(src[2]) << 8) | uint64(src[3])
			if idx+kLen+vLen+4 > shardSize {
				// Corrupt key/value length
				shard.mu.RUnlock()
				continue
			}

			idx += 4

			idx += kLen
			if dst == nil || len(dst) < int(vLen) {
				dst = make([]byte, vLen)
			}
			copy(dst, shard.data[idx:idx+vLen])
			shard.mu.RUnlock()
			return dst[:vLen], true
		}
		it.r.mu.RUnlock()
	}
}

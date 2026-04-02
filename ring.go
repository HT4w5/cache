package cache

import (
	"bytes"
	"maps"
	"sync"
)

const (
	ringIdxBits       = 63
	maxRingSize       = 1 << ringIdxBits
	payloadHeaderSize = 4

	vacuumFactor = 2
)

type ring struct {
	mu     sync.RWMutex
	idxMap map[uint64]uint64
	shards [][]byte

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
	r.shards = make([][]byte, numShards)
	r.idxMap = make(map[uint64]uint64)
	r.mu.Unlock()
}

func (r *ring) set(k, v []byte, h uint64) {
	payloadSize := uint64(payloadHeaderSize + len(k) + len(v))
	if payloadSize > shardSize {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	startIdx := r.idx
	endIdx := startIdx + payloadSize
	startShardIdx := startIdx / shardSize
	endShardIdx := endIdx / shardSize
	// Check if payload spans across shards,
	// excluding the case where it ends exactly at a shard boundary
	if endShardIdx > startShardIdx && endIdx%shardSize != 0 {
		if endShardIdx >= uint64(len(r.shards)) { // Wrap ring
			// Perform vacuum
			r.vacuum()
			startIdx = 0
			startShardIdx = 0
			endIdx = payloadSize
			r.wrapBit = !r.wrapBit
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
	srd := r.shards[startShardIdx]

	if srd == nil {
		srd = shardPool.Get().([]byte)[:shardSize:shardSize]
		r.shards[startShardIdx] = srd
	}
	idx := startIdx % shardSize
	writePayload(srd[idx:], k, v)
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
	defer r.mu.RUnlock()
	idx, ok := r.idxMap[h]
	if !ok {
		// No idx mapping
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
		srd := r.shards[shardIdx]

		src := srd[idx:]

		kLen := (uint64(src[0]) << 8) | uint64(src[1])
		vLen := (uint64(src[2]) << 8) | uint64(src[3])
		if idx+kLen+vLen+4 > shardSize {
			// Corrupt key/value length
			return nil, false
		}

		idx += 4
		if !bytes.Equal(k, srd[idx:idx+kLen]) {
			// Collision
			return nil, false
		}

		idx += kLen
		if copyValue {
			dst = dst[:0]
			dst = append(dst, srd[idx:idx+vLen]...)
			return dst, true
		} else {
			return nil, true
		}
	}

	// Invalid mapping
	return nil, false
}

func (r *ring) del(h uint64) {
	r.mu.Lock()
	delete(r.idxMap, h)
	r.mu.Unlock()
}

// Caller is responsible for (un)locking the mutex
func (r *ring) vacuum() {
	cap := len(r.idxMap)
	maps.DeleteFunc(r.idxMap, func(h uint64, idx uint64) bool {
		wrapBit := (idx >> ringIdxBits) == 1
		idx &= (1 << ringIdxBits) - 1

		return !(wrapBit == r.wrapBit && idx < r.idx || wrapBit != r.wrapBit && idx >= r.idx) // Invalid entries
	})

	if len(r.idxMap)*vacuumFactor <= cap {
		// Shrink map by re-creating
		r.idxMap = maps.Clone(r.idxMap)
	}
}

func (r *ring) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := range len(r.shards) {
		s := r.shards[i]
		if s != nil && cap(s) != shardSize {
			shardPool.Put(s)
		}
		s = nil
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

func (it *ringIter) getNext(kDst, vDst []byte) ([]byte, []byte, bool) {
	it.r.mu.RLock()
	defer it.r.mu.RUnlock()

	for {
		it.i++
		if it.i >= int64(len(it.idxes)) {
			return nil, nil, false
		}

		idx := it.idxes[it.i]
		wrapBit := (idx >> ringIdxBits) == 1
		idx &= (1 << ringIdxBits) - 1

		if wrapBit == it.r.wrapBit && idx < it.r.idx || wrapBit != it.r.wrapBit && idx >= it.r.idx {
			// Valid mapping
			shardIdx := idx / shardSize
			if shardIdx >= uint64(len(it.r.shards)) {
				// Corrupt idx mapping
				continue
			}
			idx %= shardSize
			srd := it.r.shards[shardIdx]

			src := srd[idx:]

			kLen := (uint64(src[0]) << 8) | uint64(src[1])
			vLen := (uint64(src[2]) << 8) | uint64(src[3])
			if idx+kLen+vLen+4 > shardSize {
				// Corrupt key/value length
				continue
			}

			idx += 4
			kDst = kDst[:0]
			kDst = append(kDst, srd[idx:idx+kLen]...)

			idx += kLen
			vDst = vDst[:0]
			vDst = append(vDst, srd[idx:idx+vLen]...)
			return kDst, vDst, true
		}
	}
}

package cache

import (
	"bytes"
	"crypto/rand"
	"sync"
	"testing"

	"github.com/zeebo/xxh3"
)

// TestSmallRing tests a ring with only one shard
func TestSmallRing(t *testing.T) {
	r := &ring{}
	r.init(shardSize) // One shard

	// Test basic set/get
	key := []byte("test-key")
	value := []byte("test-value")
	h := xxh3.Hash(key)

	// Set the key/value
	r.set(key, value, h)

	// Try to get it back
	got, ok := r.get(nil, key, h)
	if !ok {
		t.Fatal("Failed to get key that was just set")
	}

	if !bytes.Equal(got, value) {
		t.Fatalf("Got wrong value: expected %q, got %q", value, got)
	}

	// Test with provided buffer
	dst := make([]byte, len(value))
	got, ok = r.get(dst, key, h)
	if !ok {
		t.Fatal("Failed to get key that was just set with buffer")
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("Got wrong value with buffer: expected %q, got %q", value, got)
	}
	if &got[0] != &dst[0] {
		t.Error("get() should return the provided buffer when large enough")
	}

	// Test getting non-existent key
	nonExistentKey := []byte("non-existent")
	h2 := xxh3.Hash(nonExistentKey)
	_, ok = r.get(nil, nonExistentKey, h2)
	if ok {
		t.Fatal("Should not get non-existent key")
	}

	// Test delete
	r.del(h)
	_, ok = r.get(nil, key, h)
	if ok {
		t.Fatal("Should not get deleted key")
	}
}

// Helper function to test setting and getting n items
func testSetGetN(t *testing.T, r *ring, n int) {
	// Create n unique keys and values
	keys := make([][]byte, n)
	values := make([][]byte, n)
	hashes := make([]uint64, n)

	for i := 0; i < n; i++ {
		key := make([]byte, 16)
		value := make([]byte, 32)
		rand.Read(key)
		rand.Read(value)
		keys[i] = key
		values[i] = value
		hashes[i] = xxh3.Hash(key)
	}

	// Set all items
	for i := 0; i < n; i++ {
		r.set(keys[i], values[i], hashes[i])
	}

	// Get all items and verify
	for i := 0; i < n; i++ {
		dst := make([]byte, len(values[i]))
		got, ok := r.get(dst, keys[i], hashes[i])
		if !ok {
			t.Errorf("Failed to get key %d that was just set", i)
			continue
		}
		if !bytes.Equal(got, values[i]) {
			t.Errorf("Wrong value for key %d: expected %x, got %x", i, values[i], got)
		}
	}

	// Can't get non-existent items
	nonExistentKey := make([]byte, 16)
	rand.Read(nonExistentKey)
	h := xxh3.Hash(nonExistentKey)
	dst := make([]byte, 32)
	_, ok := r.get(dst, nonExistentKey, h)
	if ok {
		t.Error("Should not get non-existent key")
	}
}

// TestSerialRing tests the ring in serial mode
func TestSerialRing(t *testing.T) {
	// Test with different ring sizes
	testCases := []struct {
		name     string
		maxBytes uint64
		numItems int
	}{
		{"small ring", shardSize, 10},
		{"medium ring", 4 * shardSize, 100},
		{"large ring", 16 * shardSize, 1000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := &ring{}
			r.init(tc.maxBytes)
			testSetGetN(t, r, tc.numItems)
		})
	}
}

// TestConcurrentRing tests the ring with concurrent access
func TestConcurrentRing(t *testing.T) {
	r := &ring{}
	r.init(8 * shardSize) // 8 shards

	const numWorkers = 10
	const itemsPerWorker = 100
	const totalItems = numWorkers * itemsPerWorker

	// Create all keys and values
	keys := make([][]byte, totalItems)
	values := make([][]byte, totalItems)
	hashes := make([]uint64, totalItems)

	for i := range totalItems {
		key := make([]byte, 16)
		value := make([]byte, 32)
		rand.Read(key)
		rand.Read(value)
		keys[i] = key
		values[i] = value
		hashes[i] = xxh3.Hash(key)
	}

	// Concurrent set
	var wg sync.WaitGroup
	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			start := workerID * itemsPerWorker
			end := start + itemsPerWorker
			for i := start; i < end; i++ {
				r.set(keys[i], values[i], hashes[i])
			}
		}(w)
	}
	wg.Wait()

	// Concurrent get
	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			start := workerID * itemsPerWorker
			end := start + itemsPerWorker
			for i := start; i < end; i++ {
				dst := make([]byte, len(values[i]))
				got, ok := r.get(dst, keys[i], hashes[i])
				if !ok {
					t.Errorf("Worker %d: failed to get key %d", workerID, i)
				}
				if !bytes.Equal(got, values[i]) {
					t.Errorf("Worker %d: wrong value for key %d", workerID, i)
				}
			}
		}(w)
	}
	wg.Wait()

	// Concurrent mixed operations
	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			start := workerID * itemsPerWorker
			end := start + itemsPerWorker
			for i := start; i < end; i++ {
				// Every 3rd operation is a delete
				if i%3 == 0 {
					r.del(hashes[i])
					// Try to get deleted item
					dst := make([]byte, len(values[i]))
					_, ok := r.get(dst, keys[i], hashes[i])
					if ok {
						t.Errorf("Worker %d: should not get deleted key %d", workerID, i)
					}
				} else {
					// Set or get
					if i%2 == 0 {
						r.set(keys[i], values[i], hashes[i])
					} else {
						dst := make([]byte, len(values[i]))
						_, _ = r.get(dst, keys[i], hashes[i])
					}
				}
			}
		}(w)
	}
	wg.Wait()
}

// TestRingWrap tests that the ring properly wraps around when full
func TestRingWrap(t *testing.T) {
	// Create a small ring (2 shards) to force wrap quickly
	r := &ring{}
	r.init(2 * shardSize)

	// Calculate how many items we need to fill the ring
	// Each item: 4-byte header + 16-byte key + 32-byte value = 52 bytes
	itemSize := uint64(payloadHeaderSize + 16 + 32)
	itemsPerShard := shardSize / itemSize
	totalItems := int(itemsPerShard * 3) // Enough to cause wrap

	keys := make([][]byte, totalItems)
	values := make([][]byte, totalItems)
	hashes := make([]uint64, totalItems)

	for i := range totalItems {
		key := make([]byte, 16)
		value := make([]byte, 32)
		rand.Read(key)
		rand.Read(value)
		keys[i] = key
		values[i] = value
		hashes[i] = xxh3.Hash(key)
	}

	// Set all items - this should cause wrap
	for i := range totalItems {
		r.set(keys[i], values[i], hashes[i])
	}

	// After wrap, some early items should be evicted
	// Check at least some items are still retrievable
	foundCount := 0
	for i := range totalItems {
		dst := make([]byte, len(values[i]))
		got, ok := r.get(dst, keys[i], hashes[i])
		if ok {
			foundCount++
			if !bytes.Equal(got, values[i]) {
				t.Errorf("Wrong value for key %d after wrap", i)
			}
		}
	}

	if foundCount == 0 {
		t.Error("No items found after wrap - something is wrong")
	}
	if foundCount == totalItems {
		t.Error("All items found after wrap - ring may not be wrapping properly")
	}

	// Can still add new items after wrap
	newKey := make([]byte, 16)
	newValue := make([]byte, 32)
	rand.Read(newKey)
	rand.Read(newValue)
	newHash := xxh3.Hash(newKey)

	r.set(newKey, newValue, newHash)
	dst := make([]byte, len(newValue))
	got, ok := r.get(dst, newKey, newHash)
	if !ok {
		t.Error("Failed to get new item after wrap")
	}
	if !bytes.Equal(got, newValue) {
		t.Error("Wrong value for new item after wrap")
	}
}

// TestLargestPossibleKeyValue tests the maximum size key/value that can be stored
func TestLargestPossibleKeyValue(t *testing.T) {
	r := &ring{}
	r.init(4 * shardSize)

	maxSize := shardSize - payloadHeaderSize

	// Test with max size key and zero size value
	key := make([]byte, maxSize)
	value := []byte{}
	rand.Read(key)
	h := xxh3.Hash(key)

	r.set(key, value, h)
	dst := make([]byte, len(value))
	got, ok := r.get(dst, key, h)
	if !ok {
		t.Error("Failed to get max size key with empty value")
	}
	if !bytes.Equal(got, value) {
		t.Error("Wrong value for max size key with empty value")
	}

	// Test with zero size key and max size value
	key2 := []byte{}
	value2 := make([]byte, maxSize)
	rand.Read(value2)
	h2 := xxh3.Hash(key2)

	r.set(key2, value2, h2)
	dst2 := make([]byte, len(value2))
	got2, ok := r.get(dst2, key2, h2)
	if !ok {
		t.Error("Failed to get empty key with max size value")
	}
	if !bytes.Equal(got2, value2) {
		t.Error("Wrong value for max size value")
	}

	// Test with split sizes
	keySize := maxSize / 3
	valueSize := maxSize - keySize - payloadHeaderSize
	key3 := make([]byte, keySize)
	value3 := make([]byte, valueSize)
	rand.Read(key3)
	rand.Read(value3)
	h3 := xxh3.Hash(key3)

	r.set(key3, value3, h3)
	dst3 := make([]byte, len(value3))
	got3, ok := r.get(dst3, key3, h3)
	if !ok {
		t.Error("Failed to get large key/value pair")
	}
	if !bytes.Equal(got3, value3) {
		t.Error("Wrong value for large key/value pair")
	}

	// Test that slightly too large payload is rejected
	tooLargeKey := make([]byte, maxSize+1)
	tooLargeValue := []byte{}
	rand.Read(tooLargeKey)
	h4 := xxh3.Hash(tooLargeKey)

	r.set(tooLargeKey, tooLargeValue, h4)

	// Should not be able to retrieve
	dst4 := make([]byte, len(tooLargeValue))
	_, ok = r.get(dst4, tooLargeKey, h4)
	if ok {
		t.Error("Should not get too large key")
	}
}

// TestRingIterator tests the ring iterator
func TestRingIterator(t *testing.T) {
	r := &ring{}
	r.init(4 * shardSize)

	// Create test data
	const numItems = 100
	keys := make([][]byte, numItems)
	values := make([][]byte, numItems)
	hashes := make([]uint64, numItems)

	for i := range numItems {
		key := make([]byte, 16)
		value := make([]byte, 32)
		rand.Read(key)
		rand.Read(value)
		keys[i] = key
		values[i] = value
		hashes[i] = xxh3.Hash(key)
	}

	for i := range numItems {
		r.set(keys[i], values[i], hashes[i])
	}

	// Test iterator
	it := r.iterator()
	visited := make(map[string]bool)
	count := 0

	for {
		got, ok := it.getNext(nil)
		if !ok {
			break
		}
		count++

		found := false
		for i, value := range values {
			if bytes.Equal(got, value) {
				keyStr := string(keys[i])
				if visited[keyStr] {
					t.Errorf("Iterator returned duplicate value for key %d", i)
				}
				visited[keyStr] = true
				found = true
				break
			}
		}
		if !found {
			t.Error("Iterator returned value not in original set")
		}
	}

	if count != numItems {
		t.Errorf("Iterator returned %d items, expected %d", count, numItems)
	}

	// Test iterator with buffer
	it2 := r.iterator()
	visited2 := make(map[string]bool)
	count2 := 0

	for {
		buf := make([]byte, 32)
		got, ok := it2.getNext(buf)
		if !ok {
			break
		}
		count2++

		if &got[0] != &buf[0] {
			t.Error("Iterator should return provided buffer when large enough")
		}

		found := false
		for i, value := range values {
			if bytes.Equal(got, value) {
				keyStr := string(keys[i])
				if visited2[keyStr] {
					t.Errorf("Iterator returned duplicate value for key %d", i)
				}
				visited2[keyStr] = true
				found = true
				break
			}
		}
		if !found {
			t.Error("Iterator returned value not in original set")
		}
	}

	if count2 != numItems {
		t.Errorf("Iterator with buffer returned %d items, expected %d", count2, numItems)
	}

	// Test iterator on empty ring
	r2 := &ring{}
	r2.init(shardSize)
	it3 := r2.iterator()
	if _, ok := it3.getNext(nil); ok {
		t.Error("Iterator should return false on empty ring")
	}

	// Test iterator after some deletions
	r3 := &ring{}
	r3.init(4 * shardSize)

	for range 10 {
		key := make([]byte, 16)
		value := make([]byte, 32)
		rand.Read(key)
		rand.Read(value)
		r3.set(key, value, xxh3.Hash(key))
	}

	it4 := r3.iterator()
	it4Count := 0
	for {
		_, ok := it4.getNext(nil)
		if !ok {
			break
		}
		it4Count++
	}

	if it4Count == 0 {
		t.Error("Iterator should return items even after some operations")
	}
}

func TestReset(t *testing.T) {
	r := &ring{}
	r.init(4 * shardSize)

	// Create test data
	const numItems = 100
	keys := make([][]byte, numItems)
	values := make([][]byte, numItems)
	hashes := make([]uint64, numItems)

	for i := range numItems {
		key := make([]byte, 16)
		value := make([]byte, 32)
		rand.Read(key)
		rand.Read(value)
		keys[i] = key
		values[i] = value
		hashes[i] = xxh3.Hash(key)
	}

	for i := range numItems {
		r.set(keys[i], values[i], hashes[i])
	}

	// Perform reset
	r.reset()

	it := r.iterator()
	if _, ok := it.getNext(nil); ok {
		t.Error("ring should be reset")
	}
}

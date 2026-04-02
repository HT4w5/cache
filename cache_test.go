package cache

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// These tests are modified from
// https://github.com/VictoriaMetrics/fastcache/blob/v1.13.3/fastcache_test.go,
// licensed under The MIT License
//
// The MIT License (MIT)
//
// Copyright (c) 2018 VictoriaMetrics
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// Modified by HT4w5 in 2026.

func TestCacheSmall(t *testing.T) {
	c := New(WithSize(1), WithNumRings(1))
	defer c.Reset()

	if v := c.Get(nil, []byte("aaa")); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}

	c.Set([]byte("key"), []byte("value"))
	if v := c.Get(nil, []byte("key")); string(v) != "value" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "value")
	}
	if v := c.Get(nil, nil); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}
	if v, exist := c.HasGet(nil, nil); exist {
		t.Fatalf("unexpected nil-keyed value obtained in small cache: %q", v)
	}
	if v := c.Get(nil, []byte("aaa")); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}

	c.Set([]byte("aaa"), []byte("bbb"))
	if v := c.Get(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}

	c.Reset()
	if v := c.Get(nil, []byte("aaa")); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from empty cache: %q", v)
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}

	// Test empty value
	k := []byte("empty")
	c.Set(k, nil)
	if v := c.Get(nil, k); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from empty entry: %q", v)
	}
	if v, exist := c.HasGet(nil, k); !exist {
		t.Fatalf("cannot find empty entry for key %q", k)
	} else if len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from empty entry: %q", v)
	}
	if !c.Has(k) {
		t.Fatalf("cannot find empty entry for key %q", k)
	}
	if c.Has([]byte("foobar")) {
		t.Fatalf("non-existing entry found in the cache")
	}
}

func TestCacheWrap(t *testing.T) {
	c := New(WithSize(shardSize*1.5), WithNumRings(1))
	defer c.Reset()

	calls := uint64(5e6)

	for i := range calls {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
		vv := c.Get(nil, k)
		if string(vv) != string(v) {
			t.Fatalf("unexpected value for key %q; got %q; want %q", k, vv, v)
		}
	}
	for i := range calls / 10 {
		x := i * 10
		k := []byte(fmt.Sprintf("key %d", x))
		v := []byte(fmt.Sprintf("value %d", x))
		vv := c.Get(nil, k)
		if len(vv) > 0 && string(v) != string(vv) {
			t.Fatalf("unexpected value for key %q; got %q; want %q", k, vv, v)
		}
	}
}

func TestCacheDel(t *testing.T) {
	c := New(WithSize(1024), WithNumRings(16))
	defer c.Reset()

	for i := range 100 {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
		vv := c.Get(nil, k)
		if string(vv) != string(v) {
			t.Fatalf("unexpected value for key %q; got %q; want %q", k, vv, v)
		}
		c.Del(k)
		vv = c.Get(nil, k)
		if len(vv) > 0 {
			t.Fatalf("unexpected non-empty value got for key %q: %q", k, vv)
		}
	}
}

func TestCacheBigKeyValue(t *testing.T) {
	c := New(WithSize(1024), WithNumRings(16))
	defer c.Reset()

	// Both key and value exceed 64Kb
	k := make([]byte, 90*1024)
	v := make([]byte, 100*1024)
	c.Set(k, v)
	vv := c.Get(nil, k)
	if len(vv) > 0 {
		t.Fatalf("unexpected non-empty value got for key %q: %q", k, vv)
	}

	// len(key) + len(value) > 64Kb
	k = make([]byte, 40*1024)
	v = make([]byte, 40*1024)
	c.Set(k, v)
	vv = c.Get(nil, k)
	if len(vv) > 0 {
		t.Fatalf("unexpected non-empty value got for key %q: %q", k, vv)
	}
}

func TestCacheSetGetSerial(t *testing.T) {
	itemsCount := 10000
	c := New(WithSize(uint64(30*itemsCount)), WithNumRings(16))
	defer c.Reset()
	if err := testCacheGetSet(c, itemsCount); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestCacheGetSetConcurrent(t *testing.T) {
	itemsCount := 10000
	const gorotines = 10
	c := New(WithSize(uint64(30*itemsCount*gorotines)), WithNumRings(16))
	defer c.Reset()

	ch := make(chan error, gorotines)
	for range gorotines {
		go func() {
			ch <- testCacheGetSet(c, itemsCount)
		}()
	}
	for range gorotines {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout")
		}
	}
}

func testCacheGetSet(c *Cache, itemsCount int) error {
	for i := range itemsCount {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
		vv := c.Get(nil, k)
		if string(vv) != string(v) {
			return fmt.Errorf("unexpected value for key %q after insertion; got %q; want %q", k, vv, v)
		}
	}
	misses := 0
	for i := range itemsCount {
		k := []byte(fmt.Sprintf("key %d", i))
		vExpected := fmt.Sprintf("value %d", i)
		v := c.Get(nil, k)
		if string(v) != string(vExpected) {
			if len(v) > 0 {
				return fmt.Errorf("unexpected value for key %q after all insertions; got %q; want %q", k, v, vExpected)
			}
			misses++
		}
	}
	if misses >= itemsCount/100 {
		return fmt.Errorf("too many cache misses; got %d; want less than %d", misses, itemsCount/100)
	}
	return nil
}

func TestCacheResetUpdateStatsSetConcurrent(t *testing.T) {
	c := New(WithSize(12334), WithNumRings(16))
	defer c.Reset()

	stopCh := make(chan struct{})

	// run workers for cache reset
	var resettersWG sync.WaitGroup
	for range 10 {
		resettersWG.Add(1)
		go func() {
			defer resettersWG.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					c.Reset()
					runtime.Gosched()
				}
			}
		}()
	}

	// run workers for setting data to cache
	var settersWG sync.WaitGroup
	for range 10 {
		settersWG.Add(1)
		go func() {
			defer settersWG.Done()
			for j := range 100 {
				key := []byte(fmt.Sprintf("key_%d", j))
				value := []byte(fmt.Sprintf("value_%d", j))
				c.Set(key, value)
				runtime.Gosched()
			}
		}()
	}

	// wait for setters
	settersWG.Wait()
	close(stopCh)
	resettersWG.Wait()
}

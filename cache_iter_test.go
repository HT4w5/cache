package cache

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"path"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

// These tests are modified from
// https://github.com/allegro/bigcache/blob/532eb6410aefb749509084c74f56b8313e200f4a/iterator_test.go,
// licensed under The Apache License Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0
//
// Modified by HT4w5 in 2026

func TestIterateOnEmptyCache(t *testing.T) {
	c := New()

	it := c.Iterator()

	if _, _, ok := it.GetNext(nil, nil); ok {
		t.Error("GetNext should return false on empty cache")
	}
}

func TestEntriesIterator(t *testing.T) {
	// given
	keysCount := 1000
	cache := New()
	value := []byte("value")

	for i := range keysCount {
		cache.Set([]byte(fmt.Sprintf("key%d", i)), value)
	}

	// when
	keys := make(map[string]struct{})
	iterator := cache.Iterator()

	for {
		k, _, ok := iterator.GetNext(nil, nil)
		if !ok {
			break
		}
		keys[string(k)] = struct{}{}
	}

	// then
	assertEqual(t, keysCount, len(keys))
}

func TestEntriesIteratorWithMostRingsEmpty(t *testing.T) {
	t.Parallel()

	// given
	cache := New()

	cache.Set([]byte("key"), []byte("value"))

	// when
	iterator := cache.Iterator()

	// then
	if k, v, ok := iterator.GetNext(nil, nil); !ok {
		t.Errorf("Iterator should contain at least single element")
	} else {
		// then
		assertEqual(t, "key", string(k))
		assertEqual(t, []byte("value"), v)
	}
}

func TestEntriesIteratorParallelAdd(t *testing.T) {
	bc := New()

	wg := sync.WaitGroup{}
	wg.Go(func() {
		for i := range 10000 {
			bc.Set([]byte(strconv.Itoa(i)), []byte("aaaaaaa"))
			runtime.Gosched()
		}
	})

	for range 100 {
		iter := bc.Iterator()
		for {
			_, _, ok := iter.GetNext(nil, nil)
			if !ok {
				break
			}
		}
	}
	wg.Wait()
}

func TestParallelSetAndIteration(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(0))

	cache := New()

	entrySize := 1024 * 100
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	wg := sync.WaitGroup{}

	wg.Go(func() {
		defer func() {
			err := recover()
			// no panic
			assertEqual(t, err, nil)
		}()

		isTimeout := false

		for !isTimeout {

			select {
			case <-ctx.Done():
				isTimeout = true
			default:
				cache.Set([]byte(strconv.Itoa(rnd.Intn(100))), blob('a', entrySize))
			}
		}
	})

	go func() {
		defer func() {
			err := recover()
			// no panic
			assertEqual(t, nil, err)
		}()

		isTimeout := false

		for !isTimeout {

			select {
			case <-ctx.Done():
				isTimeout = true
			default:
				iter := cache.Iterator()
				for {
					_, v, ok := iter.GetNext(nil, nil)
					if !ok {
						break
					}
					assertEqual(t, entrySize, len(v))
				}
			}
		}
	}()

	wg.Wait()
}

// Helpers

func assertEqual(t *testing.T, expected, actual interface{}, msgAndArgs ...interface{}) {
	if !objectsAreEqual(expected, actual) {
		_, file, line, _ := runtime.Caller(1)
		file = path.Base(file)
		t.Errorf(fmt.Sprintf("\n%s:%d: Not equal: \n"+
			"expected: %T(%#v)\n"+
			"actual  : %T(%#v)\n",
			file, line, expected, expected, actual, actual), msgAndArgs...)
	}
}

func objectsAreEqual(expected, actual interface{}) bool {
	if expected == nil || actual == nil {
		return expected == actual
	}

	exp, ok := expected.([]byte)
	if !ok {
		return reflect.DeepEqual(expected, actual)
	}

	act, ok := actual.([]byte)
	if !ok {
		return false
	}
	if exp == nil || act == nil {
		return exp == nil && act == nil
	}
	return bytes.Equal(exp, act)
}

func blob(char byte, len int) []byte {
	return bytes.Repeat([]byte{char}, len)
}

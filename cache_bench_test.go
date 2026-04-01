package cache

import (
	"fmt"
	"testing"
)

var numRings = []int{512}

func BenchmarkCacheSet(b *testing.B) {
	for _, n := range numRings {
		b.Run(fmt.Sprintf("NumRings-%d", n), func(b *testing.B) {
			benchmarkCacheSet(b, n)
		})
	}
}

func BenchmarkCacheGet(b *testing.B) {
	for _, n := range numRings {
		b.Run(fmt.Sprintf("NumRings-%d", n), func(b *testing.B) {
			benchmarkCacheGet(b, n)
		})
	}
}

func BenchmarkCacheHas(b *testing.B) {
	for _, n := range numRings {
		b.Run(fmt.Sprintf("NumRings-%d", n), func(b *testing.B) {
			benchmarkCacheHas(b, n)
		})
	}
}

func BenchmarkCacheSetGet(b *testing.B) {
	for _, n := range numRings {
		b.Run(fmt.Sprintf("NumRings-%d", n), func(b *testing.B) {
			benchmarkCacheSetGet(b, n)
		})
	}
}

func benchmarkCacheSet(b *testing.B, numRings int) {
	const items = 1 << 16
	c := New(WithSize(12*1024*items), WithNumRings(numRings))
	defer c.Reset()
	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		for pb.Next() {
			for range items {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				c.Set(k, v)
			}
		}
	})
}

func benchmarkCacheGet(b *testing.B, numRings int) {
	const items = 1 << 16
	c := New(WithSize(12*1024*items), WithNumRings(numRings))
	defer c.Reset()
	k := []byte("\x00\x00\x00\x00")
	v := []byte("xyza")
	for range items {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		c.Set(k, v)
	}

	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		var buf []byte
		k := []byte("\x00\x00\x00\x00")
		for pb.Next() {
			for range items {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				buf = c.Get(buf[:0], k)
				if string(buf) != string(v) {
					panic(fmt.Errorf("BUG: invalid value obtained; got %q; want %q", buf, v))
				}
			}
		}
	})
}

func benchmarkCacheHas(b *testing.B, numRings int) {
	const items = 1 << 16
	c := New(WithSize(12*1024*items), WithNumRings(numRings))
	defer c.Reset()
	k := []byte("\x00\x00\x00\x00")
	for range items {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		c.Set(k, nil)
	}

	b.ReportAllocs()
	b.SetBytes(items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		for pb.Next() {
			for range items {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				if !c.Has(k) {
					panic(fmt.Errorf("BUG: missing value for key %q", k))
				}
			}
		}
	})
}

func benchmarkCacheSetGet(b *testing.B, numRings int) {
	const items = 1 << 16
	c := New(WithSize(12*1024*items), WithNumRings(numRings))
	defer c.Reset()
	b.ReportAllocs()
	b.SetBytes(2 * items)
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		var buf []byte
		for pb.Next() {
			for range items {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				c.Set(k, v)
			}
			for range items {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				buf, _ = c.HasGet(nil, k)
				if string(buf) != string(v) {
					b.Fatalf("BUG: invalid value obtained; got %q; want %q", buf, v)
				}
			}
		}
	})
}

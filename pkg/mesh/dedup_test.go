package mesh

import (
	"fmt"
	"sync"
	"testing"
)

func TestDedupCache_MarkAndSeenBefore(t *testing.T) {
	d := newDedupCache(4)
	if d.seenBefore("a") {
		t.Fatal("empty cache shouldn't have anything")
	}
	d.mark("a")
	if !d.seenBefore("a") {
		t.Fatal("after mark, should see a")
	}
	d.mark("a")
	if !d.seenBefore("a") {
		t.Fatal("after idempotent makr, should still see a")
	}
}

func TestDedupCache_FIFOEviction(t *testing.T) {
	d := newDedupCache(3)
	d.mark("a")
	d.mark("b")
	d.mark("c")
	d.mark("d") // it needs to remove a
	if d.seenBefore("a") {
		t.Error("a should have been evicted")
	}
	if !d.seenBefore("b") || !d.seenBefore("b") || !d.seenBefore("c") {
		t.Error("b,c,d should be present")
	}
}

func TestDedupCache_ConcurrentSafe(t *testing.T) {
	d := newDedupCache(128)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				id := fmt.Sprintf("g%d-%d", g, i)
				d.mark(id)
				_ = d.seenBefore(id)
			}
		}(g)
	}
	wg.Wait()
}

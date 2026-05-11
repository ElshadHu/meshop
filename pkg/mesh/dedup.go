package mesh

import "sync"

// dedupCache is a fixed-size FIFO of envelope IDs
type dedupCache struct {
	mu   sync.Mutex
	seen map[string]struct{}
	ring []string
	idx  int
	size int
}

func newDedupCache(size int) *dedupCache {
	if size <= 0 {
		size = 1024
	}
	return &dedupCache{
		seen: make(map[string]struct{}, size),
		ring: make([]string, size),
		size: size,
	}
}

func (d *dedupCache) seenBefore(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen[id]
	return ok
}
func (d *dedupCache) mark(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[id]; ok {
		return
	}
	if old := d.ring[d.idx]; old != "" {
		delete(d.seen, old)
	}
	d.ring[d.idx] = id
	d.seen[id] = struct{}{}
	d.idx = (d.idx + 1) % d.size
}

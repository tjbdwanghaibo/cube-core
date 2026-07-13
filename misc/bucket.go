package misc

import "sync"

type BucketHolder[K Integer, T any] struct {
	RangeCursor uint
	Buckets     []*Bucket[K, T]
	BucketCnt   uint64
	DataBuilder func(K) T
}

func NewBucketHolder[K Integer, T any](bucketCnt int, db func(K) T, buildIF bool) *BucketHolder[K, T] {
	ret := &BucketHolder[K, T]{
		BucketCnt:   uint64(bucketCnt),
		Buckets:     make([]*Bucket[K, T], bucketCnt),
		DataBuilder: db,
	}
	for i := 0; i < int(ret.BucketCnt); i++ {
		ret.Buckets[i] = NewBucket(db, buildIF)
	}
	return ret
}

func (h *BucketHolder[K, T]) Get(k K) T {
	return h.Buckets[Hash64(uint64(k))%h.BucketCnt].Get(k)
}

func (h *BucketHolder[K, T]) Add(k K, t T) {
	h.Buckets[Hash64(uint64(k))%h.BucketCnt].Add(k, t)
}

func (h *BucketHolder[K, T]) Del(k K) {
	h.Buckets[Hash64(uint64(k))%h.BucketCnt].Del(k)
}

func (h *BucketHolder[K, T]) RangeWithCursor(f func(K, T) bool) {
	h.Buckets[h.RangeCursor%uint(h.BucketCnt)].Range(f)
	h.RangeCursor++
}

func (h *BucketHolder[K, T]) RangeWithCursorCnt(cursorCnt uint, f func(K, T) bool) {
	for range cursorCnt {
		h.Buckets[h.RangeCursor%uint(h.BucketCnt)].Range(f)
		h.RangeCursor++
	}
}

func (h *BucketHolder[K, T]) RangeByCursor(cursor uint, f func(K, T) bool) {
	h.Buckets[cursor%uint(h.BucketCnt)].Range(f)
}

func (h *BucketHolder[K, T]) RangeAll(f func(K, T) bool) {
	for _, bucket := range h.Buckets {
		bucket.Range(f)
	}
}

func (h *BucketHolder[K, T]) Count() int {
	var cnt int
	for _, bucket := range h.Buckets {
		cnt += bucket.Count()
	}
	return cnt
}

type Bucket[K Integer, T any] struct {
	sync.RWMutex
	dataMap     map[K]T
	dataBuilder func(K) T
	buildIF     bool
}

func NewBucket[K Integer, T any](db func(K) T, buildIF bool) *Bucket[K, T] {
	return &Bucket[K, T]{
		dataMap:     make(map[K]T),
		dataBuilder: db,
		buildIF:     buildIF,
	}
}

func (b *Bucket[K, T]) Get(k K) T {
	b.RLock()
	data, exist := b.dataMap[k]
	if exist {
		b.RUnlock()
		return data
	}
	b.RUnlock()
	b.Lock()
	defer b.Unlock()
	data, exist = b.dataMap[k]
	if exist {
		return data
	}
	if b.buildIF {
		data = b.dataBuilder(k)
		b.dataMap[k] = data
	}
	return data
}

func (b *Bucket[K, T]) Del(k K) {
	b.Lock()
	defer b.Unlock()
	delete(b.dataMap, k)
}

func (b *Bucket[K, T]) Add(k K, t T) {
	b.Lock()
	defer b.Unlock()
	b.dataMap[k] = t
}

func (b *Bucket[K, T]) Clear() {
	b.Lock()
	defer b.Unlock()
	clear(b.dataMap)
}

func (b *Bucket[K, T]) Range(f func(K, T) bool) {
	b.RLock()
	defer b.RUnlock()
	for k, v := range b.dataMap {
		if !f(k, v) {
			break
		}
	}
}

func (b *Bucket[K, T]) Count() int {
	b.RLock()
	defer b.RUnlock()
	return len(b.dataMap)
}

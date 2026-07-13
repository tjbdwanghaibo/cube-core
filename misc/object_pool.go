package misc

import "sync"

// ObjectPool is a function-scoped object pool using local worklist and freelist.
type ObjectPool[T any] struct {
	pool      sync.Pool
	newFunc   func() T
	resetFunc func(T) T

	workList []T
	freeList []T
}

func NewObjectPool[T any](newFunc func() T, resetFunc func(T) T) *ObjectPool[T] {
	if newFunc == nil {
		panic("newFunc cannot be nil")
	}
	return &ObjectPool[T]{
		pool: sync.Pool{
			New: func() interface{} {
				return newFunc()
			},
		},
		newFunc:   newFunc,
		resetFunc: resetFunc,
		workList:  make([]T, 0, 8),
		freeList:  make([]T, 0, 8),
	}
}

func (p *ObjectPool[T]) Get() T {
	var obj T
	if len(p.freeList) > 0 {
		obj = p.freeList[len(p.freeList)-1]
		p.freeList = p.freeList[:len(p.freeList)-1]
	} else {
		obj = p.pool.Get().(T)
		if p.resetFunc != nil {
			obj = p.resetFunc(obj)
		}
	}
	p.workList = append(p.workList, obj)
	return obj
}

func (p *ObjectPool[T]) Put(obj T) {
	for i, v := range p.workList {
		if any(v) == any(obj) {
			p.workList = append(p.workList[:i], p.workList[i+1:]...)
			p.freeList = append(p.freeList, obj)
			return
		}
	}
	p.freeList = append(p.freeList, obj)
}

func (p *ObjectPool[T]) Release() {
	for _, obj := range p.workList {
		p.pool.Put(obj)
	}
	p.workList = p.workList[:0]
	for _, obj := range p.freeList {
		p.pool.Put(obj)
	}
	p.freeList = p.freeList[:0]
}

func (p *ObjectPool[T]) Clear() {
	p.workList = p.workList[:0]
	p.freeList = p.freeList[:0]
}

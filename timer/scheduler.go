package timer

import (
	"container/heap"
	"time"
)

const TypeClosure int32 = 0

type ChangeType uint8

const (
	ChangeUpsert ChangeType = iota + 1
	ChangeDelete
)

type Node struct {
	ID      int64
	Type    int32
	Param1  int64
	Param2  int64
	Payload []byte
	End     time.Time
	Delay   time.Duration

	index   int
	handler Handler
}

type Context struct {
	OwnerID int64
	Node    Node
	Now     time.Time
}

type Handler func(ctx Context) time.Duration
type ChangeFunc func(change ChangeType, node Node)

type Scheduler struct {
	ownerID int64
	seed    int64
	nodes   timerHeap
	byID    map[int64]*Node

	handlers map[int32]Handler
	onChange ChangeFunc

	running  bool
	deferred []func()
}

func NewScheduler(ownerID int64, seed int64, saved []Node, onChange ChangeFunc) *Scheduler {
	s := &Scheduler{
		ownerID:  ownerID,
		seed:     seed,
		byID:     make(map[int64]*Node, len(saved)),
		handlers: make(map[int32]Handler),
		onChange: onChange,
	}
	for i := range saved {
		node := saved[i].clone()
		if node.ID <= 0 || node.Type == TypeClosure || node.End.IsZero() {
			continue
		}
		if node.ID > s.seed {
			s.seed = node.ID
		}
		s.push(&node)
	}
	return s
}

func (s *Scheduler) OwnerID() int64 {
	if s == nil {
		return 0
	}
	return s.ownerID
}

func (s *Scheduler) Seed() int64 {
	if s == nil {
		return 0
	}
	return s.seed
}

func (s *Scheduler) RegisterHandler(timerType int32, h Handler) {
	if s == nil || timerType == TypeClosure || h == nil {
		return
	}
	s.handlers[timerType] = h
}

func (s *Scheduler) NewTimer(delay time.Duration, timerType int32, param1 int64, param2 int64, payload []byte) int64 {
	if s == nil || delay <= 0 || timerType == TypeClosure {
		return 0
	}
	return s.add(delay, timerType, param1, param2, payload, nil)
}

func (s *Scheduler) NewClosureTimer(delay time.Duration, h Handler) int64 {
	if s == nil || delay <= 0 || h == nil {
		return 0
	}
	return s.add(delay, TypeClosure, 0, 0, nil, h)
}

func (s *Scheduler) RemoveTimer(id int64) bool {
	if s == nil || id <= 0 {
		return false
	}
	if s.running {
		s.deferred = append(s.deferred, func() { s.RemoveTimer(id) })
		return true
	}
	node := s.byID[id]
	if node == nil {
		return false
	}
	heap.Remove(&s.nodes, node.index)
	delete(s.byID, id)
	if node.Type != TypeClosure {
		s.emit(ChangeDelete, *node)
	}
	return true
}

func (s *Scheduler) ChangeTimer(id int64, now time.Time, delay time.Duration) bool {
	if s == nil || id <= 0 || delay <= 0 {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if s.running {
		s.deferred = append(s.deferred, func() { s.ChangeTimer(id, now, delay) })
		return true
	}
	node := s.byID[id]
	if node == nil {
		return false
	}
	node.Delay = delay
	node.End = now.Add(delay)
	heap.Fix(&s.nodes, node.index)
	if node.Type != TypeClosure {
		s.emit(ChangeUpsert, *node)
	}
	return true
}

func (s *Scheduler) Tick(now time.Time) {
	if s == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	s.running = true
	defer func() {
		s.running = false
		for _, op := range s.deferred {
			op()
		}
		s.deferred = s.deferred[:0]
	}()

	for {
		node := s.min()
		if node == nil || now.Before(node.End) {
			return
		}
		heap.Pop(&s.nodes)
		delete(s.byID, node.ID)
		if node.Type != TypeClosure {
			s.emit(ChangeDelete, *node)
		}

		next := time.Duration(0)
		if node.Type == TypeClosure {
			if node.handler != nil {
				next = node.handler(Context{OwnerID: s.ownerID, Node: node.clone(), Now: now})
			}
		} else if h := s.handlers[node.Type]; h != nil {
			next = h(Context{OwnerID: s.ownerID, Node: node.clone(), Now: now})
		}
		if next <= 0 {
			continue
		}
		node.Delay = next
		node.End = now.Add(next)
		s.push(node)
		if node.Type != TypeClosure {
			s.emit(ChangeUpsert, *node)
		}
	}
}

func (s *Scheduler) NextTime() time.Time {
	if s == nil {
		return time.Time{}
	}
	if node := s.min(); node != nil {
		return node.End
	}
	return time.Time{}
}

func (s *Scheduler) Nodes() []Node {
	if s == nil {
		return nil
	}
	out := make([]Node, 0, len(s.nodes))
	for _, node := range s.nodes {
		if node.Type == TypeClosure {
			continue
		}
		out = append(out, node.clone())
	}
	return out
}

func (s *Scheduler) add(delay time.Duration, timerType int32, param1 int64, param2 int64, payload []byte, h Handler) int64 {
	s.seed++
	id := s.seed
	if s.running {
		s.deferred = append(s.deferred, func() {
			s.addNode(id, delay, timerType, param1, param2, payload, h)
		})
		return id
	}
	s.addNode(id, delay, timerType, param1, param2, payload, h)
	return id
}

func (s *Scheduler) addNode(id int64, delay time.Duration, timerType int32, param1 int64, param2 int64, payload []byte, h Handler) {
	node := &Node{
		ID:      id,
		Type:    timerType,
		Param1:  param1,
		Param2:  param2,
		Payload: append([]byte(nil), payload...),
		End:     time.Now().Add(delay),
		Delay:   delay,
		handler: h,
	}
	s.push(node)
	if timerType != TypeClosure {
		s.emit(ChangeUpsert, *node)
	}
}

func (s *Scheduler) push(node *Node) {
	if node == nil {
		return
	}
	heap.Push(&s.nodes, node)
	s.byID[node.ID] = node
}

func (s *Scheduler) min() *Node {
	if s == nil || len(s.nodes) == 0 {
		return nil
	}
	return s.nodes[0]
}

func (s *Scheduler) emit(change ChangeType, node Node) {
	if s != nil && s.onChange != nil {
		s.onChange(change, node.clone())
	}
}

func (n Node) clone() Node {
	n.Payload = append([]byte(nil), n.Payload...)
	n.index = -1
	return n
}

type timerHeap []*Node

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	return h[i].End.Before(h[j].End)
}
func (h timerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *timerHeap) Push(x any) {
	node := x.(*Node)
	node.index = len(*h)
	*h = append(*h, node)
}
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	node := old[n-1]
	node.index = -1
	old[n-1] = nil
	*h = old[:n-1]
	return node
}

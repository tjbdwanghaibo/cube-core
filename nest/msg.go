package nest

import (
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"slices"
	"strconv"
	"sync"
)

type MsgType uint8

const (
	MsgTypeSingle MsgType = iota
	MsgTypeMulti
	MsgTypeMultiGroup
	MsgTypeBroadcast
	MsgTypeGroupTransition
)

func (t MsgType) String() string {
	switch t {
	case MsgTypeSingle:
		return "Single"
	case MsgTypeMulti:
		return "Multi"
	case MsgTypeMultiGroup:
		return "MultiGroup"
	case MsgTypeBroadcast:
		return "Broadcast"
	case MsgTypeGroupTransition:
		return "GroupTransition"
	default:
		return "Unknown"
	}
}

// Msg is the internal message routed through the nest worker pool.
type Msg struct {
	RetChan         chan any
	Cb1             func(es []any, params []any) (any, error)
	RemoteRelease   func() // called after dispatch to release remote entity distributed locks
	Name            string
	Tids            []int64
	GroupTIds       [][]int64
	Params          []any
	Tid             int64
	RefCount        int
	PendingRequeues int
	Type            MsgType
	Cost            bool
	HasRemote       bool // message involves remote entities
	Context         fctx.ContextSnapshot
	GroupTransition *GroupTransitionRequest
}

func (m *Msg) Key() int64 {
	if m.Tid != 0 {
		return m.Tid
	} else if len(m.Tids) > 0 {
		return m.Tids[0]
	} else if len(m.GroupTIds) > 0 && len(m.GroupTIds[0]) > 0 {
		return m.GroupTIds[0][0]
	}
	return 0
}

func (m *Msg) TraceActive() bool {
	return m != nil && m.Context.Trace.Active()
}

func (m *Msg) clean() {
	*m = Msg{}
}

func (m *Msg) OnSend() {
	m.RefCount++
}

func (m *Msg) OnRelease() {
	m.RefCount--
	if m.RefCount == 0 {
		recycleMsg(m)
	}
}

func (m *Msg) Clone() *Msg {
	ret := &Msg{
		Tid:             m.Tid,
		Type:            m.Type,
		Name:            m.Name,
		Tids:            slices.Clone(m.Tids),
		GroupTIds:       slices.Clone(m.GroupTIds),
		Params:          slices.Clone(m.Params),
		PendingRequeues: m.PendingRequeues,
		RetChan:         m.RetChan,
		Cb1:             m.Cb1,
		Cost:            m.Cost,
		HasRemote:       m.HasRemote,
		RemoteRelease:   m.RemoteRelease,
		Context:         m.Context.Clone(),
		GroupTransition: m.GroupTransition,
	}
	return ret
}

func (m *Msg) String() string {
	buf := make([]byte, 0, 128)
	buf = append(buf, "Msg{Name:"...)
	buf = append(buf, m.Name...)
	buf = append(buf, ",Type:"...)
	buf = append(buf, m.Type.String()...)
	if m.Tid != 0 {
		buf = append(buf, ",Tid:"...)
		buf = strconv.AppendInt(buf, m.Tid, 10)
	}
	if len(m.Tids) > 0 {
		buf = append(buf, ",Tids:["...)
		for i, id := range m.Tids {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = strconv.AppendInt(buf, id, 10)
		}
		buf = append(buf, ']')
	}
	buf = append(buf, '}')
	return string(buf)
}

// TickMsg is dispatched each frame tick.
type TickMsg struct {
	Elapsed     int64 // nanoseconds
	FrameNumber uint64
}

var msgPool = sync.Pool{
	New: func() interface{} {
		return &Msg{}
	},
}

func GenMsg(msgType MsgType) *Msg {
	msg := msgPool.Get().(*Msg)
	msg.Type = msgType
	return msg
}

func GenSyncMsg(msgType MsgType) (*Msg, chan any) {
	msg := GenMsg(msgType)
	ch := make(chan any, 1)
	msg.RetChan = ch
	return msg, ch
}

func recycleMsg(m *Msg) {
	if m != nil {
		m.clean()
		msgPool.Put(m)
	}
}

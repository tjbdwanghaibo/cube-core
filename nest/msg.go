package nest

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"

	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
)

func (m *Msg) addRemoteRelease(release entity.RemoteEntityRelease) {
	if m != nil && release != nil {
		m.RemoteReleases = append(m.RemoteReleases, release)
	}
}

func (m *Msg) releaseRemoteEntities() error {
	if m == nil || len(m.RemoteReleases) == 0 {
		return nil
	}
	releases := m.RemoteReleases
	m.RemoteReleases = nil
	var joined error
	for i := len(releases) - 1; i >= 0; i-- {
		if releases[i] != nil {
			joined = errors.Join(joined, releases[i]())
		}
	}
	return joined
}

func (m *Msg) setRemoteWriteBatch(batch entity.RemoteWriteBatch) {
	if m != nil {
		m.RemoteWriteBatch = batch
	}
}

func (m *Msg) finalizeRemoteWriteBatch(tx *RollbackTx) error {
	if m == nil || m.RemoteWriteBatch == nil || m.remoteFinalized {
		return nil
	}
	if tx == nil {
		return entity.ErrRemoteCommitNotFinalized
	}
	outcome := entity.NewRemoteTransactionOutcome(
		entity.RemoteTransactionID(tx.ID()), tx.handler, tx.requestID(), true, uint8(tx.durability),
	)
	if err := m.RemoteWriteBatch.FinalizeLocked(outcome); err != nil {
		return err
	}
	for _, commit := range m.RemoteWriteBatch.Commits() {
		commit := commit.Clone()
		if err := tx.AddMutation(EntityMutation{
			EntityID: commit.EntityID,
			Resource: "remote_entity",
			Version:  commit.NextVersion,
			Schema:   commit.Schema,
			Codec:    "remote",
			Remote:   &commit,
		}); err != nil {
			return err
		}
	}
	m.remoteFinalized = true
	return nil
}

func (m *Msg) finishRemoteWriteBatch(ctx context.Context, dispatchErr error) error {
	if m == nil || m.RemoteWriteBatch == nil {
		return nil
	}
	batch := m.RemoteWriteBatch
	m.RemoteWriteBatch = nil
	var err error
	if m.remoteIndeterminate {
		// WAL may already contain the transaction. Close transfers ownership of
		// the held gates/leases to the manager's status-driven finalizer.
	} else if dispatchErr != nil || !m.remoteFinalized {
		err = batch.Abort(ctx, dispatchErr)
	} else {
		_, err = batch.Commit(ctx)
	}
	err = errors.Join(err, batch.Close(ctx))
	if err == nil && dispatchErr == nil {
		m.runPostRemoteCommit()
	}
	return err
}

func (m *Msg) abortRemoteWriteBatchLocked(cause error) error {
	if m == nil || m.RemoteWriteBatch == nil {
		return nil
	}
	return m.RemoteWriteBatch.Abort(context.Background(), cause)
}

func (m *Msg) markRemoteWriteIndeterminateLocked(cause error) error {
	if m == nil || m.RemoteWriteBatch == nil {
		return nil
	}
	if err := m.RemoteWriteBatch.Indeterminate(context.Background(), cause); err != nil {
		return err
	}
	m.remoteIndeterminate = true
	return nil
}

func (m *Msg) addAfterUnlock(fn func()) {
	if m != nil && fn != nil {
		m.afterUnlock = append(m.afterUnlock, fn)
	}
}

func (m *Msg) runAfterUnlock() {
	if m == nil {
		return
	}
	callbacks := m.afterUnlock
	m.afterUnlock = nil
	for _, callback := range callbacks {
		if callback != nil {
			callback()
		}
	}
}

func (m *Msg) addPostRemoteCommit(callbacks ...func()) {
	if m != nil {
		m.postRemoteCommit = append(m.postRemoteCommit, callbacks...)
	}
}

func (m *Msg) runPostRemoteCommit() {
	callbacks := m.postRemoteCommit
	m.postRemoteCommit = nil
	for _, callback := range callbacks {
		if callback != nil {
			callback()
		}
	}
}

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
	RetChan             chan any
	RemoteReleases      []entity.RemoteEntityRelease
	RemoteWriteBatch    entity.RemoteWriteBatch
	Name                string
	Tids                []int64
	GroupTIds           [][]int64
	Params              []any
	Tid                 int64
	RefCount            int
	PendingRequeues     int
	Type                MsgType
	Cost                bool
	HasRemote           bool // message involves remote entities
	Context             fctx.ContextSnapshot
	GroupTransition     *GroupTransitionRequest
	remoteFinalized     bool
	remoteIndeterminate bool
	afterUnlock         []func()
	postRemoteCommit    []func()
	getter              entity.Getter
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
		Cost:            m.Cost,
		HasRemote:       m.HasRemote,
		RemoteReleases:  slices.Clone(m.RemoteReleases),
		Context:         m.Context.Clone(),
		GroupTransition: m.GroupTransition,
		getter:          m.getter,
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

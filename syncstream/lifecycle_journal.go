package syncstream

import (
	"time"
)

const HistoryMutationVersion uint32 = 1

type HistoryMutationKind string

const (
	HistoryMutationAppend      HistoryMutationKind = "append"
	HistoryMutationAcknowledge HistoryMutationKind = "acknowledge"
	HistoryMutationDelete      HistoryMutationKind = "delete_stream"
	HistoryMutationDeleteView  HistoryMutationKind = "delete_observer"
	HistoryMutationSweep       HistoryMutationKind = "sweep_idle"
	HistoryMutationRotateEpoch HistoryMutationKind = "rotate_epoch"
)

// HistoryMutation is the write-ahead record required to reproduce one
// successful in-memory mutation. A journal must durably commit Record before it
// returns nil.
type HistoryMutation struct {
	Version           uint32
	Kind              HistoryMutationKind
	Epoch             uint64
	Packet            Packet
	Observer          Observer
	Stream            Stream
	Sequence          uint64
	Targets           []HistoryTarget
	PruneAcknowledged bool
}

type HistoryTarget struct {
	Observer Observer
	Stream   Stream
}

// HistoryJournal supplies crash-consistent write-ahead persistence. Load must
// return the latest checkpoint with all committed mutations replayed.
type HistoryJournal interface {
	Load() (HistorySnapshot, error)
	Record(HistoryMutation) error
	Checkpoint(HistorySnapshot) error
}

func NewHistoryWithJournal(options HistoryOptions, journal HistoryJournal) (*History, error) {
	if journal == nil {
		return nil, ErrHistoryJournalRequired
	}
	history := NewHistory(options)
	snapshot, err := journal.Load()
	if err != nil {
		return nil, err
	}
	if err := history.Import(snapshot); err != nil {
		return nil, err
	}
	history.journal = journal
	return history, nil
}

func (history *History) Checkpoint() error {
	if history == nil || history.journal == nil {
		return ErrHistoryJournalRequired
	}
	history.mutex.RLock()
	defer history.mutex.RUnlock()
	return history.journal.Checkpoint(history.exportLocked())
}

func (history *History) Epoch() uint64 {
	if history == nil {
		return 0
	}
	history.mutex.RLock()
	defer history.mutex.RUnlock()
	return history.epoch
}

// RotateEpoch invalidates every old stream chain. A zero epoch generates a new
// unpredictable generation.
func (history *History) RotateEpoch(epoch uint64) error {
	if epoch == 0 {
		epoch = newEpoch()
	}
	history.mutex.Lock()
	defer history.mutex.Unlock()
	if epoch == history.epoch {
		return nil
	}
	if history.journal != nil {
		if err := history.journal.Record(HistoryMutation{Version: HistoryMutationVersion, Kind: HistoryMutationRotateEpoch, Epoch: epoch}); err != nil {
			return err
		}
	}
	history.epoch = epoch
	history.streams = make(map[streamKey]*streamState)
	return nil
}

func (history *History) DeleteStream(observer Observer, stream Stream) error {
	history.mutex.Lock()
	defer history.mutex.Unlock()
	key := streamKey{Observer: observer, Stream: stream}
	if _, ok := history.streams[key]; !ok {
		return nil
	}
	if history.journal != nil {
		if err := history.journal.Record(HistoryMutation{Version: HistoryMutationVersion, Kind: HistoryMutationDelete, Epoch: history.epoch, Observer: observer, Stream: stream}); err != nil {
			return err
		}
	}
	delete(history.streams, key)
	return nil
}

func (history *History) DeleteObserver(observer Observer) (int, error) {
	history.mutex.Lock()
	defer history.mutex.Unlock()
	count := 0
	for key := range history.streams {
		if key.Observer == observer {
			count++
		}
	}
	if count == 0 {
		return 0, nil
	}
	if history.journal != nil {
		if err := history.journal.Record(HistoryMutation{Version: HistoryMutationVersion, Kind: HistoryMutationDeleteView, Epoch: history.epoch, Observer: observer}); err != nil {
			return 0, err
		}
	}
	for key := range history.streams {
		if key.Observer == observer {
			delete(history.streams, key)
		}
	}
	return count, nil
}

// SweepIdle removes streams that have seen neither append nor ACK during the
// configured IdleTTL. Deletion is journaled per stream before becoming visible.
func (history *History) SweepIdle(now time.Time) (int, error) {
	if history == nil || history.options.IdleTTL <= 0 {
		return 0, nil
	}
	cutoff := now.Add(-history.options.IdleTTL).UnixNano()
	history.mutex.Lock()
	defer history.mutex.Unlock()
	keys := make([]streamKey, 0)
	for key, state := range history.streams {
		if state.activity != 0 && state.activity <= cutoff {
			keys = append(keys, key)
		}
	}
	if len(keys) > 0 && history.journal != nil {
		targets := make([]HistoryTarget, len(keys))
		for index, key := range keys {
			targets[index] = HistoryTarget{Observer: key.Observer, Stream: key.Stream}
		}
		if err := history.journal.Record(HistoryMutation{Version: HistoryMutationVersion, Kind: HistoryMutationSweep, Epoch: history.epoch, Targets: targets}); err != nil {
			return 0, err
		}
	}
	for _, key := range keys {
		delete(history.streams, key)
	}
	return len(keys), nil
}

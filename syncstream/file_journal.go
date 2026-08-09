package syncstream

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// FileHistoryJournal stores immutable checkpoint/WAL generations. A
// checkpoint first creates and fsyncs the next WAL, then atomically publishes
// a uniquely named checkpoint. Recovery requires the newest pair to be valid,
// so corruption is never hidden by silently rolling authoritative state back.
// The previous generation remains available for explicit operator recovery.
type FileHistoryJournal struct {
	mutex        sync.Mutex
	directory    string
	initialEpoch uint64
	generation   uint64
}

type fileCheckpoint struct {
	Generation uint64          `json:"generation"`
	Snapshot   HistorySnapshot `json:"snapshot"`
}

func NewFileHistoryJournal(directory string, initialEpoch uint64) (*FileHistoryJournal, error) {
	if directory == "" {
		return nil, ErrHistoryJournalRequired
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, err
	}
	if initialEpoch == 0 {
		initialEpoch = newEpoch()
	}
	return &FileHistoryJournal{directory: absolute, initialEpoch: initialEpoch, generation: 1}, nil
}

func (journal *FileHistoryJournal) checkpointPath(generation uint64) string {
	return filepath.Join(journal.directory, fmt.Sprintf("history.checkpoint-%020d.json", generation))
}

func (journal *FileHistoryJournal) walPath(generation uint64) string {
	return filepath.Join(journal.directory, fmt.Sprintf("history.wal-%020d.jsonl", generation))
}

func (journal *FileHistoryJournal) Load() (HistorySnapshot, error) {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()

	generations, err := journal.checkpointGenerations()
	if err != nil {
		return HistorySnapshot{}, err
	}
	if len(generations) == 0 {
		journal.generation = 1
		snapshot := HistorySnapshot{Version: HistorySnapshotVersion, Epoch: journal.initialEpoch}
		if info, statErr := os.Stat(journal.walPath(1)); statErr == nil {
			if info.Size() > 0 {
				// The first committed mutation is the durable source of the
				// randomly generated epoch before the first checkpoint.
				snapshot.Epoch = 0
			}
			if replayErr := replayWAL(journal.walPath(1), &snapshot); replayErr != nil {
				return HistorySnapshot{}, replayErr
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return HistorySnapshot{}, statErr
		}
		return snapshot, nil
	}

	generation := generations[0]
	snapshot, err := journal.loadGeneration(generation)
	if err != nil {
		return HistorySnapshot{}, fmt.Errorf("syncstream: newest history generation %d is invalid: %w", generation, err)
	}
	journal.generation = generation
	return snapshot, nil
}

func (journal *FileHistoryJournal) checkpointGenerations() ([]uint64, error) {
	paths, err := filepath.Glob(filepath.Join(journal.directory, "history.checkpoint-*.json"))
	if err != nil {
		return nil, err
	}
	result := make([]uint64, 0, len(paths))
	for _, path := range paths {
		name := filepath.Base(path)
		value := strings.TrimSuffix(strings.TrimPrefix(name, "history.checkpoint-"), ".json")
		generation, parseErr := strconv.ParseUint(value, 10, 64)
		if parseErr == nil && generation > 0 {
			result = append(result, generation)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i] > result[j] })
	return result, nil
}

func (journal *FileHistoryJournal) loadGeneration(generation uint64) (HistorySnapshot, error) {
	data, err := os.ReadFile(journal.checkpointPath(generation))
	if err != nil {
		return HistorySnapshot{}, err
	}
	var checkpoint fileCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return HistorySnapshot{}, err
	}
	if checkpoint.Generation != generation {
		return HistorySnapshot{}, ErrInvalidSnapshot
	}
	if err := replayWAL(journal.walPath(generation), &checkpoint.Snapshot); err != nil {
		return HistorySnapshot{}, err
	}
	return checkpoint.Snapshot, nil
}

func replayWAL(path string, snapshot *HistorySnapshot) error {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("syncstream: WAL generation missing: %w", err)
	}
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 && (readErr == nil || line[len(line)-1] == '\n') {
			var mutation HistoryMutation
			if err := json.Unmarshal(line, &mutation); err != nil {
				return fmt.Errorf("syncstream: decode WAL: %w", err)
			}
			if err := replayHistoryMutation(snapshot, mutation); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil // an unterminated tail was never durably committed
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (journal *FileHistoryJournal) Record(mutation HistoryMutation) error {
	if mutation.Version != HistoryMutationVersion {
		return ErrInvalidSnapshot
	}
	data, err := json.Marshal(mutation)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	file, err := os.OpenFile(journal.walPath(journal.generation), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

func (journal *FileHistoryJournal) Checkpoint(snapshot HistorySnapshot) error {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()

	next := journal.generation + 1
	wal, err := os.OpenFile(journal.walPath(next), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errors.Is(err, os.ErrExist) {
		// An orphan from a crash before checkpoint publication contains no
		// committed records and is safe to replace.
		if removeErr := os.Remove(journal.walPath(next)); removeErr != nil {
			return removeErr
		}
		wal, err = os.OpenFile(journal.walPath(next), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	}
	if err != nil {
		return err
	}
	if err := wal.Sync(); err != nil {
		_ = wal.Close()
		return err
	}
	if err := wal.Close(); err != nil {
		return err
	}

	data, err := json.Marshal(fileCheckpoint{Generation: next, Snapshot: snapshot})
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(journal.directory, "history.checkpoint-*.tmp")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, journal.checkpointPath(next)); err != nil {
		return err
	}
	removeTemporary = false
	journal.generation = next
	journal.removeObsoleteGenerations(next)
	return nil
}

func (journal *FileHistoryJournal) removeObsoleteGenerations(current uint64) {
	if current <= 2 {
		return
	}
	obsolete := current - 2
	_ = os.Remove(journal.checkpointPath(obsolete))
	_ = os.Remove(journal.walPath(obsolete))
}

func replayHistoryMutation(snapshot *HistorySnapshot, mutation HistoryMutation) error {
	if snapshot == nil || mutation.Version != HistoryMutationVersion {
		return ErrInvalidSnapshot
	}
	if mutation.Kind == HistoryMutationRotateEpoch {
		snapshot.Epoch, snapshot.Streams = mutation.Epoch, nil
		return nil
	}
	if snapshot.Epoch == 0 {
		snapshot.Epoch = mutation.Epoch
	}
	if mutation.Epoch != snapshot.Epoch {
		return ErrInvalidSnapshot
	}
	find := func(observer Observer, stream Stream) int {
		for index := range snapshot.Streams {
			if snapshot.Streams[index].Observer == observer && snapshot.Streams[index].Stream == stream {
				return index
			}
		}
		return -1
	}
	switch mutation.Kind {
	case HistoryMutationAppend:
		index := find(mutation.Packet.Observer, mutation.Packet.Stream)
		if index < 0 {
			snapshot.Streams = append(snapshot.Streams, HistoryStreamSnapshot{Observer: mutation.Packet.Observer, Stream: mutation.Packet.Stream})
			index = len(snapshot.Streams) - 1
		}
		value := &snapshot.Streams[index]
		if mutation.Packet.Sequence != value.Latest+1 {
			return ErrInvalidSnapshot
		}
		value.Latest = mutation.Packet.Sequence
		value.Schema = mutation.Packet.SchemaVersion
		value.Packets = append(value.Packets, mutation.Packet.Clone())
	case HistoryMutationAcknowledge:
		index := find(mutation.Observer, mutation.Stream)
		if index < 0 || mutation.Sequence > snapshot.Streams[index].Latest {
			return ErrInvalidSnapshot
		}
		if mutation.Sequence > snapshot.Streams[index].Acked {
			value := &snapshot.Streams[index]
			value.Acked = mutation.Sequence
			if mutation.PruneAcknowledged {
				pruned := 0
				for pruned < len(value.Packets) && value.Packets[pruned].Sequence <= mutation.Sequence {
					pruned++
				}
				value.Packets = append([]Packet(nil), value.Packets[pruned:]...)
				value.Pruned += uint64(pruned)
			}
		}
	case HistoryMutationDelete:
		index := find(mutation.Observer, mutation.Stream)
		if index >= 0 {
			snapshot.Streams = append(snapshot.Streams[:index], snapshot.Streams[index+1:]...)
		}
	case HistoryMutationDeleteView:
		result := snapshot.Streams[:0]
		for _, value := range snapshot.Streams {
			if value.Observer != mutation.Observer {
				result = append(result, value)
			}
		}
		snapshot.Streams = result
	case HistoryMutationSweep:
		for _, target := range mutation.Targets {
			index := find(target.Observer, target.Stream)
			if index >= 0 {
				snapshot.Streams = append(snapshot.Streams[:index], snapshot.Streams[index+1:]...)
			}
		}
	default:
		return ErrInvalidSnapshot
	}
	return nil
}

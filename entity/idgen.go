package entity

import (
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
)

// IDGen generates the dynamic portion of EntityID.
//
// Layout:
//
//	unique 52 bits = [blockNo][offset]
//
// The default offset width is 20 bits, so each acquired block provides
// 1,048,576 local IDs. The block number should be allocated globally and
// monotonically, for example by Redis INCR minus one or an etcd transaction.
// This keeps early IDs compact because block 0 starts at unique 1; unique 0 is
// reserved as an invalid/empty value for all full EntityIDs. The upper half of
// the 52-bit unique range is reserved for explicit static IDs.
const (
	IDGenOffsetBits = 20
	IDGenBlockSize  = uint64(1) << IDGenOffsetBits

	UniqueIDBits        = 52
	UniqueIDMask        = (uint64(1) << UniqueIDBits) - 1
	StaticUniqueIDBase  = uint64(1) << (UniqueIDBits - 1)
	DynamicUniqueIDMask = StaticUniqueIDBase - 1
	MaxBlockNo          = DynamicUniqueIDMask >> IDGenOffsetBits
)

// EntityID layout in positive int64:
//
//	[sign(1)=0][remote(1)][unique(52)][kind(8)][category(2)]
const (
	EntityCategoryBits = 2
	EntityKindBits     = 8
	EntityRemoteBits   = 1
	EntityIDTagBits    = EntityCategoryBits + EntityKindBits

	EntityCategoryMask = (uint64(1) << EntityCategoryBits) - 1
	EntityKindMask     = (uint64(1) << EntityKindBits) - 1
	EntityRemoteMask   = (uint64(1) << EntityRemoteBits) - 1

	EntityKindShift    = EntityCategoryBits
	UniqueIDShift      = EntityIDTagBits
	EntityRemoteShift  = UniqueIDShift + UniqueIDBits
	EntityIDValueBits  = UniqueIDBits + EntityIDTagBits + EntityRemoteBits // 63 bits, positive int64
	EntityIDValueMask  = (uint64(1) << EntityIDValueBits) - 1
	MaxEntityKindValue = EntityKindMask
)

var (
	ErrInvalidBlockNo    = errors.New("idgen: block number exceeds 52-bit unique range")
	ErrBlockExhausted    = errors.New("idgen: current block exhausted and no block allocator configured")
	ErrNoBlockSource     = errors.New("idgen: no initial block or block allocator configured")
	ErrInvalidCategory   = errors.New("entity id: category exceeds 2-bit limit")
	ErrInvalidEntityKind = errors.New("entity id: kind exceeds 8-bit limit")
	ErrInvalidEntityID   = errors.New("entity id: invalid")
)

// IDGen is a lock-free fast path unique ID generator. It only calls acquireBlock
// when the current block is exhausted.
type IDGen struct {
	next         atomic.Uint64
	end          atomic.Uint64
	rotating     atomic.Int32
	acquireBlock atomic.Value // stores func() (uint64, error)
}

func NewIDGen(initialBlock uint64, acquireBlock func() (uint64, error)) (*IDGen, error) {
	if initialBlock > MaxBlockNo {
		return nil, ErrInvalidBlockNo
	}
	gen := &IDGen{}
	if acquireBlock != nil {
		gen.acquireBlock.Store(acquireBlock)
	}
	gen.resetBlock(initialBlock)
	return gen, nil
}

func (g *IDGen) Generate() (uint64, error) {
	for {
		next := g.next.Load()
		end := g.end.Load()
		if next < end {
			if g.next.CompareAndSwap(next, next+1) {
				return next, nil
			}
			continue
		}

		if err := g.rotateBlock(); err != nil {
			return 0, err
		}
	}
}

func (g *IDGen) rotateBlock() error {
	fn, _ := g.acquireBlock.Load().(func() (uint64, error))
	if fn == nil {
		return ErrBlockExhausted
	}

	if g.rotating.CompareAndSwap(0, 1) {
		defer g.rotating.Store(0)
		blockNo, err := fn()
		if err != nil {
			return err
		}
		if blockNo > MaxBlockNo {
			return ErrInvalidBlockNo
		}
		g.resetBlock(blockNo)
		return nil
	}

	for g.rotating.Load() != 0 {
		runtime.Gosched()
	}
	return nil
}

func (g *IDGen) resetBlock(blockNo uint64) {
	start := blockNo << IDGenOffsetBits
	if blockNo == 0 {
		start = 1
	}
	g.next.Store(start)
	g.end.Store((blockNo << IDGenOffsetBits) + IDGenBlockSize)
}

func ParseUniqueID(id uint64) (blockNo uint64, offset uint64) {
	id &= UniqueIDMask
	return id >> IDGenOffsetBits, id & (IDGenBlockSize - 1)
}

func BuildEntityID(uniqueID int64, kind EntityKind) (int64, error) {
	category, err := ResolveEntityKindCategory(kind)
	if err != nil {
		return 0, err
	}
	return buildEntityIDWithCategory(uniqueID, category, kind)
}

func buildEntityIDWithCategory(uniqueID int64, category EntityCategory, kind EntityKind) (int64, error) {
	if uniqueID <= 0 || uint64(uniqueID) > UniqueIDMask {
		return 0, fmt.Errorf("%w: unique id %d outside 1..%d", ErrInvalidEntityID, uniqueID, UniqueIDMask)
	}
	if uint64(category) > EntityCategoryMask {
		return 0, ErrInvalidCategory
	}
	if category == EntityCategoryNone {
		return 0, fmt.Errorf("%w: category is none", ErrInvalidEntityID)
	}
	if uint64(kind) > EntityKindMask {
		return 0, ErrInvalidEntityKind
	}
	if kind == EntityKindNone {
		return 0, fmt.Errorf("%w: kind is none", ErrInvalidEntityID)
	}
	return makeEntityID(uniqueID, category, kind, kind != EntityKindNone && IsEntityKindRemoteCapable(kind)), nil
}

func makeEntityID(uniqueID int64, category EntityCategory, kind EntityKind, remoteCapable bool) int64 {
	if uint64(category) > EntityCategoryMask {
		panic(ErrInvalidCategory)
	}
	if uint64(kind) > EntityKindMask {
		panic(ErrInvalidEntityKind)
	}
	id := ((uint64(uniqueID) & UniqueIDMask) << UniqueIDShift) |
		((uint64(kind) & EntityKindMask) << EntityKindShift) |
		(uint64(category) & EntityCategoryMask)
	if kind != EntityKindNone && IsEntityKindRemoteCapable(kind) {
		remoteCapable = true
	}
	if remoteCapable {
		id |= EntityRemoteMask << EntityRemoteShift
	}
	return int64(id)
}

func GetEntityCategoryFromID(id int64) EntityCategory {
	return EntityCategory(uint64(id) & EntityCategoryMask)
}

func GetEntityKindFromID(id int64) EntityKind {
	return EntityKind((uint64(id) >> EntityKindShift) & EntityKindMask)
}

func GetEntityRemoteFromID(id int64) bool {
	return ((uint64(id) >> EntityRemoteShift) & EntityRemoteMask) != 0
}

func GetUniqueIDFromEntityID(id int64) int64 {
	return int64((uint64(id) >> UniqueIDShift) & UniqueIDMask)
}

func NormalizeFullID(id int64, kind EntityKind) (int64, error) {
	if id == 0 {
		return 0, fmt.Errorf("%w: empty id", ErrInvalidEntityID)
	}
	meta := ResolveEntityID(id)
	if kind != EntityKindNone && meta.Kind != kind {
		return 0, fmt.Errorf("%w: id %d kind=%d want=%d", ErrInvalidEntityID, id, meta.Kind, kind)
	}
	if meta.Category == EntityCategoryNone {
		return 0, fmt.Errorf("%w: id %d has empty category", ErrInvalidEntityID, id)
	}
	if meta.Kind == EntityKindNone {
		return 0, fmt.Errorf("%w: id %d has empty kind", ErrInvalidEntityID, id)
	}
	if meta.UniqueID == 0 {
		return 0, fmt.Errorf("%w: id %d has empty unique id", ErrInvalidEntityID, id)
	}
	category, err := ResolveEntityKindCategory(meta.Kind)
	if err != nil {
		return 0, err
	}
	if meta.Category != category {
		return 0, fmt.Errorf("%w: id %d category=%d want=%d for kind=%d", ErrInvalidEntityID, id, meta.Category, category, meta.Kind)
	}
	return meta.FullID, nil
}

func MatchEntityID(id int64, kind EntityKind) bool {
	_, err := NormalizeFullID(id, kind)
	return err == nil
}

// IsRemoteCapableEntityID returns whether the EntityID carries the
// remote-capable identity. EntityKind registration is authoritative when it is
// available; the remote bit is the wire/storage hint for processes that only
// have the ID.
func IsRemoteCapableEntityID(id int64) bool {
	kind := GetEntityKindFromID(id)
	if kind != EntityKindNone && IsEntityKindRemoteCapable(kind) {
		return true
	}
	return GetEntityRemoteFromID(id)
}

func setRemoteCapableBit(id int64) int64 {
	return int64(uint64(id) | (EntityRemoteMask << EntityRemoteShift))
}

func clearRemoteCapableBit(id int64) int64 {
	return int64(uint64(id) & ^(EntityRemoteMask << EntityRemoteShift))
}

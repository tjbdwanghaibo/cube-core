package entitysync

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

var (
	ErrSubscriberInvalid       = errors.New("entitysync: subscriber is invalid")
	ErrSubscriptionSubject     = errors.New("entitysync: subscription subject is invalid")
	ErrEnvelopeSinkRequired    = errors.New("entitysync: reliable envelope sink is required")
	ErrEnvelopeAdmission       = errors.New("entitysync: envelope admission failed")
	ErrSubscriptionNotFound    = errors.New("entitysync: subscription not found")
	ErrSubscriptionState       = errors.New("entitysync: invalid subscription state")
	ErrPreparedProfilesMissing = errors.New("entitysync: prepared update is missing a subscribed profile")
)

type SubscriberKind uint8

const (
	SubscriberKindNone SubscriberKind = iota
	SubscriberKindPlayer
	SubscriberKindServer
	SubscriberKindEntity
	SubscriberKindGroup
)

type SubscriberRef struct {
	Kind SubscriberKind
	ID   int64
	Sid  int32
	Key  string
}

func (r SubscriberRef) Normalize() SubscriberRef {
	if r.Kind == SubscriberKindNone && r.ID != 0 {
		r.Kind = SubscriberKindPlayer
	}
	return r
}

func (r SubscriberRef) Empty() bool {
	r = r.Normalize()
	return r.Kind == SubscriberKindNone || (r.ID == 0 && r.Sid == 0 && r.Key == "")
}

type SubscriptionState uint8

const (
	SubscriptionPending SubscriptionState = iota + 1
	SubscriptionActive
	SubscriptionClosing
)

type Subscription struct {
	Subscriber     SubscriberRef
	SubjectID      int64
	Namespace      string
	SubjectKind    uint32
	Profile        entity.SyncProfile
	State          SubscriptionState
	ContentVersion uint64
	Revision       uint64
}

type EnvelopeKind uint8

const (
	EnvelopeSnapshot EnvelopeKind = iota + 1
	EnvelopeDelta
	EnvelopeLeave
)

type DeliveryEnvelope struct {
	Subscriber SubscriberRef
	Kind       EnvelopeKind
	Update     entity.SubjectSyncUpdate
}

// ReliableEnvelopeSink must admit the complete batch or return an error. It
// owns session sequencing, history, routing, and transport-specific framing.
type ReliableEnvelopeSink interface {
	AdmitEnvelopes(context.Context, []DeliveryEnvelope) error
}

type ReliableEnvelopeSinkFunc func(context.Context, []DeliveryEnvelope) error

func (f ReliableEnvelopeSinkFunc) AdmitEnvelopes(ctx context.Context, envelopes []DeliveryEnvelope) error {
	if f == nil {
		return ErrEnvelopeSinkRequired
	}
	return f(ctx, envelopes)
}

type subscriptionKey struct {
	subscriber SubscriberRef
	subjectID  int64
}

const subscriptionSubjectShardCount = 64

// SubscriptionCoordinator owns membership independently from Entity state.
// Operations for one subject are serialized; different subjects remain parallel.
type SubscriptionCoordinator struct {
	mu            sync.RWMutex
	sink          ReliableEnvelopeSink
	subscriptions map[subscriptionKey]Subscription
	revision      uint64
	subjectOps    [subscriptionSubjectShardCount]sync.Mutex
}

func NewSubscriptionCoordinator(sink ReliableEnvelopeSink) *SubscriptionCoordinator {
	return &SubscriptionCoordinator{
		sink:          sink,
		subscriptions: make(map[subscriptionKey]Subscription),
	}
}

func (c *SubscriptionCoordinator) SetSink(sink ReliableEnvelopeSink) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.sink = sink
	c.mu.Unlock()
}

func (c *SubscriptionCoordinator) Subscribe(ctx context.Context, subscriber SubscriberRef, state *entity.SubjectSyncState, profile entity.SyncProfile) (Subscription, error) {
	subscriber = subscriber.Normalize()
	if c == nil || subscriber.Empty() {
		return Subscription{}, ErrSubscriberInvalid
	}
	if state == nil || state.SubjectID() == 0 {
		return Subscription{}, ErrSubscriptionSubject
	}
	if ctx == nil {
		ctx = context.Background()
	}
	profile = profile.Normalize()
	subjectID := state.SubjectID()
	op := c.subjectOp(subjectID)
	op.Lock()
	defer op.Unlock()

	key := subscriptionKey{subscriber: subscriber, subjectID: subjectID}
	c.mu.RLock()
	existing, exists := c.subscriptions[key]
	c.mu.RUnlock()
	if exists && existing.State == SubscriptionActive && existing.Profile == profile {
		return existing, nil
	}

	pending := Subscription{
		Subscriber: subscriber, SubjectID: subjectID, Profile: profile,
		Namespace: state.Namespace(), SubjectKind: state.SubjectKind(),
		State: SubscriptionPending, Revision: c.nextRevision(),
	}
	c.mu.Lock()
	c.subscriptions[key] = pending
	sink := c.sink
	c.mu.Unlock()
	if sink == nil {
		c.rollbackSubscription(key, existing, exists)
		return Subscription{}, ErrEnvelopeSinkRequired
	}

	snapshots, err := state.CaptureSnapshot([]entity.SyncProfile{profile}, entity.SyncFullReasonResync)
	if err != nil || len(snapshots) != 1 {
		c.rollbackSubscription(key, existing, exists)
		if err == nil {
			err = ErrPreparedProfilesMissing
		}
		return Subscription{}, err
	}
	envelope := DeliveryEnvelope{Subscriber: subscriber, Kind: EnvelopeSnapshot, Update: snapshots[0]}
	if err := admitEnvelopes(ctx, sink, []DeliveryEnvelope{envelope}); err != nil {
		c.rollbackSubscription(key, existing, exists)
		return Subscription{}, errors.Join(ErrEnvelopeAdmission, err)
	}

	pending.State = SubscriptionActive
	pending.ContentVersion = snapshots[0].Version
	c.mu.Lock()
	current, ok := c.subscriptions[key]
	if !ok || current.Revision != pending.Revision || current.State != SubscriptionPending {
		c.mu.Unlock()
		return Subscription{}, ErrSubscriptionState
	}
	c.subscriptions[key] = pending
	c.mu.Unlock()
	return pending, nil
}

func (c *SubscriptionCoordinator) Unsubscribe(ctx context.Context, subscriber SubscriberRef, subjectID int64) error {
	subscriber = subscriber.Normalize()
	if c == nil || subscriber.Empty() {
		return ErrSubscriberInvalid
	}
	if subjectID == 0 {
		return ErrSubscriptionSubject
	}
	if ctx == nil {
		ctx = context.Background()
	}
	op := c.subjectOp(subjectID)
	op.Lock()
	defer op.Unlock()
	key := subscriptionKey{subscriber: subscriber, subjectID: subjectID}
	c.mu.Lock()
	current, ok := c.subscriptions[key]
	if !ok {
		c.mu.Unlock()
		return ErrSubscriptionNotFound
	}
	if current.State != SubscriptionActive {
		c.mu.Unlock()
		return ErrSubscriptionState
	}
	current.State = SubscriptionClosing
	current.Revision = c.nextRevisionLocked()
	c.subscriptions[key] = current
	sink := c.sink
	c.mu.Unlock()
	if sink == nil {
		c.restoreActive(key, current)
		return ErrEnvelopeSinkRequired
	}
	leave := DeliveryEnvelope{
		Subscriber: subscriber,
		Kind:       EnvelopeLeave,
		Update: entity.SubjectSyncUpdate{
			SubjectID: subjectID, Namespace: current.Namespace, SubjectKind: current.SubjectKind,
			Profile: current.Profile,
			Version: current.ContentVersion, BaseVersion: current.ContentVersion,
		},
	}
	if err := admitEnvelopes(ctx, sink, []DeliveryEnvelope{leave}); err != nil {
		c.restoreActive(key, current)
		return errors.Join(ErrEnvelopeAdmission, err)
	}
	c.mu.Lock()
	latest, ok := c.subscriptions[key]
	if ok && latest.Revision == current.Revision && latest.State == SubscriptionClosing {
		delete(c.subscriptions, key)
	}
	c.mu.Unlock()
	return nil
}

// Distribute admits all envelopes first and commits Entity content only after
// successful admission. Subscriber membership is never stored in Entity state.
func (c *SubscriptionCoordinator) Distribute(ctx context.Context, prepared *entity.PreparedSubjectSync) error {
	if c == nil || prepared == nil {
		return ErrSubscriptionSubject
	}
	updates := prepared.Updates()
	if len(updates) == 0 || updates[0].SubjectID == 0 {
		_ = prepared.AbortWithError(ErrSubscriptionSubject)
		return ErrSubscriptionSubject
	}
	if ctx == nil {
		ctx = context.Background()
	}
	subjectID := updates[0].SubjectID
	op := c.subjectOp(subjectID)
	op.Lock()
	defer op.Unlock()

	byProfile := make(map[entity.SyncProfile]entity.SubjectSyncUpdate, len(updates))
	for _, update := range updates {
		if update.SubjectID != subjectID {
			_ = prepared.AbortWithError(ErrSubscriptionSubject)
			return ErrSubscriptionSubject
		}
		byProfile[update.Profile.Normalize()] = update
	}
	c.mu.RLock()
	sink := c.sink
	subscriptions := make([]Subscription, 0)
	for _, subscription := range c.subscriptions {
		if subscription.SubjectID == subjectID && subscription.State == SubscriptionActive {
			subscriptions = append(subscriptions, subscription)
		}
	}
	c.mu.RUnlock()
	sortSubscriptions(subscriptions)
	if len(subscriptions) > 0 && sink == nil {
		_ = prepared.AbortWithError(ErrEnvelopeSinkRequired)
		return ErrEnvelopeSinkRequired
	}
	envelopes := make([]DeliveryEnvelope, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		update, ok := byProfile[subscription.Profile.Normalize()]
		if !ok {
			_ = prepared.AbortWithError(ErrPreparedProfilesMissing)
			return fmt.Errorf("%w: %q", ErrPreparedProfilesMissing, subscription.Profile.Key)
		}
		kind := EnvelopeDelta
		if update.Full {
			kind = EnvelopeSnapshot
		}
		envelopes = append(envelopes, DeliveryEnvelope{Subscriber: subscription.Subscriber, Kind: kind, Update: update})
	}
	if len(envelopes) > 0 {
		if err := admitEnvelopes(ctx, sink, envelopes); err != nil {
			_ = prepared.AbortWithError(err)
			return errors.Join(ErrEnvelopeAdmission, err)
		}
	}
	if err := prepared.Commit(); err != nil {
		return err
	}
	c.mu.Lock()
	for _, subscription := range subscriptions {
		key := subscriptionKey{subscriber: subscription.Subscriber, subjectID: subjectID}
		current, ok := c.subscriptions[key]
		if ok && current.State == SubscriptionActive && current.Revision == subscription.Revision {
			current.ContentVersion = prepared.Version()
			c.subscriptions[key] = current
		}
	}
	c.mu.Unlock()
	return nil
}

// FlushSubject derives the active profile set, prepares each profile exactly
// once, and transactionally distributes the resulting shared payloads.
func (c *SubscriptionCoordinator) FlushSubject(ctx context.Context, state *entity.SubjectSyncState) error {
	if c == nil || state == nil || state.SubjectID() == 0 {
		return ErrSubscriptionSubject
	}
	profiles := c.Profiles(state.SubjectID())
	prepared, err := state.Prepare(profiles)
	if errors.Is(err, entity.ErrSubjectSyncNotDirty) {
		return nil
	}
	if err != nil {
		return err
	}
	return c.Distribute(ctx, prepared)
}

func (c *SubscriptionCoordinator) Get(subscriber SubscriberRef, subjectID int64) (Subscription, bool) {
	if c == nil {
		return Subscription{}, false
	}
	key := subscriptionKey{subscriber: subscriber.Normalize(), subjectID: subjectID}
	c.mu.RLock()
	subscription, ok := c.subscriptions[key]
	c.mu.RUnlock()
	return subscription, ok
}

func (c *SubscriptionCoordinator) Subscribers(subjectID int64) []Subscription {
	if c == nil || subjectID == 0 {
		return nil
	}
	c.mu.RLock()
	out := make([]Subscription, 0)
	for _, subscription := range c.subscriptions {
		if subscription.SubjectID == subjectID && subscription.State == SubscriptionActive {
			out = append(out, subscription)
		}
	}
	c.mu.RUnlock()
	sortSubscriptions(out)
	return out
}

func (c *SubscriptionCoordinator) Profiles(subjectID int64) []entity.SyncProfile {
	subscriptions := c.Subscribers(subjectID)
	unique := make(map[entity.SyncProfile]struct{}, len(subscriptions))
	for _, subscription := range subscriptions {
		unique[subscription.Profile.Normalize()] = struct{}{}
	}
	profiles := make([]entity.SyncProfile, 0, len(unique))
	for profile := range unique {
		profiles = append(profiles, profile)
	}
	sort.Slice(profiles, func(i, j int) bool {
		if profiles[i].Key != profiles[j].Key {
			return profiles[i].Key < profiles[j].Key
		}
		if profiles[i].LOD != profiles[j].LOD {
			return profiles[i].LOD < profiles[j].LOD
		}
		return profiles[i].SchemaVersion < profiles[j].SchemaVersion
	})
	return profiles
}

func (c *SubscriptionCoordinator) subjectOp(subjectID int64) *sync.Mutex {
	idx := uint64(subjectID)
	idx ^= idx >> 33
	idx *= 0xff51afd7ed558ccd
	idx ^= idx >> 33
	return &c.subjectOps[idx%subscriptionSubjectShardCount]
}

func (c *SubscriptionCoordinator) nextRevision() uint64 {
	c.mu.Lock()
	revision := c.nextRevisionLocked()
	c.mu.Unlock()
	return revision
}

func (c *SubscriptionCoordinator) nextRevisionLocked() uint64 {
	c.revision++
	return c.revision
}

func (c *SubscriptionCoordinator) rollbackSubscription(key subscriptionKey, previous Subscription, existed bool) {
	c.mu.Lock()
	if existed {
		c.subscriptions[key] = previous
	} else {
		delete(c.subscriptions, key)
	}
	c.mu.Unlock()
}

func (c *SubscriptionCoordinator) restoreActive(key subscriptionKey, subscription Subscription) {
	subscription.State = SubscriptionActive
	c.mu.Lock()
	c.subscriptions[key] = subscription
	c.mu.Unlock()
}

func sortSubscriptions(subscriptions []Subscription) {
	sort.Slice(subscriptions, func(i, j int) bool {
		a := subscriptions[i].Subscriber.Normalize()
		b := subscriptions[j].Subscriber.Normalize()
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		if a.Sid != b.Sid {
			return a.Sid < b.Sid
		}
		return a.Key < b.Key
	})
}

func admitEnvelopes(ctx context.Context, sink ReliableEnvelopeSink, envelopes []DeliveryEnvelope) (err error) {
	if sink == nil {
		return ErrEnvelopeSinkRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: sink panic: %v", ErrEnvelopeAdmission, recovered)
		}
	}()
	return sink.AdmitEnvelopes(ctx, envelopes)
}

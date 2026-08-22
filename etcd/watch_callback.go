package etcd

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

// WatchCallback consumes watcher.EventChan in a dedicated goroutine and calls
// handler serially. The returned subscription owns watcher and closes it when
// the context ends, the handler fails, or the subscription is closed.
//
// Delivery is intentionally lossless and does not add another queue: a slow
// handler applies backpressure to the watcher. Use a LocalMirror subscription
// when callback isolation and a bounded per-subscriber queue are required.
func WatchCallback(ctx context.Context, watcher IWatcher, handler WatchHandler) (IWatchSubscription, error) {
	if isNilInterface(watcher) {
		return nil, fmt.Errorf("%w: watcher is nil", ErrWatchInvalidCallback)
	}
	if handler == nil {
		return nil, fmt.Errorf("%w: handler is nil", ErrWatchInvalidCallback)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	callbackCtx, cancel := context.WithCancel(ctx)
	subscription := &watchCallbackSubscription{
		ctx:     callbackCtx,
		cancel:  cancel,
		watcher: watcher,
		handler: handler,
		done:    make(chan struct{}),
	}
	go subscription.run()
	return subscription, nil
}

type watchCallbackSubscription struct {
	ctx     context.Context
	cancel  context.CancelFunc
	watcher IWatcher
	handler WatchHandler
	done    chan struct{}

	closeOnce sync.Once
	errMu     sync.RWMutex
	err       error
	closing   atomic.Bool
}

func (s *watchCallbackSubscription) Done() <-chan struct{} { return s.done }

func (s *watchCallbackSubscription) Err() error {
	s.errMu.RLock()
	defer s.errMu.RUnlock()
	return s.err
}

func (s *watchCallbackSubscription) Close() error {
	s.requestClose()
	<-s.done
	return nil
}

func (s *watchCallbackSubscription) CloseWithContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.requestClose()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *watchCallbackSubscription) requestClose() {
	s.closeOnce.Do(func() {
		s.closing.Store(true)
		s.cancel()
		_ = s.watcher.Close()
	})
}

func (s *watchCallbackSubscription) run() {
	var terminalErr error
	defer func() {
		if terminalErr != nil {
			s.errMu.Lock()
			s.err = terminalErr
			s.errMu.Unlock()
		}
		s.cancel()
		_ = s.watcher.Close()
		close(s.done)
	}()

	for {
		select {
		case <-s.ctx.Done():
			if !s.closing.Load() {
				terminalErr = s.ctx.Err()
			}
			return
		case event, ok := <-s.watcher.EventChan():
			if !ok {
				if s.closing.Load() {
					return
				}
				terminalErr = ErrWatchClosed
				if reporter, supported := s.watcher.(IWatcherError); supported && reporter.WatchError() != nil {
					terminalErr = reporter.WatchError()
				}
				return
			}
			if err := invokeWatchHandler(s.ctx, s.handler, event); err != nil {
				terminalErr = err
				return
			}
		}
	}
}

func invokeWatchHandler(ctx context.Context, handler WatchHandler, event *WatchEvent) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v", ErrWatchCallbackPanic, recovered)
		}
	}()
	if err := handler(ctx, event); err != nil {
		return err
	}
	return nil
}

var _ IWatchSubscription = (*watchCallbackSubscription)(nil)

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

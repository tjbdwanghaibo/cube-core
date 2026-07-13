package misc

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

func SafeFunc(f func()) {
	if f == nil {
		return
	}
	defer func() {
		if err := recover(); err != nil {
			slog.Error("SafeFunc panic", "err", err)
		}
	}()
	f()
}

func SafeFuncWrapper(f func()) func() {
	return func() {
		SafeFunc(f)
	}
}

func SafeFuncWithRet[T any](f func() T) (t T) {
	if f == nil {
		return
	}
	defer func() {
		if err := recover(); err != nil {
			slog.Error("SafeFuncWithRet panic", "err", err)
		}
	}()
	t = f()
	return
}

func SafeFuncWithTryCount(tryCount int, f func() error) error {
	for c := 0; c < tryCount; c++ {
		err := f()
		if err == nil {
			return nil
		}
	}
	return errors.New("try count exceeded")
}

func SafeFuncWithExpireCtx(d time.Duration, f func(ctx context.Context)) {
	ctx, rls := context.WithTimeout(context.Background(), d)
	defer rls()
	SafeFunc(func() {
		f(ctx)
	})
}

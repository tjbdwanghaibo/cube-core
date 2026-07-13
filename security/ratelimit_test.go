package security

import (
	"testing"
	"time"
)

func TestRateLimiter(t *testing.T) {
	now := time.Unix(100, 0)
	limiter := NewRateLimiter(RateLimitConfig{Capacity: 2, Refill: 1, Interval: time.Second})
	limiter.now = func() time.Time { return now }
	key := RateLimitKey{OwnerID: 1, Action: 7}
	if !limiter.Allow(key) || !limiter.Allow(key) {
		t.Fatal("first two requests should pass")
	}
	if limiter.Allow(key) {
		t.Fatal("third request should be limited")
	}
	now = now.Add(time.Second)
	if !limiter.Allow(key) {
		t.Fatal("request after refill should pass")
	}
}

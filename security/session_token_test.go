package security

import (
	"errors"
	"testing"
	"time"
)

func TestSessionTokenRoundTrip(t *testing.T) {
	now := time.Unix(100, 0)
	token, err := SignSessionToken(123, "secret", time.Minute, now)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	claims, err := VerifySessionToken(token, "secret", 123, now.Add(30*time.Second))
	if err != nil {
		t.Fatalf("verify token: %v", err)
	}
	if claims.PlayerID != 123 || claims.ExpiresAt != now.Add(time.Minute).UnixMilli() || claims.Nonce == "" {
		t.Fatalf("claims = %+v", claims)
	}
}

func TestSessionTokenRejectsWrongPlayerAndExpired(t *testing.T) {
	now := time.Unix(100, 0)
	token, err := SignSessionToken(123, "secret", time.Minute, now)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	if _, err := VerifySessionToken(token, "secret", 456, now); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("wrong player err = %v", err)
	}
	if _, err := VerifySessionToken(token, "secret", 123, now.Add(time.Minute)); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("expired err = %v", err)
	}
}

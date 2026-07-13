package security

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	ErrTokenInvalid = errors.New("security: session token invalid")
	ErrTokenExpired = errors.New("security: session token expired")
)

type SessionClaims struct {
	PlayerID  int64
	ExpiresAt int64
	Nonce     string
}

func SignSessionToken(playerID int64, secret string, ttl time.Duration, now time.Time) (string, error) {
	if playerID == 0 {
		return "", fmt.Errorf("%w: player id required", ErrTokenInvalid)
	}
	if secret == "" {
		return "", fmt.Errorf("%w: secret required", ErrTokenInvalid)
	}
	if ttl <= 0 {
		ttl = time.Hour
	}
	if now.IsZero() {
		now = time.Now()
	}
	nonce, err := randomNonce()
	if err != nil {
		return "", err
	}
	claims := SessionClaims{
		PlayerID:  playerID,
		ExpiresAt: now.Add(ttl).UnixMilli(),
		Nonce:     nonce,
	}
	payload := encodePayload(claims)
	sig := sign(payload, secret)
	return base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func VerifySessionToken(token string, secret string, expectPlayerID int64, now time.Time) (SessionClaims, error) {
	if secret == "" {
		return SessionClaims{}, fmt.Errorf("%w: secret required", ErrTokenInvalid)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return SessionClaims{}, ErrTokenInvalid
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return SessionClaims{}, ErrTokenInvalid
	}
	gotSig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return SessionClaims{}, ErrTokenInvalid
	}
	payload := string(payloadBytes)
	wantSig := sign(payload, secret)
	if !hmac.Equal(gotSig, wantSig) {
		return SessionClaims{}, ErrTokenInvalid
	}
	claims, err := decodePayload(payload)
	if err != nil {
		return SessionClaims{}, err
	}
	if expectPlayerID != 0 && claims.PlayerID != expectPlayerID {
		return SessionClaims{}, ErrTokenInvalid
	}
	if now.IsZero() {
		now = time.Now()
	}
	if claims.ExpiresAt <= now.UnixMilli() {
		return SessionClaims{}, ErrTokenExpired
	}
	return claims, nil
}

func encodePayload(claims SessionClaims) string {
	return strconv.FormatInt(claims.PlayerID, 10) + ":" + strconv.FormatInt(claims.ExpiresAt, 10) + ":" + claims.Nonce
}

func decodePayload(payload string) (SessionClaims, error) {
	parts := strings.Split(payload, ":")
	if len(parts) != 3 {
		return SessionClaims{}, ErrTokenInvalid
	}
	playerID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || playerID == 0 {
		return SessionClaims{}, ErrTokenInvalid
	}
	expiresAt, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || expiresAt <= 0 {
		return SessionClaims{}, ErrTokenInvalid
	}
	if parts[2] == "" {
		return SessionClaims{}, ErrTokenInvalid
	}
	return SessionClaims{PlayerID: playerID, ExpiresAt: expiresAt, Nonce: parts[2]}, nil
}

func sign(payload string, secret string) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(payload))
	return mac.Sum(nil)
}

func randomNonce() (string, error) {
	var buf [12]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf[:]), nil
}

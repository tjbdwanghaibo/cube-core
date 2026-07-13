package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func SignPayload(raw []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(raw)
	return hex.EncodeToString(mac.Sum(nil))
}

func VerifyPayloadSignature(raw []byte, signature string, secret string) bool {
	signature = strings.TrimSpace(signature)
	signature = strings.TrimPrefix(signature, "sha256=")
	if signature == "" || secret == "" {
		return false
	}
	want, err := hex.DecodeString(SignPayload(raw, secret))
	if err != nil {
		return false
	}
	got, err := hex.DecodeString(signature)
	if err != nil {
		return false
	}
	return hmac.Equal(got, want)
}

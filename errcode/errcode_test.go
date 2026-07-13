package errcode

import (
	"errors"
	"strings"
	"testing"
)

func TestIntErrorMapsClientCodeAndPreservesCause(t *testing.T) {
	cause := errors.New("shop state missing")
	def := Define(500101, "shop.item_not_found", "商店商品不存在")

	err := Wrap(def, cause, "shop_id", 10, "item_id", 20)

	if !errors.Is(err, cause) {
		t.Fatalf("wrapped error should preserve cause")
	}
	if got := CodeOf(err); got != 500101 {
		t.Fatalf("CodeOf = %d, want 500101", got)
	}
	if got := ReasonOf(err); got != "商店商品不存在" {
		t.Fatalf("ReasonOf = %q, want business message", got)
	}
	code, reason := ClientError(err)
	if code != 500101 || reason != "商店商品不存在" {
		t.Fatalf("ClientError = (%d,%q), want (500101, business message)", code, reason)
	}
	if got := err.Error(); !strings.Contains(got, "shop_id=10") || !strings.Contains(got, "item_id=20") {
		t.Fatalf("Error() missing context fields: %q", got)
	}
}

func TestClientErrorFallsBackForPlainError(t *testing.T) {
	code, reason := ClientError(errors.New("plain failure"))
	if code != CodeInternal || reason != "server error" {
		t.Fatalf("ClientError plain = (%d,%q), want internal fallback", code, reason)
	}
}

func TestRemoteErrorPreservesClientCode(t *testing.T) {
	err := Remote(500101, "商店参数无效", "fallback")
	code, reason := ClientError(err)
	if code != 500101 || reason != "商店参数无效" {
		t.Fatalf("ClientError remote = (%d,%q), want remote code/reason", code, reason)
	}
}

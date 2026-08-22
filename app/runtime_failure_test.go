package app

import (
	"errors"
	"testing"
)

func TestRuntimeFailureFirstReportWakesShutdown(t *testing.T) {
	runtime := NewRuntimeFailure()
	first := errors.New("first")
	runtime.Fail(first)
	runtime.Fail(errors.New("second"))
	select {
	case got := <-runtime.Done():
		if !errors.Is(got, first) {
			t.Fatalf("failure=%v", got)
		}
	default:
		t.Fatal("runtime failure did not signal")
	}
	if runtime.Err() == nil {
		t.Fatal("runtime failure did not retain diagnostics")
	}
}

package obs

import (
	"strings"
	"testing"
	"time"
)

func TestRegistrySnapshot(t *testing.T) {
	reg := NewRegistry()
	reg.IncCounter("requests", Labels{"handler": "login"}, 2)
	reg.SetGauge("online", nil, 10)
	reg.ObserveDuration("cost", Labels{"handler": "login"}, 3*time.Millisecond)
	snap := reg.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("snapshot len = %d", len(snap))
	}
	foundTimer := false
	for _, metric := range snap {
		if metric.Name == "cost" {
			foundTimer = metric.Count == 1 && metric.TotalNanos == int64(3*time.Millisecond)
		}
	}
	if !foundTimer {
		t.Fatalf("timer not found in %+v", snap)
	}
}

func TestPrometheusTextDoesNotDoubleTotalSuffix(t *testing.T) {
	reg := NewRegistry()
	reg.IncCounter("bus.dispatch.total", nil, 1)
	reg.IncCounter("bus_dead_letter_total", nil, 1)

	text := string(PrometheusText(reg.Snapshot()))

	if strings.Contains(text, "total_total") {
		t.Fatalf("prometheus text should not double total suffix:\n%s", text)
	}
	if !strings.Contains(text, "bus_dispatch_total 1") || !strings.Contains(text, "bus_dead_letter_total 1") {
		t.Fatalf("prometheus text missing normalized counters:\n%s", text)
	}
}

func TestRegistryLimitsMetricSeriesCardinality(t *testing.T) {
	reg := NewRegistry(WithMaxSeriesPerMetric(2))

	reg.IncCounter("player.event.total", Labels{"player_id": "1"}, 1)
	reg.IncCounter("player.event.total", Labels{"player_id": "2"}, 1)
	reg.IncCounter("player.event.total", Labels{"player_id": "3"}, 1)

	snap := reg.Snapshot()
	series := 0
	for _, metric := range snap {
		if metric.Name == "player.event.total" {
			series++
		}
	}
	if series != 2 {
		t.Fatalf("series count = %d, want 2 snapshot=%+v", series, snap)
	}
	if dropped := reg.DroppedSeries(); dropped != 1 {
		t.Fatalf("dropped series = %d, want 1", dropped)
	}
}

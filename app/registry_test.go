package app

import (
	"testing"

	"github.com/tjbdwanghaibo/cube-core/obs"

	"github.com/spf13/viper"
)

func TestNewRegistryInstallsRuntimeObsRegistry(t *testing.T) {
	old := obs.DefaultRegistry()
	t.Cleanup(func() { obs.SetDefaultRegistry(old) })
	reg := NewRegistry(viper.New())
	metrics := MustLookup[*obs.Registry](reg, ModObs)

	obs.IncCounter("app_registry_test_total", obs.Labels{"case": "runtime"}, 1)

	found := false
	for _, metric := range metrics.Snapshot() {
		if metric.Name == "app_registry_test_total" && metric.Value == 1 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("package obs facade did not write into app runtime registry")
	}
}

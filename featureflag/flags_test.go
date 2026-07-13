package featureflag

import "testing"

func TestStore(t *testing.T) {
	store := NewStore()
	store.Set(Flag{Name: "battle.v2", Enabled: true})
	if !store.Enabled("battle.v2") {
		t.Fatal("flag should be enabled")
	}
	store.Replace([]Flag{{Name: "battle.v2", Enabled: false}})
	if store.Enabled("battle.v2") {
		t.Fatal("flag should be disabled after replace")
	}
}

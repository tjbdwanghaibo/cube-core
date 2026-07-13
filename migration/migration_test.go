package migration

import (
	"context"
	"testing"
)

type testData struct {
	version int32
	value   int
}

func (d *testData) DataVersion() int32     { return d.version }
func (d *testData) SetDataVersion(v int32) { d.version = v }

func TestRegistryRun(t *testing.T) {
	reg := NewRegistry()
	reg.MustRegister(Step{From: 0, To: 1, Apply: func(_ context.Context, data any) error {
		data.(*testData).value += 10
		return nil
	}})
	reg.MustRegister(Step{From: 1, To: 2, Apply: func(_ context.Context, data any) error {
		data.(*testData).value *= 2
		return nil
	}})
	data := &testData{}
	if err := reg.Run(context.Background(), data, 2); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if data.version != 2 || data.value != 20 {
		t.Fatalf("data = %+v", data)
	}
}

func TestDAORegistryMigratesRawPayloadByCollection(t *testing.T) {
	reg := NewDAORegistry()
	reg.MustRegisterDAO(DAOStep{
		Collection: "players",
		From:       1,
		To:         2,
		Apply: func(_ context.Context, raw []byte) ([]byte, error) {
			return append(raw, []byte(":v2")...), nil
		},
	})
	reg.MustRegisterDAO(DAOStep{
		Collection: "players",
		From:       2,
		To:         3,
		Apply: func(_ context.Context, raw []byte) ([]byte, error) {
			return append(raw, []byte(":v3")...), nil
		},
	})

	raw, version, err := reg.MigrateDAO(context.Background(), "players", []byte("doc"), 1, 3)
	if err != nil {
		t.Fatalf("MigrateDAO: %v", err)
	}
	if string(raw) != "doc:v2:v3" || version != 3 {
		t.Fatalf("raw=%q version=%d", raw, version)
	}
}

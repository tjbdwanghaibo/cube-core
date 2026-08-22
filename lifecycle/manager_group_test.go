package lifecycle

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

type groupTestManager struct {
	name       string
	events     *[]string
	initErr    error
	startErr   error
	panicStart bool
	panicStop  bool
}

func (m *groupTestManager) Name() string { return m.name }
func (m *groupTestManager) Init(string) error {
	*m.events = append(*m.events, "init:"+m.name)
	return m.initErr
}
func (m *groupTestManager) Start() error {
	*m.events = append(*m.events, "start:"+m.name)
	if m.panicStart {
		panic("start " + m.name)
	}
	return m.startErr
}
func (m *groupTestManager) Stop(reason string) {
	*m.events = append(*m.events, "stop:"+m.name+":"+reason)
	if m.panicStop {
		panic("stop " + m.name)
	}
}

func TestManagerGroupLifecycleAndIdempotentStop(t *testing.T) {
	events := []string{}
	group, err := NewManagerGroup[string, string](
		&groupTestManager{name: "one", events: &events},
		&groupTestManager{name: "two", events: &events},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Init("ctx", "init_rollback"); err != nil {
		t.Fatal(err)
	}
	if err := group.Start("start_rollback"); err != nil {
		t.Fatal(err)
	}
	if err := group.Stop("done"); err != nil {
		t.Fatal(err)
	}
	if err := group.Stop("duplicate"); err != nil {
		t.Fatal(err)
	}
	want := []string{"init:one", "init:two", "start:one", "start:two", "stop:two:done", "stop:one:done"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestManagerGroupInitFailureRollsBackInitializedOnly(t *testing.T) {
	events := []string{}
	boom := errors.New("boom")
	group, err := NewManagerGroup[string, string](
		&groupTestManager{name: "one", events: &events},
		&groupTestManager{name: "two", events: &events, initErr: boom},
		&groupTestManager{name: "three", events: &events},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = group.Init("ctx", "rollback")
	if !errors.Is(err, boom) {
		t.Fatalf("Init error = %v, want boom", err)
	}
	want := []string{"init:one", "init:two", "stop:one:rollback"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestManagerGroupStartFailureContainsPanicAndContinuesCleanup(t *testing.T) {
	events := []string{}
	group, err := NewManagerGroup[string, string](
		&groupTestManager{name: "one", events: &events},
		&groupTestManager{name: "two", events: &events, panicStart: true},
		&groupTestManager{name: "three", events: &events, panicStop: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Init("ctx", "rollback"); err != nil {
		t.Fatal(err)
	}
	err = group.Start("rollback")
	if err == nil || !strings.Contains(err.Error(), "start manager two") || !strings.Contains(err.Error(), "stop manager three") {
		t.Fatalf("Start error = %v", err)
	}
	want := []string{"init:one", "init:two", "init:three", "start:one", "start:two", "stop:three:rollback", "stop:two:rollback", "stop:one:rollback"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestNewManagerGroupRejectsInvalidNames(t *testing.T) {
	events := []string{}
	if _, err := NewManagerGroup[string, string](&groupTestManager{events: &events}); !errors.Is(err, ErrManagerNameEmpty) {
		t.Fatalf("empty name error = %v", err)
	}
	if _, err := NewManagerGroup[string, string](
		&groupTestManager{name: "same", events: &events},
		&groupTestManager{name: "same", events: &events},
	); !errors.Is(err, ErrManagerDuplicate) {
		t.Fatalf("duplicate name error = %v", err)
	}
}

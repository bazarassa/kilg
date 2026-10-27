package jobs

import (
	"errors"
	"testing"

	"github.com/kafka-llm-gateway/gateway/internal/protocol"
)

func TestMemoryStoreCreateErrExists(t *testing.T) {
	s := NewMemoryStore()
	if err := s.Create(&Job{ID: "a"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	err := s.Create(&Job{ID: "a"})
	if !errors.Is(err, ErrExists) {
		t.Errorf("err = %v, want ErrExists", err)
	}
}

func TestMemoryStoreGetReturnsCopy(t *testing.T) {
	s := NewMemoryStore()
	_ = s.Create(&Job{ID: "a", Status: "queued"})

	cp, ok := s.Get("a")
	if !ok {
		t.Fatal("Get: not found")
	}
	cp.Status = "mutated"

	orig, _ := s.Get("a")
	if orig.Status != "queued" {
		t.Errorf("store mutated through copy: %q", orig.Status)
	}
}

func TestMemoryStoreListAndForget(t *testing.T) {
	s := NewMemoryStore()
	_ = s.Create(&Job{ID: "a"})
	_ = s.Create(&Job{ID: "b"})

	list := s.List()
	if len(list) != 2 || list[0].ID != "a" || list[1].ID != "b" {
		t.Fatalf("list order = %v", list)
	}

	s.Forget("a")
	list = s.List()
	if len(list) != 1 || list[0].ID != "b" {
		t.Fatalf("list after forget = %v", list)
	}

	// Forget of unknown id must not panic.
	s.Forget("unknown")
}

func TestManagerApplyFailedTerminal(t *testing.T) {
	m := NewManager(NewMemoryStore())
	m.Ensure("REQF", "heavy", "m")

	payload := protocol.FailedPayload{
		Error:     "provider exploded",
		Retryable: false,
		Attempt:   2,
	}
	ev, _ := protocol.NewEvent("REQF", 2, protocol.TypeFailed, payload)
	if !m.Apply(ev) {
		t.Fatal("failed event must be terminal")
	}

	j, _ := m.Store().Get("REQF")
	if j.Status != protocol.StatusFailed {
		t.Errorf("status = %q", j.Status)
	}
	if j.Error != "provider exploded" {
		t.Errorf("error = %q", j.Error)
	}
	if j.CompletedAt.IsZero() {
		t.Error("completed_at must be set on failure")
	}
}

func TestManagerApplyCancelledTerminal(t *testing.T) {
	m := NewManager(NewMemoryStore())
	m.Ensure("REQC", "heavy", "m")

	ev, _ := protocol.NewEvent("REQC", 1, protocol.TypeCancelled,
		map[string]string{})
	if !m.Apply(ev) {
		t.Fatal("cancelled event must be terminal")
	}
	j, _ := m.Store().Get("REQC")
	if j.Status != protocol.StatusCancelled {
		t.Errorf("status = %q", j.Status)
	}
}

func TestManagerApplyUnknownJobIsNoop(t *testing.T) {
	m := NewManager(NewMemoryStore())

	ev, _ := protocol.NewEvent("NOPE", 1, protocol.TypeStarted,
		map[string]string{})
	if m.Apply(ev) {
		t.Error("unknown job must not be terminal")
	}
	if _, ok := m.Store().Get("NOPE"); ok {
		t.Error("unknown job must not be created by Apply")
	}
}

func TestManagerEnsureIsIdempotent(t *testing.T) {
	m := NewManager(NewMemoryStore())
	m.Ensure("REQE", "heavy", "m1")

	// Second call must keep the original provider/model.
	m.Ensure("REQE", "other", "m2")

	j, ok := m.Store().Get("REQE")
	if !ok {
		t.Fatal("job not found")
	}
	if j.Provider != "heavy" || j.Model != "m1" {
		t.Errorf("job = %+v, want original provider/model", j)
	}
	if j.Status != protocol.StatusQueued {
		t.Errorf("status = %q", j.Status)
	}
}

func TestManagerApplyCountsEvents(t *testing.T) {
	m := NewManager(NewMemoryStore())
	m.Ensure("REQN", "heavy", "m")

	m.Apply(mustEvent("REQN", 0, protocol.TypeQueued))
	m.Apply(mustEvent("REQN", 1, protocol.TypeStarted))

	j, _ := m.Store().Get("REQN")
	if j.Events != 2 {
		t.Errorf("events = %d, want 2", j.Events)
	}
}

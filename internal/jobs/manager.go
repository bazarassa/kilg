// Package jobs tracks the state of LLM generation jobs.
//
// Kafka is the source of truth; the in-memory store is a fast view that is
// rebuilt from Kafka events as they flow through the gateway and can be
// reconstructed after a restart by replaying the events topic.
package jobs

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/kafka-llm-gateway/gateway/internal/protocol"
)

// Job is the tracked state of one generation.
type Job struct {
	ID           string
	Provider     string
	Model        string
	Status       string
	Sequence     uint64
	Events       uint64
	CreatedAt    time.Time
	UpdatedAt    time.Time
	StartedAt    time.Time
	CompletedAt  time.Time
	FinishReason string
	Error        string
	Content      string
	Reasoning    string
	ReasoningAvailable bool
	Usage        json.RawMessage
}

// Store is the job state interface.
type Store interface {
	Create(job *Job) error
	Get(id string) (*Job, bool)
	Update(id string, fn func(*Job))
	List() []*Job
	Forget(id string)
}

// MemoryStore is a mutex-protected in-memory job store.
type MemoryStore struct {
	mu    sync.RWMutex
	jobs  map[string]*Job
	order []string
}

// NewMemoryStore creates an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{jobs: map[string]*Job{}}
}

// Create adds a new job. It fails if the id already exists.
func (s *MemoryStore) Create(job *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[job.ID]; ok {
		return ErrExists
	}
	s.jobs[job.ID] = job
	s.order = append(s.order, job.ID)
	return nil
}

// Get returns a copy of the job.
func (s *MemoryStore) Get(id string) (*Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	cp := *j
	return &cp, true
}

// Update mutates the job under the lock.
func (s *MemoryStore) Update(id string, fn func(*Job)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return
	}
	fn(j)
	j.UpdatedAt = time.Now().UTC()
}

// List returns copies of all jobs, oldest first.
func (s *MemoryStore) List() []*Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Job, 0, len(s.order))
	for _, id := range s.order {
		cp := *s.jobs[id]
		out = append(out, &cp)
	}
	return out
}

// Forget removes a job.
func (s *MemoryStore) Forget(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[id]; !ok {
		return
	}
	delete(s.jobs, id)
	for i, v := range s.order {
		if v == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

// Manager applies protocol events to the job store.
type Manager struct {
	store Store
}

// NewManager wraps a store.
func NewManager(store Store) *Manager {
	return &Manager{store: store}
}

// Store exposes the underlying store.
func (m *Manager) Store() Store { return m.store }

// Update mutates a job.
func (m *Manager) Update(id string, fn func(*Job)) { m.store.Update(id, fn) }

// Ensure creates the job if it does not exist yet.
func (m *Manager) Ensure(id, provider, model string) {
	m.store.Update(id, func(*Job) {})
	if _, ok := m.store.Get(id); !ok {
		now := time.Now().UTC()
		_ = m.store.Create(&Job{
			ID:        id,
			Provider:  provider,
			Model:     model,
			Status:    protocol.StatusQueued,
			CreatedAt: now,
			UpdatedAt: now,
		})
	}
}

// Apply updates job state from an event. It returns true when the event
// moved the job to a terminal state.
func (m *Manager) Apply(ev protocol.Event) bool {
	m.store.Update(ev.RequestID, func(j *Job) {
		if j.Provider == "" {
			j.Provider = ev.Provider
		}
		if j.Model == "" {
			j.Model = ev.Model
		}
		if ev.Sequence > j.Sequence {
			j.Sequence = ev.Sequence
		}
		j.Events++
		switch ev.Type {
		case protocol.TypeQueued:
			j.Status = protocol.StatusQueued
		case protocol.TypeStarted:
			j.Status = protocol.StatusRunning
			if j.StartedAt.IsZero() {
				j.StartedAt = ev.Timestamp
			}
		case protocol.TypeCompleted:
			j.Status = protocol.StatusCompleted
			j.CompletedAt = ev.Timestamp
			var p protocol.CompletedPayload
			if json.Unmarshal(ev.Payload, &p) == nil {
				j.FinishReason = p.FinishReason
				j.Content = p.Content
				j.Reasoning = p.Reasoning
				j.ReasoningAvailable = p.ReasoningAvailable
				j.Usage = p.Usage
			}
		case protocol.TypeFailed:
			j.Status = protocol.StatusFailed
			j.CompletedAt = ev.Timestamp
			var p protocol.FailedPayload
			if json.Unmarshal(ev.Payload, &p) == nil {
				j.Error = p.Error
			}
		case protocol.TypeCancelled:
			j.Status = protocol.StatusCancelled
			j.CompletedAt = ev.Timestamp
		}
	})
	j, ok := m.store.Get(ev.RequestID)
	if !ok {
		return false
	}
	switch j.Status {
	case protocol.StatusCompleted, protocol.StatusFailed, protocol.StatusCancelled:
		return true
	}
	return false
}

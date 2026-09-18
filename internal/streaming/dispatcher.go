package streaming

import (
	"sync"

	"github.com/kafka-llm-gateway/gateway/internal/protocol"
)

// Stream is a live SSE subscriber for one request.
type Stream struct {
	RequestID string
	// Ch receives events destined for this stream.
	Ch chan protocol.Event
	// Closed is closed when the client disconnects.
	Closed chan struct{}
	closeOnce sync.Once
}

// Close marks the stream as closed.
func (s *Stream) Close() {
	s.closeOnce.Do(func() { close(s.Closed) })
}

// Dispatcher routes events from Kafka to live SSE streams.
//
// Events for requests without a live stream are dropped here; they remain
// durable in Kafka and are served on replay via the replay endpoint.
type Dispatcher struct {
	mu       sync.RWMutex
	streams  map[string]*Stream
	buffer   int
}

// NewDispatcher creates a dispatcher with a per-stream buffer size.
func NewDispatcher(buffer int) *Dispatcher {
	if buffer <= 0 {
		buffer = 1024
	}
	return &Dispatcher{streams: map[string]*Stream{}, buffer: buffer}
}

// Register creates and returns a stream for the request.
func (d *Dispatcher) Register(requestID string) *Stream {
	s := &Stream{
		RequestID: requestID,
		Ch:        make(chan protocol.Event, d.buffer),
		Closed:    make(chan struct{}),
	}
	d.mu.Lock()
	d.streams[requestID] = s
	d.mu.Unlock()
	return s
}

// Unregister removes a stream.
func (d *Dispatcher) Unregister(requestID string) {
	d.mu.Lock()
	if s, ok := d.streams[requestID]; ok {
		s.Close()
		delete(d.streams, requestID)
	}
	d.mu.Unlock()
}

// Dispatch delivers an event to the live stream, if any.
// It returns true when a live stream received the event.
func (d *Dispatcher) Dispatch(ev protocol.Event) bool {
	d.mu.RLock()
	s, ok := d.streams[ev.RequestID]
	d.mu.RUnlock()
	if !ok {
		return false
	}
	select {
	case s.Ch <- ev:
		return true
	default:
		// Buffer full (slow client): drop the live delivery. The event is
		// durable in Kafka and will be served on replay/reconnect.
		return false
	}
}

// Get returns the live stream for a request, if any.
func (d *Dispatcher) Get(requestID string) (*Stream, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	s, ok := d.streams[requestID]
	return s, ok
}

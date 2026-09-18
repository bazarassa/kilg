package kafka

import (
	"context"
	"sync"
)

// FakeProducer is an in-memory EventProducer for tests.
type FakeProducer struct {
	mu      sync.Mutex
	Records map[string][]Record
}

// NewFakeProducer creates an empty fake producer.
func NewFakeProducer() *FakeProducer {
	return &FakeProducer{Records: map[string][]Record{}}
}

// Publish stores a record in memory.
func (f *FakeProducer) Publish(_ context.Context, topic, key string, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Records[topic] = append(f.Records[topic], Record{Topic: topic, Key: []byte(key), Value: value})
	return nil
}

// Close is a no-op.
func (f *FakeProducer) Close() error { return nil }

// RecordsFor returns the records for a topic.
func (f *FakeProducer) RecordsFor(topic string) []Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Record, len(f.Records[topic]))
	copy(out, f.Records[topic])
	return out
}

// FakeReader is an in-memory Reader for tests.
type FakeReader struct {
	mu      sync.Mutex
	Records map[string][]Record
}

// NewFakeReader creates an empty fake reader.
func NewFakeReader() *FakeReader {
	return &FakeReader{Records: map[string][]Record{}}
}

// SetRecords sets the records for a key.
func (f *FakeReader) SetRecords(key string, recs []Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Records[key] = recs
}

// ReadAll returns the records for a key.
func (f *FakeReader) ReadAll(_ context.Context, key string) ([]Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Record, len(f.Records[key]))
	copy(out, f.Records[key])
	return out, nil
}

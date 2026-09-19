package kafka

import (
	"context"
	"testing"
)

func TestFakeProducer(t *testing.T) {
	prod := NewFakeProducer()

	ctx := context.Background()
	if err := prod.Publish(ctx, "topic1", "key1", []byte("value1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	recs := prod.RecordsFor("topic1")
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1", len(recs))
	}

	if string(recs[0].Key) != "key1" {
		t.Errorf("key = %q", recs[0].Key)
	}
	if string(recs[0].Value) != "value1" {
		t.Errorf("value = %q", recs[0].Value)
	}
}

func TestFakeProducerClose(t *testing.T) {
	prod := NewFakeProducer()
	if err := prod.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestFakeReader(t *testing.T) {
	reader := NewFakeReader()

	recs := []Record{
		{Topic: "t1", Key: []byte("k1"), Value: []byte("v1")},
		{Topic: "t1", Key: []byte("k2"), Value: []byte("v2")},
	}
	reader.SetRecords("key1", recs)

	ctx := context.Background()
	got, err := reader.ReadAll(ctx, "key1")
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("records = %d, want 2", len(got))
	}
}

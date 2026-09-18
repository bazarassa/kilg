package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Reader reads durable records for a key from the event log.
type Reader interface {
	ReadAll(ctx context.Context, key string) ([]Record, error)
}

// ReplayReader reads a topic from the beginning and yields records matching
// a key. It is used to serve replay (Last-Event-ID) and job state recovery.
type ReplayReader struct {
	brokers []string
	topic   string
	rewrite string
}

// NewReplayReader builds a replay reader.
func NewReplayReader(brokers []string, topic string) *ReplayReader {
	return &ReplayReader{brokers: brokers, topic: topic}
}

// SetRewrite sets the advertised-address rewrite spec.
func (r *ReplayReader) SetRewrite(spec string) { r.rewrite = spec }

// ReadAll reads all records for the key from the start of the topic.
// It returns when the current end offset is reached.
func (r *ReplayReader) ReadAll(ctx context.Context, key string) ([]Record, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_4_0_0
	applyRewrite(cfg, r.rewrite)
	client, err := sarama.NewClient(r.brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("replay client: %w", err)
	}
	defer client.Close()

	partitions, err := client.Partitions(r.topic)
	if err != nil {
		return nil, fmt.Errorf("replay partitions: %w", err)
	}

	var mu sync.Mutex
	var out []Record
	var wg sync.WaitGroup

	for _, p := range partitions {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			recs, err := r.readPartition(ctx, client, p, key)
			if err != nil {
				return
			}
			mu.Lock()
			out = append(out, recs...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out, nil
}

func (r *ReplayReader) readPartition(ctx context.Context, client sarama.Client, partition int32, key string) ([]Record, error) {
	begin, err := client.GetOffset(r.topic, partition, sarama.OffsetOldest)
	if err != nil {
		return nil, err
	}
	end, err := client.GetOffset(r.topic, partition, sarama.OffsetNewest)
	if err != nil {
		return nil, err
	}

	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(r.topic, partition, begin)
	if err != nil {
		return nil, err
	}
	defer pc.Close()

	var out []Record
	// Short idle timeout: once we stop receiving messages while still below
	// the end offset, the partition is caught up.
	idle := 300 * time.Millisecond
	timeout := time.NewTimer(idle)
	defer timeout.Stop()
	for begin < end {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-timeout.C:
			// No new data; the partition is caught up.
			return out, nil
		case msg, ok := <-pc.Messages():
			if !ok {
				return out, nil
			}
			begin = msg.Offset + 1
			timeout.Reset(idle)
			if string(msg.Key) == key {
				out = append(out, Record{
					Topic:     msg.Topic,
					Key:       msg.Key,
					Value:     msg.Value,
					Offset:    msg.Offset,
					Partition: msg.Partition,
				})
			}
		}
	}
	return out, nil
}

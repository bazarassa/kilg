// Package kafka provides the Kafka producer/consumer abstraction used by the
// gateway and worker. The real implementation uses sarama; tests can swap in
// in-memory fakes via the EventProducer / EventConsumer interfaces.
package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
)

// EventProducer publishes records to Kafka.
type EventProducer interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
	Close() error
}

// Record is a consumed Kafka record.
type Record struct {
	Topic     string
	Key       []byte
	Value     []byte
	Offset    int64
	Partition int32
}

// EventConsumer consumes records from a topic.
type EventConsumer interface {
	// Consume blocks until ctx is done, invoking handler for each record.
	Consume(ctx context.Context, handler func(Record) error) error
	Close() error
}

// Producer is a sarama-based async producer configured for durability:
// acks=all, idempotent, with retries.
type Producer struct {
	p sarama.AsyncProducer
}

// NewProducer builds a durable async producer.
func NewProducer(brokers []string) (*Producer, error) {
	return NewProducerWithRewrite(brokers, "")
}

// NewProducerWithRewrite builds a durable async producer with an optional
// advertised-address rewrite spec.
func NewProducerWithRewrite(brokers []string, rewrite string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_4_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 10
	cfg.Producer.Retry.Backoff = 500 * time.Millisecond
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.Idempotent = true
	cfg.Producer.Timeout = 30 * time.Second
	cfg.Net.MaxOpenRequests = 1 // required for idempotent producer
	cfg.Net.DialTimeout = 10 * time.Second
	cfg.Net.ReadTimeout = 30 * time.Second
	cfg.Net.WriteTimeout = 30 * time.Second
	applyRewrite(cfg, rewrite)

	p, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Producer{p: p}, nil
}

// Publish sends a record and waits for broker acknowledgement.
// The event is considered stored only after this returns nil.
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
	p.p.Input() <- msg
	select {
	case resp := <-p.p.Successes():
		_ = resp
		return nil
	case resp := <-p.p.Errors():
		return fmt.Errorf("kafka publish %s: %w", topic, resp.Err)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close flushes and stops the producer.
func (p *Producer) Close() error {
	return p.p.Close()
}

// Consumer is a sarama-based consumer-group consumer for one topic.
type Consumer struct {
	brokers []string
	group   string
	topic   string
	start   string
	rewrite string
	log     *slog.Logger
}

// NewConsumer builds a consumer for one topic in a consumer group.
func NewConsumer(brokers []string, group, topic, start string) *Consumer {
	return &Consumer{
		brokers: brokers,
		group:   group,
		topic:   topic,
		start:   start,
		log:     slog.Default(),
	}
}

// SetRewrite sets the advertised-address rewrite spec.
func (c *Consumer) SetRewrite(spec string) { c.rewrite = spec }

// Consume blocks until ctx is done, invoking handler for each record.
// Offsets are committed after the handler succeeds. Broker-level errors
// trigger a reconnect with backoff; handler errors are logged and the
// message is skipped (the handler is responsible for DLQ routing).
func (c *Consumer) Consume(ctx context.Context, handler func(Record) error) error {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_4_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	if c.start == "earliest" {
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	cfg.Consumer.Offsets.CommitInterval = 500 * time.Millisecond
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	applyRewrite(cfg, c.rewrite)

	client, err := sarama.NewConsumerGroup(c.brokers, c.group, cfg)
	if err != nil {
		return fmt.Errorf("kafka consumer group: %w", err)
	}
	defer client.Close()

	for ctx.Err() == nil {
		err := c.consumeOnce(ctx, client, handler)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			c.log.Error("kafka consume session ended, reconnecting", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
		}
	}
	return nil
}

func (c *Consumer) consumeOnce(ctx context.Context, client sarama.ConsumerGroup, handler func(Record) error) error {
	return client.Consume(ctx, []string{c.topic}, &sessionHandler{
		log: c.log,
		handler: func(sess sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) error {
			rec := Record{
				Topic:     msg.Topic,
				Key:       msg.Key,
				Value:     msg.Value,
				Offset:    msg.Offset,
				Partition: msg.Partition,
			}
			if err := handler(rec); err != nil {
				// Poison message: log, skip, keep consuming.
				c.log.Error("handler error, skipping message",
					"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "err", err)
			}
			sess.MarkMessage(msg, "")
			return nil
		},
	})
}

type sessionHandler struct {
	log     *slog.Logger
	handler func(sarama.ConsumerGroupSession, *sarama.ConsumerMessage) error
}

func (h *sessionHandler) Setup(sarama.ConsumerGroupSession) error    { return nil }
func (h *sessionHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *sessionHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-sess.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if err := h.handler(sess, msg); err != nil {
				return err
			}
		}
	}
}

// Close is a no-op for the consumer (the client is closed per Consume call).
func (c *Consumer) Close() error { return nil }

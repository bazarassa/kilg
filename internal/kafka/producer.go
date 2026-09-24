// Package kafka provides the Kafka producer/consumer abstraction used by
// the gateway and worker.
//
// Важный принцип этого пакета:
//
//   Kafka message -> handler -> успешное завершение обработки -> MarkMessage
//
// Мы НЕ считаем Kafka-сообщение обработанным в момент его чтения.
//
// Для worker это особенно важно:
//
//   1. прочитали llm.requests;
//   2. вызвали LLM;
//   3. записали llm.completed / llm.failed;
//   4. только после этого MarkMessage.
//
// Таким образом мы получаем at-least-once semantics.
//
// Возможный duplicate после crash/rebalance всё равно возможен:
//
//   LLM успешно ответил
//       |
//       +--> llm.completed записан
//       |
//       X worker умер до commit offset
//       |
//       +--> Kafka отдаст request повторно
//
// Поэтому request_id должен использоваться как correlation/idempotency key.
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

// Record is a Kafka record exposed to application code.
//
// Мы намеренно не передаём sarama.ConsumerMessage наружу.
//
// Это позволяет остальному приложению не зависеть от конкретной Kafka
// библиотеки.
type Record struct {
	Topic     string
	Key       []byte
	Value     []byte
	Offset    int64
	Partition int32
}

// EventConsumer consumes records from a Kafka consumer group.
//
// Context, переданный handler'у, является session context конкретной
// ConsumerGroup session.
//
// Это критически важно:
//
// если Kafka начинает rebalance, session.Context() отменяется.
//
// Worker должен передать этот context дальше:
//
// Kafka session
//     -> worker handler
//         -> process()
//             -> HTTP request to LLM
//
// В результате rebalance может корректно отменить HTTP request.
type EventConsumer interface {
	Consume(
		ctx context.Context,
		handler func(context.Context, Record) error,
	) error

	Close() error
}

// Producer is a sarama-based async producer configured for durability.
type Producer struct {
	p sarama.AsyncProducer
}

// NewProducer builds a durable async producer.
func NewProducer(brokers []string) (*Producer, error) {
	return NewProducerWithRewrite(brokers, "")
}

// NewProducerWithRewrite builds a durable async producer with optional
// advertised-address rewrite.
func NewProducerWithRewrite(brokers []string, rewrite string) (*Producer, error) {
	cfg := sarama.NewConfig()

	// В текущем проекте Kafka используется с API уровня 2.4.
	cfg.Version = sarama.V2_4_0_0

	// Ждём подтверждение от всех ISR.
	//
	// Для replication=1 это фактически ACK единственной реплики,
	// но при дальнейшем увеличении replication semantics останется
	// правильной.
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	// Retry публикации.
	cfg.Producer.Retry.Max = 10
	cfg.Producer.Retry.Backoff = 500 * time.Millisecond

	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	// Idempotent producer защищает от части producer-side duplicates.
	cfg.Producer.Idempotent = true

	cfg.Producer.Timeout = 30 * time.Second

	// Обязательное условие Sarama для idempotent producer.
	cfg.Net.MaxOpenRequests = 1

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

// Publish sends a record and waits until Sarama receives either:
//
//   - producer success;
//   - producer error;
//   - context cancellation.
//
// Поэтому для вызывающего кода:
//
//     Publish(...) == nil
//
// означает, что Kafka producer получил подтверждение успешной публикации.
//
// Именно после такого результата worker может считать результат durable.
func (p *Producer) Publish(
	ctx context.Context,
	topic string,
	key string,
	value []byte,
) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	// ВАЖНО:
	//
	// Не используем отдельную goroutine для Input().
	// AsyncProducer имеет собственный input channel.
	select {
	case p.p.Input() <- msg:
	case <-ctx.Done():
		return ctx.Err()
	}

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

// Consumer is a Sarama-based consumer-group consumer for one topic.
type Consumer struct {
	brokers []string
	group   string
	topic   string
	start   string
	rewrite string
	log     *slog.Logger
}

// NewConsumer builds a consumer for one topic in a consumer group.
func NewConsumer(
	brokers []string,
	group string,
	topic string,
	start string,
) *Consumer {
	return &Consumer{
		brokers: brokers,
		group:   group,
		topic:   topic,
		start:   start,
		log:     slog.Default(),
	}
}

// SetRewrite sets advertised-address rewrite rules.
//
// Например:
//
// REDACTED:9093=REDACTED:9092
//
// Kafka bootstrap может вернуть broker address :9093,
// хотя из контейнера доступен :9092.
func (c *Consumer) SetRewrite(spec string) {
	c.rewrite = spec
}

// Consume starts consuming records.
//
// Важный lifecycle:
//
//   Consume()
//      |
//      +--> Kafka ConsumerGroup session
//              |
//              +--> ConsumeClaim()
//                      |
//                      +--> handler(session.Context(), record)
//                              |
//                              +--> nil
//                                      |
//                                      +--> MarkMessage()
//
// Если handler вернул ошибку:
//
//     handler()
//         |
//         +--> error
//                 |
//                 +--> NO MarkMessage
//                 |
//                 +--> session завершается
//                 |
//                 +--> Kafka message будет доступен повторно
//
// Это сознательная at-least-once semantics.
func (c *Consumer) Consume(
	ctx context.Context,
	handler func(context.Context, Record) error,
) error {
	cfg := sarama.NewConfig()

	cfg.Version = sarama.V2_4_0_0

	// Initial offset применяется только если у consumer group
	// ещё нет committed offset.
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	if c.start == "earliest" {
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	// Commit происходит асинхронно после MarkMessage.
	//
	// MarkMessage сам по себе не означает немедленный broker commit.
	// Это нормально: после crash сообщение может быть обработано повторно.
	cfg.Consumer.Offsets.AutoCommit.Enable = true
	cfg.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	// RoundRobin хорошо подходит для трёх partitions.
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	// Для worker очень важно не создавать большую очередь сообщений
	// внутри consumer.
	//
	// Мы хотим контролировать backpressure на уровне worker.
	cfg.ChannelBufferSize = 1

	// Время fetch.
	cfg.Consumer.MaxWaitTime = 500 * time.Millisecond

	// Это НЕ timeout LLM processing.
	//
	// Worker может обрабатывать сообщение дольше этого значения.
	// Здесь только ограничивается время, в течение которого Sarama
	// считает нормальным отсутствие передачи сообщений из broker
	// в application channel.
	cfg.Consumer.MaxProcessingTime = 1 * time.Second

	// Обязательно оставляем heartbeat отдельно от application processing.
	cfg.Consumer.Group.Session.Timeout = 30 * time.Second
	cfg.Consumer.Group.Heartbeat.Interval = 10 * time.Second

	// Rebalance должен завершаться быстро.
	//
	// Для текущего MVP LLM отвечает быстро, поэтому стандартного
	// значения достаточно.
	cfg.Consumer.Group.Rebalance.Timeout = 60 * time.Second

	applyRewrite(cfg, c.rewrite)

	client, err := sarama.NewConsumerGroup(
		c.brokers,
		c.group,
		cfg,
	)
	if err != nil {
		return fmt.Errorf("kafka consumer group: %w", err)
	}

	defer func() {
		if err := client.Close(); err != nil {
			c.log.Error(
				"kafka consumer group close failed",
				"err",
				err,
			)
		}
	}()

	for ctx.Err() == nil {
		err := c.consumeOnce(ctx, client, handler)

		if ctx.Err() != nil {
			return nil
		}

		if err == nil {
			// Sarama Consume может штатно завершить session из-за
			// rebalance. В этом случае сразу создаём новую session.
			continue
		}

		// Не делаем log.Fatal / os.Exit внутри consumer.
		//
		// Consumer должен уметь восстановиться самостоятельно.
		c.log.Error(
			"kafka consume session ended, reconnecting",
			"err",
			err,
		)

		timer := time.NewTimer(2 * time.Second)

		select {
		case <-ctx.Done():
			timer.Stop()
			return nil

		case <-timer.C:
		}
	}

	return nil
}

// consumeOnce runs one Sarama consumer-group session.
func (c *Consumer) consumeOnce(
	ctx context.Context,
	client sarama.ConsumerGroup,
	handler func(context.Context, Record) error,
) error {
	return client.Consume(
		ctx,
		[]string{c.topic},
		&sessionHandler{
			log: c.log,

			handler: func(
				session sarama.ConsumerGroupSession,
				msg *sarama.ConsumerMessage,
			) error {
				rec := Record{
					Topic:     msg.Topic,
					Key:       msg.Key,
					Value:     msg.Value,
					Offset:    msg.Offset,
					Partition: msg.Partition,
				}

				// Используем session.Context(), а не родительский ctx.
				//
				// Это принципиально:
				//
				// parent ctx:
				//     worker process жив
				//
				// session ctx:
				//     worker всё ещё владеет конкретной partition.
				//
				// Если partition ушла при rebalance, session.Context()
				// будет отменён.
				if err := handler(session.Context(), rec); err != nil {
					c.log.Error(
						"message processing failed; offset will NOT be marked",
						"topic", msg.Topic,
						"partition", msg.Partition,
						"offset", msg.Offset,
						"err", err,
					)

					return err
				}

				// КРИТИЧЕСКОЕ МЕСТО.
				//
				// Message считается успешно обработанным только после
				// того, как handler вернул nil.
				//
				// Для worker это означает:
				//
				// HTTP LLM -> completed -> Kafka ACK -> MarkMessage.
				session.MarkMessage(msg, "")

				return nil
			},
		},
	)
}

type sessionHandler struct {
	log     *slog.Logger
	handler func(
		sarama.ConsumerGroupSession,
		*sarama.ConsumerMessage,
	) error
}

func (h *sessionHandler) Setup(
	sarama.ConsumerGroupSession,
) error {
	return nil
}

func (h *sessionHandler) Cleanup(
	sarama.ConsumerGroupSession,
) error {
	return nil
}

// ConsumeClaim must remain synchronous.
//
// Sarama itself invokes ConsumeClaim in a goroutine for each partition.
// Поэтому создавать ещё одну goroutine для каждого message здесь НЕ нужно.
//
// Если вынести обработку сообщения в goroutine:
//
//     go handler(msg)
//     return nil
//
// Sarama сможет завершить session/rebalance и commit offsets,
// пока goroutine ещё работает.
//
// Именно это было проблемой предыдущей реализации worker.
func (h *sessionHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case <-session.Context().Done():
			// При rebalance/shutdown прекращаем работу текущей claim.
			return nil

		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			if err := h.handler(session, msg); err != nil {
				// Не MarkMessage.
				//
				// Возвращаем ошибку, чтобы Consume session завершилась.
				// Следующая session получит непрокоммиченный message.
				return err
			}
		}
	}
}

// Close is intentionally a no-op.
//
// Sarama consumer group создаёт client внутри Consume() и закрывает
// его после завершения Consume().
func (c *Consumer) Close() error {
	return nil
}

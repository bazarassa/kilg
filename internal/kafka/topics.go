package kafka

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
)

// Topics is the canonical Kafka topology.
type Topics struct {
	Requests  string
	Events    string
	Completed string
	Failed    string
	DLQ       string
}

// All returns every topic in creation order.
func (t Topics) All() []string {
	return []string{t.Requests, t.Events, t.Completed, t.Failed, t.DLQ}
}

// EnsureTopics creates the topics if they do not exist.
func EnsureTopics(ctx context.Context, brokers []string, t Topics, partitions, replication int) error {
	return EnsureTopicsWithRewrite(ctx, brokers, t, partitions, replication, "")
}

// EnsureTopicsWithRewrite creates topics with an optional rewrite spec.
func EnsureTopicsWithRewrite(ctx context.Context, brokers []string, t Topics, partitions, replication int, rewrite string) error {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_4_0_0
	applyRewrite(cfg, rewrite)
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		return fmt.Errorf("kafka admin: %w", err)
	}
	defer admin.Close()

	existing, err := admin.ListTopics()
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}

	for _, name := range t.All() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, ok := existing[name]; ok {
			continue
		}
		if err := admin.CreateTopic(name, &sarama.TopicDetail{
			NumPartitions:     int32(partitions),
			ReplicationFactor: int16(replication),
		}, false); err != nil && err != sarama.ErrTopicAlreadyExists {
			return fmt.Errorf("create topic %s: %w", name, err)
		}
	}
	return nil
}

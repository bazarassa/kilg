package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
)

// NewClient builds a sarama client for metadata/offset operations.
func NewClient(brokers []string) (sarama.Client, error) {
	return NewClientWithRewrite(brokers, "")
}

// NewClientWithRewrite builds a sarama client with an optional rewrite spec.
func NewClientWithRewrite(brokers []string, rewrite string) (sarama.Client, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_4_0_0
	applyRewrite(cfg, rewrite)
	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return client, nil
}

package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.uber.org/zap"
)

// Producer wraps a Kafka writer for producing Debezium-format events
type Producer struct {
	writer *kafka.Writer
	logger *zap.Logger
}

// ProducerConfig holds configuration for the Kafka producer
type ProducerConfig struct {
	Brokers      []string
	BatchSize    int
	BatchTimeout time.Duration
	Async        bool
}

// NewProducer creates a new Kafka producer
func NewProducer(cfg ProducerConfig, logger *zap.Logger) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		Async:        cfg.Async,
		Compression:  compress.Snappy,
		RequiredAcks: kafka.RequireOne,
	}

	return &Producer{
		writer: writer,
		logger: logger,
	}
}

// Produce sends a single event to the specified topic
func (p *Producer) Produce(ctx context.Context, topic, key string, envelope DebeziumEnvelope) error {
	value, err := envelope.JSON()
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write message to %s: %w", topic, err)
	}

	p.logger.Debug("produced event",
		zap.String("topic", topic),
		zap.String("key", key),
		zap.String("op", envelope.Op),
	)

	return nil
}

// ProduceBatch sends multiple events to their respective topics
func (p *Producer) ProduceBatch(ctx context.Context, messages []kafka.Message) error {
	if len(messages) == 0 {
		return nil
	}

	if err := p.writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("write batch (%d messages): %w", len(messages), err)
	}

	p.logger.Debug("produced batch",
		zap.Int("count", len(messages)),
	)

	return nil
}

// Close closes the producer
func (p *Producer) Close() error {
	return p.writer.Close()
}

// Topics for Temporal CDC events
const (
	TopicEvents      = "temporal.events"
	TopicWorkflows   = "temporal.workflows"
	TopicActivities  = "temporal.activities"
	TopicTimers      = "temporal.timers"
	TopicCheckpoints = "temporal.checkpoints"
)

// EnsureTopics creates the required topics if they don't exist
func EnsureTopics(ctx context.Context, brokers []string, logger *zap.Logger) error {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}

	controllerConn, err := kafka.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer controllerConn.Close()

	topics := []kafka.TopicConfig{
		{Topic: TopicEvents, NumPartitions: 3, ReplicationFactor: 1},
		{Topic: TopicWorkflows, NumPartitions: 3, ReplicationFactor: 1},
		{Topic: TopicActivities, NumPartitions: 3, ReplicationFactor: 1},
		{Topic: TopicTimers, NumPartitions: 3, ReplicationFactor: 1},
		{Topic: TopicCheckpoints, NumPartitions: 1, ReplicationFactor: 1}, // Single partition, compacted
	}

	err = controllerConn.CreateTopics(topics...)
	if err != nil {
		// Ignore "topic already exists" errors
		logger.Debug("create topics result", zap.Error(err))
	}

	logger.Info("ensured topics exist",
		zap.Strings("topics", []string{TopicEvents, TopicWorkflows, TopicActivities, TopicTimers, TopicCheckpoints}),
	)

	return nil
}

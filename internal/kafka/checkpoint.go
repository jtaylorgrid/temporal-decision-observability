package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/refset/temporal-decision-observability/internal/temporal"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	checkpointKey = "temporal-cdc-checkpoint"
)

// CheckpointStore handles loading and saving checkpoints to a Kafka compacted topic
type CheckpointStore struct {
	brokers []string
	topic   string
	logger  *zap.Logger
}

// NewCheckpointStore creates a new checkpoint store
func NewCheckpointStore(brokers []string, logger *zap.Logger) *CheckpointStore {
	return &CheckpointStore{
		brokers: brokers,
		topic:   TopicCheckpoints,
		logger:  logger,
	}
}

// LoadCheckpoint reads the latest checkpoint from the compacted topic
func (s *CheckpointStore) LoadCheckpoint(ctx context.Context, namespace string) (*temporal.Checkpoint, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   s.brokers,
		Topic:     s.topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// Seek to the beginning to find the latest message with our key
	if err := reader.SetOffset(kafka.FirstOffset); err != nil {
		// Topic might not exist yet, return empty checkpoint
		s.logger.Debug("checkpoint topic not readable, starting fresh", zap.Error(err))
		return &temporal.Checkpoint{Namespace: namespace}, nil
	}

	// Read through the topic to find the latest checkpoint
	// For a compacted topic with a single key, there should only be one message
	var checkpoint *temporal.Checkpoint
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			// End of topic or timeout
			break
		}

		if string(msg.Key) == checkpointKey {
			var cp temporal.Checkpoint
			if err := json.Unmarshal(msg.Value, &cp); err != nil {
				s.logger.Warn("failed to unmarshal checkpoint", zap.Error(err))
				continue
			}
			checkpoint = &cp
		}
	}

	if checkpoint == nil {
		s.logger.Info("no existing checkpoint found, starting fresh")
		return &temporal.Checkpoint{Namespace: namespace}, nil
	}

	s.logger.Info("loaded checkpoint from Kafka",
		zap.String("namespace", checkpoint.Namespace),
		zap.Time("last_event_time", checkpoint.LastEventTime),
		zap.String("last_workflow_id", checkpoint.LastWorkflowID),
		zap.Int64("last_event_id", checkpoint.LastEventID),
	)

	return checkpoint, nil
}

// SaveCheckpoint writes the checkpoint to the compacted topic
func (s *CheckpointStore) SaveCheckpoint(ctx context.Context, checkpoint *temporal.Checkpoint) error {
	value, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(s.brokers...),
		Topic:    s.topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	msg := kafka.Message{
		Key:   []byte(checkpointKey),
		Value: value,
	}

	if err := writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}

	s.logger.Debug("saved checkpoint to Kafka",
		zap.Time("last_event_time", checkpoint.LastEventTime),
		zap.String("last_workflow_id", checkpoint.LastWorkflowID),
		zap.Int64("last_event_id", checkpoint.LastEventID),
	)

	return nil
}

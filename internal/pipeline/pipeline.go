package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/refset/temporal-decision-observability/internal/kafka"
	"github.com/refset/temporal-decision-observability/internal/temporal"
	kafkago "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Pipeline struct {
	poller          *temporal.Poller
	producer        *kafka.Producer
	checkpointStore *kafka.CheckpointStore
	pollInterval    time.Duration
	batchSize       int
	logger          *zap.Logger
	namespace       string
}

type Config struct {
	PollInterval time.Duration
	BatchSize    int
	Namespace    string
}

func New(poller *temporal.Poller, producer *kafka.Producer, checkpointStore *kafka.CheckpointStore, cfg Config, logger *zap.Logger) *Pipeline {
	return &Pipeline{
		poller:          poller,
		producer:        producer,
		checkpointStore: checkpointStore,
		pollInterval:    cfg.PollInterval,
		batchSize:       cfg.BatchSize,
		namespace:       cfg.Namespace,
		logger:          logger,
	}
}

func (p *Pipeline) Run(ctx context.Context) error {
	checkpoint, err := p.checkpointStore.LoadCheckpoint(ctx, p.namespace)
	if err != nil {
		return err
	}

	p.logger.Info("loaded checkpoint",
		zap.String("namespace", checkpoint.Namespace),
		zap.Time("last_event_time", checkpoint.LastEventTime),
		zap.String("last_workflow_id", checkpoint.LastWorkflowID),
		zap.Int64("last_event_id", checkpoint.LastEventID),
	)

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	if err := p.poll(ctx, checkpoint); err != nil {
		p.logger.Error("initial poll failed", zap.Error(err))
	}

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("pipeline shutting down")
			return ctx.Err()
		case <-ticker.C:
			if err := p.poll(ctx, checkpoint); err != nil {
				p.logger.Error("poll failed", zap.Error(err))
			}
		}
	}
}

func (p *Pipeline) poll(ctx context.Context, checkpoint *temporal.Checkpoint) error {
	p.logger.Debug("polling for events",
		zap.Time("since", checkpoint.LastEventTime),
	)

	events, newCheckpoint, err := p.poller.Poll(ctx, checkpoint)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		p.logger.Debug("no new events")
		return nil
	}

	p.logger.Info("fetched events",
		zap.Int("count", len(events)),
	)

	// Process events in batches
	for i := 0; i < len(events); i += p.batchSize {
		end := i + p.batchSize
		if end > len(events) {
			end = len(events)
		}
		batch := events[i:end]

		if err := p.produceBatch(ctx, batch); err != nil {
			return err
		}

		p.logger.Debug("produced batch",
			zap.Int("size", len(batch)),
			zap.Int("progress", end),
			zap.Int("total", len(events)),
		)
	}

	*checkpoint = *newCheckpoint

	if err := p.checkpointStore.SaveCheckpoint(ctx, checkpoint); err != nil {
		return err
	}

	p.logger.Info("checkpoint updated",
		zap.Time("last_event_time", checkpoint.LastEventTime),
		zap.String("last_workflow_id", checkpoint.LastWorkflowID),
		zap.Int64("last_event_id", checkpoint.LastEventID),
	)

	return nil
}

func (p *Pipeline) produceBatch(ctx context.Context, events []temporal.Event) error {
	var messages []kafkago.Message

	for _, event := range events {
		// Always produce the raw event
		eventEnvelope := kafka.ToDebeziumEvent(event)
		eventJSON, err := eventEnvelope.JSON()
		if err != nil {
			return fmt.Errorf("marshal event %s: %w", event.ID, err)
		}
		messages = append(messages, kafkago.Message{
			Topic: kafka.TopicEvents,
			Key:   []byte(event.ID),
			Value: eventJSON,
		})

		// Produce workflow state changes
		if strings.Contains(event.EventType, "WorkflowExecutionStarted") {
			state := kafka.WorkflowState{
				ID:           fmt.Sprintf("%s/%s/%s", event.Namespace, event.WorkflowID, event.RunID),
				Namespace:    event.Namespace,
				WorkflowID:   event.WorkflowID,
				RunID:        event.RunID,
				WorkflowType: event.WorkflowType,
				Status:       "Running",
				StartTime:    event.EventTime,
			}
			envelope := kafka.ToDebeziumWorkflow(state, nil)
			json, err := envelope.JSON()
			if err != nil {
				return fmt.Errorf("marshal workflow %s: %w", state.ID, err)
			}
			messages = append(messages, kafkago.Message{
				Topic: kafka.TopicWorkflows,
				Key:   []byte(state.ID),
				Value: json,
			})
		} else if isWorkflowCloseEvent(event.EventType) {
			status := extractStatus(event)
			state := kafka.WorkflowState{
				ID:           fmt.Sprintf("%s/%s/%s", event.Namespace, event.WorkflowID, event.RunID),
				Namespace:    event.Namespace,
				WorkflowID:   event.WorkflowID,
				RunID:        event.RunID,
				WorkflowType: event.WorkflowType,
				Status:       status,
				StartTime:    event.EventTime,
				CloseTime:    &event.EventTime,
			}
			envelope := kafka.ToDebeziumWorkflow(state, nil)
			json, err := envelope.JSON()
			if err != nil {
				return fmt.Errorf("marshal workflow %s: %w", state.ID, err)
			}
			messages = append(messages, kafkago.Message{
				Topic: kafka.TopicWorkflows,
				Key:   []byte(state.ID),
				Value: json,
			})
		}

		// Produce activity state changes
		if isActivityEvent(event.EventType) {
			activityID := ""
			activityType := ""
			var attempt int64 = 1

			if id, ok := event.Attributes["activity_id"].(string); ok {
				activityID = id
			}
			if at, ok := event.Attributes["activity_type"].(string); ok {
				activityType = at
			}
			if a, ok := event.Attributes["attempt"].(int64); ok {
				attempt = a
			}

			if activityID != "" {
				status := extractActivityStatus(event.EventType)
				state := kafka.ActivityState{
					ID:            fmt.Sprintf("%s/%s/%s/%s", event.Namespace, event.WorkflowID, event.RunID, activityID),
					Namespace:     event.Namespace,
					WorkflowID:    event.WorkflowID,
					RunID:         event.RunID,
					ActivityID:    activityID,
					ActivityType:  activityType,
					Status:        status,
					Attempt:       attempt,
					LastEventTime: event.EventTime,
					LastEventType: event.EventType,
				}
				envelope := kafka.ToDebeziumActivity(state, nil)
				json, err := envelope.JSON()
				if err != nil {
					return fmt.Errorf("marshal activity %s: %w", state.ID, err)
				}
				messages = append(messages, kafkago.Message{
					Topic: kafka.TopicActivities,
					Key:   []byte(state.ID),
					Value: json,
				})
			}
		}

		// Produce timer state changes
		if isTimerEvent(event.EventType) {
			timerID := ""
			if id, ok := event.Attributes["timer_id"].(string); ok {
				timerID = id
			}

			if timerID != "" {
				status := extractTimerStatus(event.EventType)
				state := kafka.TimerState{
					ID:            fmt.Sprintf("%s/%s/%s/timer/%s", event.Namespace, event.WorkflowID, event.RunID, timerID),
					Namespace:     event.Namespace,
					WorkflowID:    event.WorkflowID,
					RunID:         event.RunID,
					TimerID:       timerID,
					Status:        status,
					LastEventTime: event.EventTime,
				}
				envelope := kafka.ToDebeziumTimer(state, nil)
				json, err := envelope.JSON()
				if err != nil {
					return fmt.Errorf("marshal timer %s: %w", state.ID, err)
				}
				messages = append(messages, kafkago.Message{
					Topic: kafka.TopicTimers,
					Key:   []byte(state.ID),
					Value: json,
				})
			}
		}
	}

	return p.producer.ProduceBatch(ctx, messages)
}

func isWorkflowCloseEvent(eventType string) bool {
	closeEvents := []string{
		"WorkflowExecutionCompleted",
		"WorkflowExecutionFailed",
		"WorkflowExecutionTimedOut",
		"WorkflowExecutionCanceled",
		"WorkflowExecutionTerminated",
	}
	for _, e := range closeEvents {
		if strings.Contains(eventType, e) {
			return true
		}
	}
	return false
}

func extractStatus(event temporal.Event) string {
	if s, ok := event.Attributes["status"].(string); ok {
		return s
	}

	switch {
	case strings.Contains(event.EventType, "Completed"):
		return "Completed"
	case strings.Contains(event.EventType, "Failed"):
		return "Failed"
	case strings.Contains(event.EventType, "TimedOut"):
		return "TimedOut"
	case strings.Contains(event.EventType, "Canceled"):
		return "Canceled"
	case strings.Contains(event.EventType, "Terminated"):
		return "Terminated"
	default:
		return "Closed"
	}
}

func isActivityEvent(eventType string) bool {
	activityEvents := []string{
		"ActivityTaskScheduled",
		"ActivityTaskStarted",
		"ActivityTaskCompleted",
		"ActivityTaskFailed",
		"ActivityTaskTimedOut",
		"ActivityTaskCanceled",
	}
	for _, e := range activityEvents {
		if strings.Contains(eventType, e) {
			return true
		}
	}
	return false
}

func extractActivityStatus(eventType string) string {
	switch {
	case strings.Contains(eventType, "ActivityTaskScheduled"):
		return "Scheduled"
	case strings.Contains(eventType, "ActivityTaskStarted"):
		return "Running"
	case strings.Contains(eventType, "ActivityTaskCompleted"):
		return "Completed"
	case strings.Contains(eventType, "ActivityTaskFailed"):
		return "Failed"
	case strings.Contains(eventType, "ActivityTaskTimedOut"):
		return "TimedOut"
	case strings.Contains(eventType, "ActivityTaskCanceled"):
		return "Canceled"
	default:
		return "Unknown"
	}
}

func isTimerEvent(eventType string) bool {
	return strings.Contains(eventType, "TimerStarted") ||
		strings.Contains(eventType, "TimerFired") ||
		strings.Contains(eventType, "TimerCanceled")
}

func extractTimerStatus(eventType string) string {
	switch {
	case strings.Contains(eventType, "TimerStarted"):
		return "Started"
	case strings.Contains(eventType, "TimerFired"):
		return "Fired"
	case strings.Contains(eventType, "TimerCanceled"):
		return "Canceled"
	default:
		return "Unknown"
	}
}

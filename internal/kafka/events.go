package kafka

import (
	"encoding/json"
	"time"

	"github.com/refset/temporal-decision-observability/internal/temporal"
)

// DebeziumEnvelope represents a Debezium-format CDC event
type DebeziumEnvelope struct {
	Before  map[string]any  `json:"before"`
	After   map[string]any  `json:"after"`
	Source  DebeziumSource  `json:"source"`
	Op      string          `json:"op"` // c=create, u=update, d=delete, r=read
	TsMs    int64           `json:"ts_ms"`
}

// DebeziumSource contains metadata about the source of the event
type DebeziumSource struct {
	Version   string `json:"version"`
	Connector string `json:"connector"`
	Name      string `json:"name"`
	TsMs      int64  `json:"ts_ms"`
	Db        string `json:"db"`
	Table     string `json:"table"`
}

// ToDebeziumEvent converts a Temporal Event to a Debezium envelope for the events topic
func ToDebeziumEvent(event temporal.Event) DebeziumEnvelope {
	after := map[string]any{
		"_id":           event.ID,
		"_valid_from":   event.EventTime.Format(time.RFC3339Nano),
		"namespace":     event.Namespace,
		"workflow_id":   event.WorkflowID,
		"run_id":        event.RunID,
		"event_id":      event.EventID,
		"event_type":    event.EventType,
		"event_time":    event.EventTime.Format(time.RFC3339Nano),
		"workflow_type": event.WorkflowType,
		"task_queue":    event.TaskQueue,
		"event_data":    event.Attributes, // New field name to avoid type conflict with old 'attributes' string column
	}

	return DebeziumEnvelope{
		Before: nil,
		After:  after,
		Source: DebeziumSource{
			Version:   "1.0.0",
			Connector: "temporal-cdc",
			Name:      "temporal",
			TsMs:      event.EventTime.UnixMilli(),
			Db:        "temporal",
			Table:     "events",
		},
		Op:   "c", // Events are always creates (append-only)
		TsMs: time.Now().UnixMilli(),
	}
}

// WorkflowState represents the current state of a workflow for upsert events
type WorkflowState struct {
	ID           string
	Namespace    string
	WorkflowID   string
	RunID        string
	WorkflowType string
	Status       string
	StartTime    time.Time
	CloseTime    *time.Time
}

// ToDebeziumWorkflow converts a WorkflowState to a Debezium envelope
func ToDebeziumWorkflow(state WorkflowState, before *WorkflowState) DebeziumEnvelope {
	after := map[string]any{
		"_id":           state.ID,
		"_valid_from":   state.StartTime.Format(time.RFC3339Nano),
		"namespace":     state.Namespace,
		"workflow_id":   state.WorkflowID,
		"run_id":        state.RunID,
		"workflow_type": state.WorkflowType,
		"status":        state.Status,
		"start_time":    state.StartTime.Format(time.RFC3339Nano),
	}
	if state.CloseTime != nil {
		after["close_time"] = state.CloseTime.Format(time.RFC3339Nano)
		after["_valid_from"] = state.CloseTime.Format(time.RFC3339Nano)
	}

	var beforeMap map[string]any
	op := "c"
	if before != nil {
		op = "u"
		beforeMap = map[string]any{
			"_id":           before.ID,
			"namespace":     before.Namespace,
			"workflow_id":   before.WorkflowID,
			"run_id":        before.RunID,
			"workflow_type": before.WorkflowType,
			"status":        before.Status,
			"start_time":    before.StartTime.Format(time.RFC3339Nano),
		}
		if before.CloseTime != nil {
			beforeMap["close_time"] = before.CloseTime.Format(time.RFC3339Nano)
		}
	}

	return DebeziumEnvelope{
		Before: beforeMap,
		After:  after,
		Source: DebeziumSource{
			Version:   "1.0.0",
			Connector: "temporal-cdc",
			Name:      "temporal",
			TsMs:      state.StartTime.UnixMilli(),
			Db:        "temporal",
			Table:     "workflows",
		},
		Op:   op,
		TsMs: time.Now().UnixMilli(),
	}
}

// ActivityState represents the current state of an activity
type ActivityState struct {
	ID            string
	Namespace     string
	WorkflowID    string
	RunID         string
	ActivityID    string
	ActivityType  string
	Status        string
	Attempt       int64
	LastEventTime time.Time
	LastEventType string
}

// ToDebeziumActivity converts an ActivityState to a Debezium envelope
func ToDebeziumActivity(state ActivityState, before *ActivityState) DebeziumEnvelope {
	after := map[string]any{
		"_id":             state.ID,
		"_valid_from":     state.LastEventTime.Format(time.RFC3339Nano),
		"namespace":       state.Namespace,
		"workflow_id":     state.WorkflowID,
		"run_id":          state.RunID,
		"activity_id":     state.ActivityID,
		"activity_type":   state.ActivityType,
		"status":          state.Status,
		"attempt":         state.Attempt,
		"last_event_time": state.LastEventTime.Format(time.RFC3339Nano),
		"last_event_type": state.LastEventType,
	}

	var beforeMap map[string]any
	op := "c"
	if before != nil {
		op = "u"
		beforeMap = map[string]any{
			"_id":             before.ID,
			"namespace":       before.Namespace,
			"workflow_id":     before.WorkflowID,
			"run_id":          before.RunID,
			"activity_id":     before.ActivityID,
			"activity_type":   before.ActivityType,
			"status":          before.Status,
			"attempt":         before.Attempt,
			"last_event_time": before.LastEventTime.Format(time.RFC3339Nano),
			"last_event_type": before.LastEventType,
		}
	}

	return DebeziumEnvelope{
		Before: beforeMap,
		After:  after,
		Source: DebeziumSource{
			Version:   "1.0.0",
			Connector: "temporal-cdc",
			Name:      "temporal",
			TsMs:      state.LastEventTime.UnixMilli(),
			Db:        "temporal",
			Table:     "activities",
		},
		Op:   op,
		TsMs: time.Now().UnixMilli(),
	}
}

// TimerState represents the current state of a timer
type TimerState struct {
	ID            string
	Namespace     string
	WorkflowID    string
	RunID         string
	TimerID       string
	Status        string
	LastEventTime time.Time
}

// ToDebeziumTimer converts a TimerState to a Debezium envelope
func ToDebeziumTimer(state TimerState, before *TimerState) DebeziumEnvelope {
	after := map[string]any{
		"_id":             state.ID,
		"_valid_from":     state.LastEventTime.Format(time.RFC3339Nano),
		"namespace":       state.Namespace,
		"workflow_id":     state.WorkflowID,
		"run_id":          state.RunID,
		"timer_id":        state.TimerID,
		"status":          state.Status,
		"last_event_time": state.LastEventTime.Format(time.RFC3339Nano),
	}

	var beforeMap map[string]any
	op := "c"
	if before != nil {
		op = "u"
		beforeMap = map[string]any{
			"_id":             before.ID,
			"namespace":       before.Namespace,
			"workflow_id":     before.WorkflowID,
			"run_id":          before.RunID,
			"timer_id":        before.TimerID,
			"status":          before.Status,
			"last_event_time": before.LastEventTime.Format(time.RFC3339Nano),
		}
	}

	return DebeziumEnvelope{
		Before: beforeMap,
		After:  after,
		Source: DebeziumSource{
			Version:   "1.0.0",
			Connector: "temporal-cdc",
			Name:      "temporal",
			TsMs:      state.LastEventTime.UnixMilli(),
			Db:        "temporal",
			Table:     "timers",
		},
		Op:   op,
		TsMs: time.Now().UnixMilli(),
	}
}

// JSON returns the JSON-encoded envelope
func (e DebeziumEnvelope) JSON() ([]byte, error) {
	return json.Marshal(e)
}

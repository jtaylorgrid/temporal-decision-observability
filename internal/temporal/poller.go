package temporal

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	commonproto "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historyproto "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
)

type Event struct {
	ID           string
	Namespace    string
	WorkflowID   string
	RunID        string
	EventID      int64
	EventType    string
	EventTime    time.Time
	WorkflowType string
	TaskQueue    string
	Attributes   map[string]any
}

type Checkpoint struct {
	Namespace      string    `json:"namespace"`
	LastEventTime  time.Time `json:"last_event_time"`
	LastWorkflowID string    `json:"last_workflow_id"`
	LastRunID      string    `json:"last_run_id"`
	LastEventID    int64     `json:"last_event_id"`
}

type Poller struct {
	client           *Client
	workflowProgress sync.Map // "workflowID/runID" → last seen event ID (int64)
}

func NewPoller(c *Client) *Poller {
	return &Poller{client: c}
}

func (p *Poller) Poll(ctx context.Context, checkpoint *Checkpoint) ([]Event, *Checkpoint, error) {
	var allEvents []Event
	newCheckpoint := &Checkpoint{
		Namespace:      p.client.Namespace(),
		LastEventTime:  checkpoint.LastEventTime,
		LastWorkflowID: checkpoint.LastWorkflowID,
		LastRunID:      checkpoint.LastRunID,
		LastEventID:    checkpoint.LastEventID,
	}

	if checkpoint.LastEventTime.IsZero() {
		events, err := p.pollByQuery(ctx, "")
		if err != nil {
			return nil, nil, fmt.Errorf("poll all workflows: %w", err)
		}
		allEvents = append(allEvents, events...)
	} else {
		runningEvents, err := p.pollByQuery(ctx, `ExecutionStatus = 'Running'`)
		if err != nil {
			return nil, nil, fmt.Errorf("poll running workflows: %w", err)
		}
		allEvents = append(allEvents, runningEvents...)

		closedQuery := fmt.Sprintf("CloseTime > '%s'", checkpoint.LastEventTime.Format(time.RFC3339))
		closedEvents, err := p.pollByQuery(ctx, closedQuery)
		if err != nil {
			return nil, nil, fmt.Errorf("poll closed workflows: %w", err)
		}
		allEvents = append(allEvents, closedEvents...)
	}

	for _, evt := range allEvents {
		if evt.EventTime.After(newCheckpoint.LastEventTime) ||
			(evt.EventTime.Equal(newCheckpoint.LastEventTime) && evt.EventID > newCheckpoint.LastEventID) {
			newCheckpoint.LastEventTime = evt.EventTime
			newCheckpoint.LastWorkflowID = evt.WorkflowID
			newCheckpoint.LastRunID = evt.RunID
			newCheckpoint.LastEventID = evt.EventID
		}
	}

	return allEvents, newCheckpoint, nil
}

func (p *Poller) pollByQuery(ctx context.Context, query string) ([]Event, error) {
	var events []Event
	var nextPageToken []byte

	for {
		resp, err := p.client.SDK().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     p.client.Namespace(),
			PageSize:      100,
			NextPageToken: nextPageToken,
			Query:         query,
		})
		if err != nil {
			return nil, fmt.Errorf("list workflows (query=%q): %w", query, err)
		}

		for _, exec := range resp.Executions {
			workflowID := exec.Execution.WorkflowId
			runID := exec.Execution.RunId

			historyEvents, err := p.getWorkflowHistory(ctx, workflowID, runID)
			if err != nil {
				return nil, fmt.Errorf("get history for %s/%s: %w", workflowID, runID, err)
			}

			events = append(events, historyEvents...)
		}

		nextPageToken = resp.NextPageToken
		if len(nextPageToken) == 0 {
			break
		}
	}

	return events, nil
}

func (p *Poller) getWorkflowHistory(ctx context.Context, workflowID, runID string) ([]Event, error) {
	key := workflowID + "/" + runID
	var startEventID int64
	if val, ok := p.workflowProgress.Load(key); ok {
		startEventID = val.(int64)
	}

	var events []Event
	iter := p.client.SDK().GetWorkflowHistory(ctx, workflowID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	for iter.HasNext() {
		historyEvent, err := iter.Next()
		if err != nil {
			return nil, err
		}

		if historyEvent.EventId <= startEventID {
			continue
		}

		event := p.transformEvent(workflowID, runID, historyEvent)
		events = append(events, event)
	}

	if len(events) > 0 {
		p.workflowProgress.Store(key, events[len(events)-1].EventID)
	}

	return events, nil
}

// decodePayloads converts Temporal payloads to a JSON-friendly format.
// Temporal typically uses JSON encoding, so we try to decode as JSON first.
// Falls back to base64-encoded string for binary payloads.
func decodePayloads(payloads *commonproto.Payloads) any {
	if payloads == nil || len(payloads.Payloads) == 0 {
		return nil
	}

	results := make([]any, 0, len(payloads.Payloads))
	for _, payload := range payloads.Payloads {
		if payload == nil || len(payload.Data) == 0 {
			results = append(results, nil)
			continue
		}

		// Check metadata for encoding type
		encoding := ""
		if payload.Metadata != nil {
			if enc, ok := payload.Metadata["encoding"]; ok {
				encoding = string(enc)
			}
		}

		// Try to decode as JSON (most common case)
		if encoding == "" || encoding == "json/plain" || encoding == "json/protobuf" {
			var decoded any
			if err := json.Unmarshal(payload.Data, &decoded); err == nil {
				results = append(results, decoded)
				continue
			}
		}

		// Fall back to string representation for plain text
		if encoding == "binary/plain" || encoding == "" {
			// Try as string first
			results = append(results, string(payload.Data))
		} else {
			// Unknown encoding, store as string
			results = append(results, string(payload.Data))
		}
	}

	// If single payload, return unwrapped
	if len(results) == 1 {
		return results[0]
	}
	return results
}

func (p *Poller) transformEvent(workflowID, runID string, he *historyproto.HistoryEvent) Event {
	event := Event{
		ID:         fmt.Sprintf("%s/%s/%s/%d", p.client.Namespace(), workflowID, runID, he.EventId),
		Namespace:  p.client.Namespace(),
		WorkflowID: workflowID,
		RunID:      runID,
		EventID:    he.EventId,
		EventType:  he.EventType.String(),
		EventTime:  he.EventTime.AsTime(),
		Attributes: make(map[string]any),
	}

	switch attrs := he.Attributes.(type) {
	case *historyproto.HistoryEvent_WorkflowExecutionStartedEventAttributes:
		a := attrs.WorkflowExecutionStartedEventAttributes
		event.WorkflowType = a.WorkflowType.GetName()
		event.TaskQueue = a.TaskQueue.GetName()
		event.Attributes["workflow_type"] = a.WorkflowType.GetName()
		event.Attributes["task_queue"] = a.TaskQueue.GetName()
		event.Attributes["attempt"] = a.Attempt
		if a.ParentWorkflowExecution != nil {
			event.Attributes["parent_workflow_id"] = a.ParentWorkflowExecution.WorkflowId
			event.Attributes["parent_run_id"] = a.ParentWorkflowExecution.RunId
		}
		if input := decodePayloads(a.Input); input != nil {
			event.Attributes["workflow_input"] = input
		}

	case *historyproto.HistoryEvent_WorkflowExecutionCompletedEventAttributes:
		a := attrs.WorkflowExecutionCompletedEventAttributes
		event.Attributes["status"] = "Completed"
		if result := decodePayloads(a.Result); result != nil {
			event.Attributes["result"] = result
		}

	case *historyproto.HistoryEvent_WorkflowExecutionFailedEventAttributes:
		a := attrs.WorkflowExecutionFailedEventAttributes
		event.Attributes["status"] = "Failed"
		if a.Failure != nil {
			event.Attributes["failure_message"] = a.Failure.Message
		}

	case *historyproto.HistoryEvent_WorkflowExecutionTimedOutEventAttributes:
		event.Attributes["status"] = "TimedOut"

	case *historyproto.HistoryEvent_WorkflowExecutionCanceledEventAttributes:
		event.Attributes["status"] = "Canceled"

	case *historyproto.HistoryEvent_ActivityTaskScheduledEventAttributes:
		a := attrs.ActivityTaskScheduledEventAttributes
		event.Attributes["activity_type"] = a.ActivityType.GetName()
		event.Attributes["activity_id"] = a.ActivityId
		event.Attributes["task_queue"] = a.TaskQueue.GetName()
		if input := decodePayloads(a.Input); input != nil {
			event.Attributes["activity_input"] = input
		}

	case *historyproto.HistoryEvent_ActivityTaskStartedEventAttributes:
		a := attrs.ActivityTaskStartedEventAttributes
		event.Attributes["scheduled_event_id"] = a.ScheduledEventId
		event.Attributes["attempt"] = a.Attempt

	case *historyproto.HistoryEvent_ActivityTaskCompletedEventAttributes:
		a := attrs.ActivityTaskCompletedEventAttributes
		event.Attributes["scheduled_event_id"] = a.ScheduledEventId
		if result := decodePayloads(a.Result); result != nil {
			event.Attributes["result"] = result
		}

	case *historyproto.HistoryEvent_TimerStartedEventAttributes:
		a := attrs.TimerStartedEventAttributes
		event.Attributes["timer_id"] = a.TimerId
		event.Attributes["start_to_fire_timeout"] = a.StartToFireTimeout.AsDuration().String()

	case *historyproto.HistoryEvent_TimerFiredEventAttributes:
		a := attrs.TimerFiredEventAttributes
		event.Attributes["timer_id"] = a.TimerId

	case *historyproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes:
		a := attrs.WorkflowExecutionSignaledEventAttributes
		event.Attributes["signal_name"] = a.SignalName
		if signalData := decodePayloads(a.Input); signalData != nil {
			event.Attributes["signal_input"] = signalData
		}
		event.Attributes["identity"] = a.Identity

	case *historyproto.HistoryEvent_ActivityTaskFailedEventAttributes:
		a := attrs.ActivityTaskFailedEventAttributes
		event.Attributes["scheduled_event_id"] = a.ScheduledEventId
		if a.Failure != nil {
			event.Attributes["failure_message"] = a.Failure.Message
			event.Attributes["failure_type"] = a.Failure.GetApplicationFailureInfo().GetType()
			if details := decodePayloads(a.Failure.GetApplicationFailureInfo().GetDetails()); details != nil {
				event.Attributes["failure_details"] = details
			}
		}
		event.Attributes["retry_state"] = a.RetryState.String()

	case *historyproto.HistoryEvent_ActivityTaskTimedOutEventAttributes:
		a := attrs.ActivityTaskTimedOutEventAttributes
		event.Attributes["scheduled_event_id"] = a.ScheduledEventId
		if a.Failure != nil {
			event.Attributes["failure_message"] = a.Failure.Message
		}
		event.Attributes["retry_state"] = a.RetryState.String()

	case *historyproto.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes:
		a := attrs.WorkflowExecutionContinuedAsNewEventAttributes
		event.Attributes["new_run_id"] = a.NewExecutionRunId
		event.Attributes["workflow_type"] = a.WorkflowType.GetName()
		event.Attributes["task_queue"] = a.TaskQueue.GetName()
		if input := decodePayloads(a.Input); input != nil {
			event.Attributes["workflow_input"] = input
		}
	}

	return event
}

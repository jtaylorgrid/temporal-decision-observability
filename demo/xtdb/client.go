package xtdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"
)

type Client struct {
	pool *pgxpool.Pool
}

func NewClient(pool *pgxpool.Pool) *Client {
	return &Client{pool: pool}
}

func NewClientFromConnString(ctx context.Context, connString string) (*Client, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("connect to XTDB: %w", err)
	}
	return &Client{pool: pool}, nil
}

func (c *Client) Close() {
	if c.pool != nil {
		c.pool.Close()
	}
}

type DecisionContext struct {
	Table      string
	ID         string
	WorkflowID string
	RunID      string
	ActivityID string
	ValidFrom  time.Time
	Data       map[string]any
}

func (c *Client) Save(ctx context.Context, dc DecisionContext) error {
	if dc.Table == "" {
		return fmt.Errorf("table name required")
	}
	if dc.ID == "" {
		return fmt.Errorf("ID required")
	}

	if dc.ValidFrom.IsZero() {
		dc.ValidFrom = time.Now()
	}

	if activityInfo := activity.GetInfo(ctx); activityInfo.WorkflowExecution.ID != "" {
		if dc.WorkflowID == "" {
			dc.WorkflowID = activityInfo.WorkflowExecution.ID
		}
		if dc.RunID == "" {
			dc.RunID = activityInfo.WorkflowExecution.RunID
		}
		if dc.ActivityID == "" {
			dc.ActivityID = activityInfo.ActivityID
		}
	}

	fields := []string{
		fmt.Sprintf("_id: %s", quote(dc.ID)),
		fmt.Sprintf("_valid_from: TIMESTAMP %s", quote(dc.ValidFrom.Format(time.RFC3339Nano))),
	}

	if dc.WorkflowID != "" {
		fields = append(fields, fmt.Sprintf("workflow_id: %s", quote(dc.WorkflowID)))
	}
	if dc.RunID != "" {
		fields = append(fields, fmt.Sprintf("run_id: %s", quote(dc.RunID)))
	}
	if dc.ActivityID != "" {
		fields = append(fields, fmt.Sprintf("activity_id: %s", quote(dc.ActivityID)))
	}

	for k, v := range dc.Data {
		fields = append(fields, fmt.Sprintf("%s: %s", k, formatValue(v)))
	}

	sql := fmt.Sprintf("INSERT INTO %s RECORDS {%s}", dc.Table, strings.Join(fields, ", "))

	_, err := c.pool.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("save to XTDB: %w", err)
	}

	return nil
}

func (c *Client) SaveDecision(ctx context.Context, table string, data map[string]any) error {
	id := fmt.Sprintf("%s:%d", table, time.Now().UnixNano())
	return c.Save(ctx, DecisionContext{
		Table:     table,
		ID:        id,
		ValidFrom: time.Now(),
		Data:      data,
	})
}

func quote(s string) string {
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'"
}

func formatValue(v any) string {
	switch val := v.(type) {
	case string:
		return quote(val)
	case int, int32, int64, uint, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return fmt.Sprintf("%v", val)
	case bool:
		return fmt.Sprintf("%t", val)
	case time.Time:
		return fmt.Sprintf("TIMESTAMP %s", quote(val.Format(time.RFC3339Nano)))
	case nil:
		return "NULL"
	default:
		return quote(fmt.Sprintf("%v", val))
	}
}

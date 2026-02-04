package xtdb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"
)

const (
	JsonOID = 114
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

	record := make(map[string]any)
	record["_id"] = dc.ID
	record["_valid_from"] = dc.ValidFrom.Format(time.RFC3339Nano)

	if dc.WorkflowID != "" {
		record["workflow_id"] = dc.WorkflowID
	}
	if dc.RunID != "" {
		record["run_id"] = dc.RunID
	}
	if dc.ActivityID != "" {
		record["activity_id"] = dc.ActivityID
	}

	for k, v := range dc.Data {
		if t, ok := v.(time.Time); ok {
			record[k] = t.Format(time.RFC3339Nano)
		} else {
			record[k] = v
		}
	}

	jsonData, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal record to JSON: %w", err)
	}

	sql := fmt.Sprintf("INSERT INTO %s RECORDS $1", dc.Table)

	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	rr := conn.Conn().PgConn().ExecParams(
		ctx,
		sql,
		[][]byte{jsonData},
		[]uint32{JsonOID},
		[]int16{pgx.TextFormatCode},
		[]int16{},
	)
	_, err = rr.Close()
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

func (c *Client) ExecParams(ctx context.Context, sql string, params [][]byte, paramOIDs []uint32) (pgconn.CommandTag, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return pgconn.CommandTag{}, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	paramFormats := make([]int16, len(params))
	for i := range paramFormats {
		paramFormats[i] = pgx.TextFormatCode
	}

	rr := conn.Conn().PgConn().ExecParams(
		ctx,
		sql,
		params,
		paramOIDs,
		paramFormats,
		[]int16{},
	)
	return rr.Close()
}

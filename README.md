# Temporal Decision Observability

A CDC connector that streams Temporal workflow events into XTDB v2 for bitemporal audit and compliance queries.

## Overview

This project solves the "decision observability" problem for AI agent orchestration: **what did the agent know when it made that decision?** and **what should it have known to make a better decision?**

Temporal is excellent at workflow orchestration but has limitations for long-term audit:
- 30-90 day retention limits (vs 6-7 years for HIPAA/SOX compliance)
- Audit logs don't capture data plane events (workflow starts, decisions, etc.)
- Querying historical data requires complex ETL pipelines

XTDB's bitemporal model enables:
- **Point-in-time queries**: `FOR VALID_TIME AS OF timestamp`
- **Audit queries**: `FOR SYSTEM_TIME AS OF timestamp`
- **Unlimited retention**: Keep data for years without archival pipelines
- **Cross-source joins**: Combine workflow events with ML predictions, CRM data, etc.

## Quick Start

The easiest way to run the demo is with the provided script:

```bash
./run-demo.sh
```

This will:
1. Start all Docker infrastructure (Temporal, XTDB, Kafka, Kafka Connect)
2. Configure the Kafka Connect XTDB sink connector
3. Start the Temporal worker with XTDB integration
4. Load demo data (churn predictions, customer outcomes)
5. Start sample workflows
6. Launch the demo UI

Then open http://localhost:3000 and press `s` to toggle the demo talk track.

### Script Options

```bash
./run-demo.sh --help        # Show all options
./run-demo.sh --fresh       # Fresh start: remove volumes and restart everything
./run-demo.sh --skip-infra  # Skip Docker startup (if already running)
./run-demo.sh --skip-data   # Skip loading demo data
```

## Architecture

```
┌─────────────┐                              ┌─────────────┐
│  Temporal   │──┬─── CDC via Kafka ────────▶│    XTDB     │
│ (workflows) │  │    Connect                │   (audit)   │
└─────────────┘  │                           └─────────────┘
                 │                                  │
                 └─── Direct Activity ─────────────┘
                      Writes (xtdb.Save)
                                                    │
                                                    ▼
                                             ┌─────────────┐
                                             │   Demo UI   │
                                             │ (localhost  │
                                             │   :3000)    │
                                             └─────────────┘
```

### Two Complementary Data Capture Approaches

1. **CDC via Kafka Connect**: Streams Temporal workflow events automatically. Captures *what the workflow did*: event types, activity scheduling, and payloads.
   - Tables: `events`, `temporal_events`, `workflows`

2. **Direct Activity Writes**: Activities call `xtdb.Save(ctx)` explicitly to capture decision context. Records *what external systems believed* at the exact moment a decision was made.
   - Tables: `activity_churn_signals`, `activity_routing_decisions`

Together, they answer: "what did the agent do?" *and* "what did it know?"

## Project Structure

```
temporal-decision-observability/
├── run-demo.sh                  # One-command demo startup script
├── main.go                      # CDC connector entry point
├── internal/
│   ├── config/config.go         # Configuration (env vars + YAML)
│   ├── temporal/
│   │   ├── client.go            # Temporal SDK client wrapper
│   │   └── poller.go            # Poll workflows + history events
│   ├── kafka/
│   │   ├── producer.go          # Kafka producer for CDC events
│   │   ├── events.go            # Event serialization
│   │   └── checkpoint.go        # Checkpoint management
│   └── pipeline/pipeline.go     # Source→Sink orchestration
├── kafka-connect-xtdb/          # Kafka Connect XTDB sink connector (Java)
│   ├── src/main/java/           # Connector source code
│   ├── pom.xml                  # Maven build
│   └── Dockerfile               # Connector Docker image
├── demo/
│   ├── README.md                # Demo-specific instructions
│   ├── workflow/
│   │   ├── workflow.go          # CustomerServiceWorkflow
│   │   └── activities.go        # AI agent activities (with XTDB integration)
│   ├── worker/main.go           # Temporal worker
│   ├── starter/main.go          # Starts sample workflows
│   ├── xtdb/client.go           # XTDB client helper for activities
│   ├── loaders/
│   │   ├── churn.go             # Load churn predictions
│   │   ├── outcomes.go          # Load customer outcomes
│   │   └── corrections.go       # Load model corrections
│   └── ui/
│       ├── main.go              # Web server with audit APIs
│       ├── templates/index.html # Dashboard + demo talk track
│       └── architecture.d2      # Architecture diagram source
├── docker-compose.yml           # Full infrastructure stack
├── Dockerfile                   # CDC connector Docker image
└── config.yaml                  # Default configuration
```

## Manual Setup (Alternative to run-demo.sh)

If you prefer to run components individually:

### 1. Start Infrastructure

```bash
docker-compose up -d
```

Wait for services:
- Temporal UI: http://localhost:8080
- XTDB: localhost:5432 (PostgreSQL wire protocol)
- Kafka Connect: http://localhost:8083

Verify services are healthy:
```bash
docker-compose ps
```

### 2. Configure Kafka Connect XTDB Sink

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "xtdb-sink",
    "config": {
      "connector.class": "com.xtdb.kafka.connect.XtdbSinkConnector",
      "tasks.max": "1",
      "topics": "temporal-events,temporal-workflows",
      "xtdb.url": "jdbc:postgresql://xtdb:5432/xtdb",
      "xtdb.user": "xtdb",
      "xtdb.password": "xtdb"
    }
  }'
```

### 3. Run Demo Components

In separate terminals:

```bash
# Terminal 1: Start demo worker (with XTDB integration for direct writes)
XTDB_CONN_STRING="postgres://localhost:5432/xtdb?sslmode=disable" go run ./demo/worker

# Terminal 2: Load sample data
go run ./demo/loaders/churn.go
go run ./demo/loaders/outcomes.go

# Terminal 3: Start sample workflows
go run ./demo/starter

# Terminal 4: Start the UI
XTDB_CONN_STRING="postgres://localhost:5432/xtdb?sslmode=disable" go run ./demo/ui
```

Open http://localhost:3000

Press `s` to toggle the demo talk track with step-by-step instructions.

## Configuration

Environment variables (or `config.yaml`):

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_HOST_PORT` | `localhost:7233` | Temporal gRPC endpoint |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `XTDB_CONN_STRING` | `postgres://localhost:5432/xtdb?sslmode=disable` | XTDB connection |
| `POLL_INTERVAL` | `10s` | How often to poll Temporal |
| `BATCH_SIZE` | `100` | Max workflows per poll |

## XTDB Tables

### CDC Tables (populated via Kafka Connect)

#### `workflows` / `temporal_workflows`
Materialized workflow state with bitemporal tracking.

```sql
SELECT * FROM temporal_workflows
FOR VALID_TIME AS OF TIMESTAMP '2024-01-15T10:00:00Z'
WHERE status = 'Running'
```

#### `events` / `temporal_events`
All workflow history events with full payloads.

```sql
SELECT * FROM temporal_events
WHERE workflow_id = 'order-123'
ORDER BY event_id
```

### Activity Context Tables (populated via direct writes)

#### `activity_churn_signals`
Churn risk scores captured at the moment activities executed.

```sql
SELECT workflow_id, customer_id, churn_score, risk_level, _valid_from
FROM activity_churn_signals
ORDER BY _valid_from DESC
```

#### `activity_routing_decisions`
Full decision context including all inputs that informed routing decisions.

```sql
SELECT workflow_id, queue, priority, reason_codes,
       input_churn_score, input_sentiment, input_customer_tier
FROM activity_routing_decisions
ORDER BY _valid_from DESC
```

### Demo Data Tables

#### `churn_predictions`
ML model predictions loaded for the demo.

#### `customer_outcomes`
Actual customer churn outcomes for counterfactual analysis.

#### `correction_markers`
Tracks when model corrections were applied (for before/after demo).

## Example Bitemporal Queries

### What workflows were running at a specific time?

```sql
SELECT * FROM temporal_workflows
FOR VALID_TIME AS OF TIMESTAMP '2024-06-15T10:00:00Z'
WHERE status = 'Running'
```

### Audit: What did we know at time of compliance check?

```sql
SELECT * FROM temporal_events
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-15T12:00:00Z'
WHERE workflow_id = 'order-123'
```

### Full bitemporal history

```sql
SELECT _id, event_type, _valid_from, _system_from
FROM temporal_events
FOR VALID_TIME ALL FOR SYSTEM_TIME ALL
WHERE workflow_id = 'order-123'
```

### Cross-source join (demo)

```sql
SELECT e.workflow_id, e.event_time, c.churn_score
FROM temporal_events e
JOIN churn_predictions FOR VALID_TIME ALL AS c
  ON c.customer_id = 'CUST-001'
WHERE c._valid_from <= e.event_time
  AND (c._valid_to IS NULL OR c._valid_to > e.event_time)
```

## Development

### Prerequisites

- Go 1.21+
- Docker & Docker Compose
- (Optional) `psql` for querying XTDB directly

### Building

```bash
go build -o cdc-connector .
```

### Running Tests

```bash
go test ./...
```

### Querying XTDB Directly

```bash
psql "postgres://localhost:5432/xtdb?sslmode=disable"

# Example queries
SELECT COUNT(*) FROM temporal_workflows;
SELECT COUNT(*) FROM temporal_events;
SELECT * FROM cdc_checkpoints;
```

### Docker Build

```bash
docker build -t temporal-cdc-xtdb .
```

## Future Claude Code Sessions

When resuming development with Claude Code, provide this context:

### Key Technical Details

1. **XTDB v2 SQL pointers**:
   - Standard pgx parameterized queries (`db.Query(ctx, "... $1", val)`) don't work
   - Parameterized queries work via low-level `conn.PgConn().ExecParams()` with explicit OIDs
   - This codebase uses `ExecParams` with JSON OID (114) to pass entire records as JSON (see `internal/xtdb/writer.go`)
   - Pattern: `INSERT INTO table RECORDS $1` with JSON-marshaled record
   - JSON approach stores timestamps as strings; use `CAST(col AS TIMESTAMPTZ)` in read queries for comparisons/ordering
   - `Ping()` doesn't work - use `SELECT 1` for health checks
   - Bitemporal joins use `FOR VALID_TIME ALL AS alias` on the table, not the JOIN clause
   - XTDB has no `DROP TABLE` - cycle the container to reset: `docker-compose rm -sf xtdb && docker volume rm temporal-decision-observability_xtdb-data && docker-compose up -d xtdb`

2. **Temporal event types**:
   - Events come as `"WorkflowExecutionStarted"`, not `"EVENT_TYPE_WORKFLOW_EXECUTION_STARTED"`
   - Activity names are in `ActivityTaskScheduled` events, not `ActivityTaskCompleted`

3. **Project conventions**:
   - CDC connector is the root `main.go`
   - Demo components are in `demo/` subdirectory
   - UI templates use Go's `embed` for single-binary deployment

### Common Tasks

**Add a new data source to the timeline:**
1. Create a loader in `demo/loaders/`
2. Add the UNION ALL clause in `handleTimeline()` in `demo/ui/main.go`
3. Add CSS for the timeline item type in `index.html`

**Add a new audit query:**
1. Add handler in `demo/ui/main.go`
2. Add API route in the `main()` function
3. Add UI tab and rendering function in `index.html`

**Track additional Temporal event fields:**
1. Update `internal/xtdb/writer.go` to extract fields from event attributes
2. Update the RECORDS insert statement

### Useful Commands

```bash
# Run the full demo with one command
./run-demo.sh

# Fresh start (removes all data)
./run-demo.sh --fresh

# Check what's running
docker-compose ps

# View CDC connector logs
docker logs cdc-connector -f

# Kill process on port 3000
fuser -k 3000/tcp

# Query XTDB
psql "postgres://localhost:5432/xtdb?sslmode=disable" -c "SELECT COUNT(*) FROM temporal_events"

# Query activity context tables
psql "postgres://localhost:5432/xtdb?sslmode=disable" -c "SELECT * FROM activity_routing_decisions LIMIT 5"

# Restart everything
docker-compose down && docker-compose up -d

# Full reset (removes volumes)
docker-compose down -v && docker-compose up -d
```

## Demo Talk Track

The UI includes a complete demo script accessible via:
- Click "Script [S]" in the bottom-right corner
- Press `s` key to toggle
- Press `Escape` to close

The talk track covers:
1. Problem framing (propagation lag, compliance nightmare)
2. Live demo walkthrough with click/scroll instructions
3. Activity Decision Context (direct XTDB writes)
4. Before/After Corrections (system-time travel)
5. Temporal's audit limitations (with user quotes from forums)
6. Why XTDB solves this
7. Closing and Q&A

### Demo Tabs

| Tab | Description |
|-----|-------------|
| Introduction | Overview, architecture diagram, two data capture approaches |
| Audit: Misrouted Customers | Bitemporal query showing customers routed despite high churn risk |
| Counterfactual Analysis | Compare ML predictions against actual outcomes |
| Event Timeline | Unified view of workflow events, predictions, and outcomes |
| Before/After Corrections | System-time travel demo showing model corrections |
| Workflow Events | Full payload inspection for any workflow |
| Activity Context | Direct activity writes: churn signals and routing decisions |

## License

[Add license here]

## Contributing

[Add contribution guidelines here]

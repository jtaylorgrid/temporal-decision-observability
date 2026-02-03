# Decision Observability Demo

This demo showcases **bitemporal audit** of AI agent decisions using Temporal + XTDB.

## Quick Start

From the project root, run:

```bash
./run-demo.sh
```

This starts everything automatically. Then open http://localhost:3000 and press `s` to see the demo talk track.

## Scenario

A customer service AI agent orchestrated by Temporal:
1. Analyzes ticket sentiment
2. Looks up customer profile (tier, LTV)
3. Checks churn risk signals (saves to XTDB)
4. Decides routing (saves decision context to XTDB)
5. Generates response draft

**The problem**: Churn predictions from the ML model have propagation lag. Customers may be routed to standard queue when they were *already* flagged as high-churn-risk in the source system.

**XTDB enables**: Bitemporal queries to audit "what was known at decision time" vs "what should have been known".

## Two Data Capture Approaches

1. **CDC via Kafka Connect**: Automatically streams Temporal workflow events. Captures *what the workflow did*.

2. **Direct Activity Writes**: Activities call `xtdb.Save(ctx)` to capture decision context. Records *what external systems believed* at decision time.

## Manual Setup (Alternative)

### 1. Start the infrastructure

```bash
# From project root
docker-compose up -d
```

Wait for services to be ready:
- Temporal UI: http://localhost:8080
- XTDB: localhost:5432
- Kafka Connect: http://localhost:8083

### 2. Configure Kafka Connect

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "xtdb-sink",
    "config": {
      "connector.class": "com.xtdb.kafka.connect.XtdbSinkConnector",
      "tasks.max": "1",
      "topics": "temporal-events,temporal-workflows",
      "xtdb.url": "jdbc:postgresql://xtdb:5432/xtdb"
    }
  }'
```

### 3. Start the demo worker (with XTDB integration)

```bash
XTDB_CONN_STRING="postgres://localhost:5432/xtdb?sslmode=disable" go run ./demo/worker
```

### 4. Load external data (simulates ML model output)

```bash
# Load churn predictions (with their valid-time)
go run ./demo/loaders/churn.go

# Load customer outcomes (what actually happened)
go run ./demo/loaders/outcomes.go
```

### 5. Start sample workflows

```bash
go run ./demo/starter
```

### 6. Open the Decision Observability UI

```bash
XTDB_CONN_STRING="postgres://localhost:5432/xtdb?sslmode=disable" go run ./demo/ui
```

Visit http://localhost:3000

## What You'll See

### Audit: Misrouted Customers
Customers who were routed to standard queue despite being flagged as high-churn-risk at the time of the decision.

```sql
SELECT workflow_id, decision_time, customer_id, churn_score
FROM temporal_events e
JOIN churn_predictions c
  FOR VALID_TIME AS OF e.event_time  -- Bitemporal point-in-time lookup!
  ON c.customer_id = ...
WHERE c.churn_score > 0.7
```

### Counterfactual Analysis
Of the customers who were flagged as high-risk, how many actually churned?

### Event Timeline
Unified view of all events (workflows, churn predictions, outcomes) ordered by when they occurred.

## Key XTDB Concepts

| Column | Meaning |
|--------|---------|
| `_valid_from` | When the fact was true in the real world |
| `_system_from` | When XTDB recorded it |

This enables:
- **Point-in-time queries**: `FOR VALID_TIME AS OF timestamp`
- **Audit queries**: `FOR SYSTEM_TIME AS OF timestamp`
- **Full history**: `FOR VALID_TIME ALL FOR SYSTEM_TIME ALL`

## Components

| Component | Description |
|-----------|-------------|
| `demo/workflow/` | Temporal workflow and activities (with XTDB integration) |
| `demo/worker/` | Temporal worker (connects to XTDB when `XTDB_CONN_STRING` is set) |
| `demo/starter/` | Creates sample workflows |
| `demo/xtdb/` | XTDB client helper for direct activity writes |
| `demo/loaders/` | Loads churn predictions, outcomes, and corrections |
| `demo/ui/` | Web UI for visualization and audit queries |

## UI Tabs

| Tab | Description |
|-----|-------------|
| Introduction | Overview, architecture, data capture approaches |
| Audit: Misrouted Customers | Bitemporal query for compliance audit |
| Counterfactual Analysis | Predicted vs actual outcomes |
| Event Timeline | Unified view of all events |
| Before/After Corrections | System-time travel demo |
| Workflow Events | Full payload inspection |
| Activity Context | Direct activity writes (churn signals, routing decisions) |

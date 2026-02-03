#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_step() {
    echo -e "${BLUE}==>${NC} $1"
}

print_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

wait_for_service() {
    local name=$1
    local url=$2
    local max_attempts=${3:-30}
    local attempt=1

    echo -n "  Waiting for $name..."
    while [ $attempt -le $max_attempts ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            echo -e " ${GREEN}ready${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    echo -e " ${RED}timeout${NC}"
    return 1
}

wait_for_postgres() {
    local name=$1
    local host=$2
    local port=$3
    local max_attempts=${4:-30}
    local attempt=1

    echo -n "  Waiting for $name..."
    while [ $attempt -le $max_attempts ]; do
        if pg_isready -h "$host" -p "$port" > /dev/null 2>&1; then
            echo -e " ${GREEN}ready${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    echo -e " ${RED}timeout${NC}"
    return 1
}

cleanup() {
    print_step "Cleaning up background processes..."
    if [ -n "$WORKER_PID" ]; then
        kill $WORKER_PID 2>/dev/null || true
    fi
    if [ -n "$UI_PID" ]; then
        kill $UI_PID 2>/dev/null || true
    fi
    exit 0
}

trap cleanup SIGINT SIGTERM

# Check prerequisites
print_step "Checking prerequisites..."
command -v docker >/dev/null 2>&1 || { print_error "docker is required but not installed"; exit 1; }
command -v docker-compose >/dev/null 2>&1 || command -v "docker compose" >/dev/null 2>&1 || { print_error "docker-compose is required but not installed"; exit 1; }
command -v go >/dev/null 2>&1 || { print_error "go is required but not installed"; exit 1; }
command -v curl >/dev/null 2>&1 || { print_error "curl is required but not installed"; exit 1; }
print_success "All prerequisites found"

# Determine docker compose command
if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
else
    COMPOSE="docker compose"
fi

# Parse arguments
SKIP_INFRA=false
SKIP_DATA=false
FRESH=false

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --skip-infra) SKIP_INFRA=true ;;
        --skip-data) SKIP_DATA=true ;;
        --fresh) FRESH=true ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --skip-infra   Skip starting Docker infrastructure (assume already running)"
            echo "  --skip-data    Skip loading demo data (churn predictions, outcomes)"
            echo "  --fresh        Fresh start: remove volumes and restart everything"
            echo "  --help, -h     Show this help message"
            echo ""
            echo "The script will:"
            echo "  1. Start Docker infrastructure (Temporal, XTDB, Kafka, Kafka Connect)"
            echo "  2. Configure Kafka Connect XTDB sink connector"
            echo "  3. Start the Temporal worker"
            echo "  4. Load demo data (churn predictions, customer outcomes)"
            echo "  5. Start sample workflows"
            echo "  6. Start the demo UI"
            echo ""
            echo "After starting, open http://localhost:3000 for the demo UI"
            echo "Press 's' in the UI to toggle the demo talk track"
            exit 0
            ;;
        *) print_error "Unknown option: $1"; exit 1 ;;
    esac
    shift
done

# Fresh start - remove volumes
if [ "$FRESH" = true ]; then
    print_step "Fresh start: stopping services and removing volumes..."
    $COMPOSE down -v 2>/dev/null || true
    print_success "Volumes removed"
fi

# Start infrastructure
if [ "$SKIP_INFRA" = false ]; then
    print_step "Starting Docker infrastructure..."
    $COMPOSE up -d
    print_success "Docker containers started"

    print_step "Waiting for services to be ready..."
    wait_for_service "Temporal" "http://localhost:8080" || { print_error "Temporal failed to start"; exit 1; }
    wait_for_postgres "XTDB" "localhost" "5432" || { print_error "XTDB failed to start"; exit 1; }
    wait_for_service "Kafka Connect" "http://localhost:8083/connectors" 60 || { print_error "Kafka Connect failed to start"; exit 1; }
    print_success "All services ready"
fi

# Configure Kafka Connect XTDB sink
print_step "Configuring Kafka Connect XTDB sink connector..."
CONNECTOR_EXISTS=$(curl -sf http://localhost:8083/connectors/xtdb-sink 2>/dev/null || echo "")

if [ -z "$CONNECTOR_EXISTS" ]; then
    curl -sf -X POST http://localhost:8083/connectors \
        -H "Content-Type: application/json" \
        -d '{
            "name": "xtdb-sink",
            "config": {
                "connector.class": "com.xtdb.kafka.connect.XtdbSinkConnector",
                "tasks.max": "1",
                "topics": "temporal-events,temporal-workflows",
                "xtdb.url": "jdbc:postgresql://xtdb:5432/xtdb",
                "xtdb.user": "xtdb",
                "xtdb.password": "xtdb",
                "xtdb.batch.size": "100"
            }
        }' > /dev/null
    print_success "Kafka Connect XTDB sink configured"
else
    print_success "Kafka Connect XTDB sink already configured"
fi

# Build Go binaries
print_step "Building Go binaries..."
go build -o /tmp/demo-worker ./demo/worker 2>/dev/null
go build -o /tmp/demo-ui ./demo/ui 2>/dev/null
go build -o /tmp/demo-starter ./demo/starter 2>/dev/null
print_success "Go binaries built"

# Start the demo worker with XTDB connection
print_step "Starting Temporal worker (with XTDB integration)..."
XTDB_CONN_STRING="postgres://localhost:5432/xtdb?sslmode=disable" /tmp/demo-worker &
WORKER_PID=$!
sleep 2
if kill -0 $WORKER_PID 2>/dev/null; then
    print_success "Worker started (PID: $WORKER_PID)"
else
    print_error "Worker failed to start"
    exit 1
fi

# Load demo data
if [ "$SKIP_DATA" = false ]; then
    print_step "Loading demo data..."

    echo "  Loading churn predictions..."
    go run ./demo/loaders/churn.go 2>/dev/null
    print_success "Churn predictions loaded"

    echo "  Loading customer outcomes..."
    go run ./demo/loaders/outcomes.go 2>/dev/null
    print_success "Customer outcomes loaded"
fi

# Start sample workflows
print_step "Starting sample workflows..."
/tmp/demo-starter 2>/dev/null
print_success "Sample workflows started"

# Wait for CDC to catch up
print_step "Waiting for CDC to process events (10 seconds)..."
sleep 10
print_success "CDC catchup complete"

# Load corrections (for before/after demo)
if [ "$SKIP_DATA" = false ]; then
    print_step "Loading model corrections (for before/after demo)..."
    go run ./demo/loaders/corrections.go 2>/dev/null || print_warning "Corrections loader not found (optional)"
fi

# Start the UI
print_step "Starting demo UI..."
XTDB_CONN_STRING="postgres://localhost:5432/xtdb?sslmode=disable" /tmp/demo-ui &
UI_PID=$!
sleep 2
if kill -0 $UI_PID 2>/dev/null; then
    print_success "UI started (PID: $UI_PID)"
else
    print_error "UI failed to start"
    exit 1
fi

# Summary
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Demo is running!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "  Demo UI:       http://localhost:3000"
echo "  Temporal UI:   http://localhost:8080"
echo "  XTDB:          localhost:5432 (PostgreSQL wire protocol)"
echo ""
echo "  Press 's' in the demo UI to toggle the talk track"
echo ""
echo "  Tabs available:"
echo "    - Introduction: Overview and architecture"
echo "    - Audit: Misrouted Customers: Bitemporal audit query"
echo "    - Counterfactual Analysis: Predicted vs actual outcomes"
echo "    - Event Timeline: Unified view of all events"
echo "    - Before/After Corrections: System-time travel demo"
echo "    - Workflow Events: Full payload inspection"
echo "    - Activity Context: Direct activity writes to XTDB"
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop the demo${NC}"
echo ""

# Wait for Ctrl+C
wait

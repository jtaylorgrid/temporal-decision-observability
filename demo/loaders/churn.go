package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()

	connString := os.Getenv("XTDB_CONN_STRING")
	if connString == "" {
		connString = "postgres://localhost:5432/xtdb?sslmode=disable"
	}

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		log.Fatal("connect:", err)
	}
	defer conn.Close(ctx)

	predictions := []struct {
		CustomerID string
		ChurnScore float64
		RiskLevel  string
		ValidFrom  time.Time
		ModelVersion string
	}{
		{"CUST-001", 0.85, "high", time.Now().Add(-2 * time.Hour), "v2.1"},
		{"CUST-002", 0.35, "low", time.Now().Add(-2 * time.Hour), "v2.1"},
		{"CUST-003", 0.92, "high", time.Now().Add(-90 * time.Minute), "v2.1"},
		{"CUST-004", 0.15, "low", time.Now().Add(-2 * time.Hour), "v2.1"},
		{"CUST-005", 0.78, "high", time.Now().Add(-75 * time.Minute), "v2.1"},
		{"CUST-006", 0.45, "medium", time.Now().Add(-2 * time.Hour), "v2.1"},
		{"CUST-007", 0.88, "high", time.Now().Add(-60 * time.Minute), "v2.1"},
		{"CUST-008", 0.22, "low", time.Now().Add(-2 * time.Hour), "v2.1"},
		{"CUST-009", 0.95, "high", time.Now().Add(-45 * time.Minute), "v2.1"},
		{"CUST-010", 0.55, "medium", time.Now().Add(-2 * time.Hour), "v2.1"},
	}

	log.Println("Loading churn predictions into XTDB...")

	for _, p := range predictions {
		id := fmt.Sprintf("churn:%s", p.CustomerID)
		sql := fmt.Sprintf(`INSERT INTO churn_predictions RECORDS {
			_id: %s,
			_valid_from: TIMESTAMP %s,
			customer_id: %s,
			churn_score: %f,
			risk_level: %s,
			model_version: %s,
			computed_at: TIMESTAMP %s
		}`,
			quote(id),
			quote(p.ValidFrom.Format(time.RFC3339Nano)),
			quote(p.CustomerID),
			p.ChurnScore,
			quote(p.RiskLevel),
			quote(p.ModelVersion),
			quote(p.ValidFrom.Format(time.RFC3339Nano)),
		)

		_, err := conn.Exec(ctx, sql)
		if err != nil {
			log.Printf("Error inserting %s: %v", p.CustomerID, err)
		} else {
			log.Printf("Loaded churn prediction for %s (score=%.2f, valid_from=%s)",
				p.CustomerID, p.ChurnScore, p.ValidFrom.Format(time.RFC3339))
		}
	}

	log.Println("Done loading churn predictions")
}

func quote(s string) string {
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'"
}

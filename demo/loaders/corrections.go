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

	// Record the pre-correction timestamp before inserting corrections.
	// This allows the UI to use SETTING DEFAULT SYSTEM_TIME AS OF to show
	// what the database believed before these corrections arrived.
	preCorrection := time.Now()
	preCorrectionStr := preCorrection.Format(time.RFC3339Nano)

	log.Printf("Pre-correction timestamp: %s", preCorrectionStr)
	log.Println("Inserting correction marker...")

	markerSQL := fmt.Sprintf(`INSERT INTO correction_markers RECORDS {
		_id: %s,
		_valid_from: TIMESTAMP %s,
		correction_type: %s,
		pre_correction_time: %s,
		description: %s
	}`,
		quote("corrections:churn-v2.2"),
		quote(preCorrection.Format(time.RFC3339Nano)),
		quote("churn_model_update"),
		quote(preCorrectionStr),
		quote("Model v2.2 corrections for CUST-002, CUST-006, CUST-010"),
	)

	_, err = conn.Exec(ctx, markerSQL)
	if err != nil {
		log.Fatal("insert correction marker:", err)
	}
	log.Println("Correction marker inserted")

	// Wait to ensure a system-time gap between the marker and the corrections
	log.Println("Waiting 2 seconds for system-time gap...")
	time.Sleep(2 * time.Second)

	// These corrections use the SAME _id values as the original churn loader,
	// so XTDB records them as new system-time versions of the same entity.
	// The _valid_from matches the original predictions' valid times.
	corrections := []struct {
		CustomerID   string
		ChurnScore   float64
		RiskLevel    string
		ValidFrom    time.Time
		ModelVersion string
	}{
		{"CUST-002", 0.82, "high", time.Now().Add(-2 * time.Hour), "v2.2"},
		{"CUST-006", 0.76, "high", time.Now().Add(-2 * time.Hour), "v2.2"},
		{"CUST-010", 0.71, "high", time.Now().Add(-2 * time.Hour), "v2.2"},
	}

	log.Println("Loading corrected churn predictions into XTDB...")

	for _, p := range corrections {
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
			quote(time.Now().Format(time.RFC3339Nano)),
		)

		_, err := conn.Exec(ctx, sql)
		if err != nil {
			log.Printf("Error inserting correction for %s: %v", p.CustomerID, err)
		} else {
			log.Printf("Loaded correction for %s (score=%.2f, risk=%s, model=%s)",
				p.CustomerID, p.ChurnScore, p.RiskLevel, p.ModelVersion)
		}
	}

	log.Printf("Done loading corrections. Pre-correction time: %s", preCorrectionStr)
	log.Println("The UI 'Before/After' tab will use this timestamp to show the difference.")
}

func quote(s string) string {
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'"
}

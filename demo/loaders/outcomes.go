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

	outcomes := []struct {
		CustomerID  string
		Churned     bool
		ChurnDate   *time.Time
		Reason      string
		FinalLTV    float64
		RecordedAt  time.Time
	}{
		{"CUST-001", true, timePtr(time.Now().Add(-1 * time.Hour)), "competitor_offer", 2500.00, time.Now()},
		{"CUST-002", false, nil, "", 850.00, time.Now()},
		{"CUST-003", true, timePtr(time.Now().Add(-30 * time.Minute)), "poor_support_experience", 5200.00, time.Now()},
		{"CUST-004", false, nil, "", 320.00, time.Now()},
		{"CUST-005", true, timePtr(time.Now().Add(-45 * time.Minute)), "pricing", 1800.00, time.Now()},
		{"CUST-006", false, nil, "", 620.00, time.Now()},
		{"CUST-007", true, timePtr(time.Now().Add(-20 * time.Minute)), "product_issues", 3100.00, time.Now()},
		{"CUST-008", false, nil, "", 180.00, time.Now()},
		{"CUST-009", true, timePtr(time.Now().Add(-15 * time.Minute)), "competitor_offer", 8500.00, time.Now()},
		{"CUST-010", false, nil, "", 550.00, time.Now()},
	}

	log.Println("Loading customer outcomes into XTDB...")

	for _, o := range outcomes {
		id := fmt.Sprintf("outcome:%s", o.CustomerID)

		churnDateStr := "NULL"
		if o.ChurnDate != nil {
			churnDateStr = fmt.Sprintf("TIMESTAMP %s", quote(o.ChurnDate.Format(time.RFC3339Nano)))
		}

		reasonStr := "NULL"
		if o.Reason != "" {
			reasonStr = quote(o.Reason)
		}

		sql := fmt.Sprintf(`INSERT INTO customer_outcomes RECORDS {
			_id: %s,
			_valid_from: TIMESTAMP %s,
			customer_id: %s,
			churned: %t,
			churn_date: %s,
			churn_reason: %s,
			final_ltv: %f,
			recorded_at: TIMESTAMP %s
		}`,
			quote(id),
			quote(o.RecordedAt.Format(time.RFC3339Nano)),
			quote(o.CustomerID),
			o.Churned,
			churnDateStr,
			reasonStr,
			o.FinalLTV,
			quote(o.RecordedAt.Format(time.RFC3339Nano)),
		)

		_, err := conn.Exec(ctx, sql)
		if err != nil {
			log.Printf("Error inserting %s: %v", o.CustomerID, err)
		} else {
			status := "retained"
			if o.Churned {
				status = "churned"
			}
			log.Printf("Loaded outcome for %s (%s, LTV=$%.2f)", o.CustomerID, status, o.FinalLTV)
		}
	}

	log.Println("Done loading customer outcomes")
}

func quote(s string) string {
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'"
}

func timePtr(t time.Time) *time.Time {
	return &t
}

package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed templates/*
var templates embed.FS

var tmpl *template.Template
var db *pgxpool.Pool

func main() {
	var err error
	tmpl, err = template.ParseFS(templates, "templates/*.html")
	if err != nil {
		log.Fatal("parse templates:", err)
	}

	connString := os.Getenv("XTDB_CONN_STRING")
	if connString == "" {
		connString = "postgres://localhost:5432/xtdb?sslmode=disable"
	}

	db, err = pgxpool.New(context.Background(), connString)
	if err != nil {
		log.Fatal("connect to XTDB:", err)
	}
	defer db.Close()

	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/api/workflows", handleWorkflows)
	http.HandleFunc("/api/events/", handleWorkflowEvents)
	http.HandleFunc("/api/audit/misrouted", handleMisroutedAudit)
	http.HandleFunc("/api/audit/counterfactual", handleCounterfactual)
	http.HandleFunc("/api/timeline", handleTimeline)
	http.HandleFunc("/api/audit/before-after", handleBeforeAfter)
	http.HandleFunc("/api/activity/churn-signals", handleActivityChurnSignals)
	http.HandleFunc("/api/activity/routing-decisions", handleActivityRoutingDecisions)
	http.HandleFunc("/api/export", handleExport)
	http.HandleFunc("/architecture.svg", handleArchitectureSVG)

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}

	log.Printf("Starting Decision Observability UI on http://localhost:%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if err := tmpl.ExecuteTemplate(w, "index.html", nil); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

func handleWorkflows(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	rows, err := db.Query(ctx, `
		SELECT workflow_id, namespace, run_id, workflow_type, status,
			start_time, close_time
		FROM workflows
		ORDER BY _valid_from DESC
		LIMIT 50
	`)
	if err != nil {
		jsonError(w, err, 500)
		return
	}
	defer rows.Close()

	var workflows []map[string]any
	for rows.Next() {
		var workflowID, namespace, runID, workflowType, status string
		var startTime string
		var closeTime *string

		if err := rows.Scan(&workflowID, &namespace, &runID, &workflowType, &status, &startTime, &closeTime); err != nil {
			continue
		}

		wf := map[string]any{
			"workflow_id":   workflowID,
			"namespace":     namespace,
			"run_id":        runID,
			"workflow_type": workflowType,
			"status":        status,
			"start_time":    startTime,
		}
		if closeTime != nil {
			wf["close_time"] = *closeTime
		}
		workflows = append(workflows, wf)
	}

	jsonResponse(w, workflows)
}

func handleWorkflowEvents(w http.ResponseWriter, r *http.Request) {
	workflowID := strings.TrimPrefix(r.URL.Path, "/api/events/")
	if workflowID == "" {
		jsonError(w, fmt.Errorf("workflow_id required"), 400)
		return
	}

	ctx := r.Context()
	// Get events from the most recent run only
	sql := fmt.Sprintf(`
		SELECT event_id, event_type, CAST(event_time AS TIMESTAMPTZ),
			workflow_type, task_queue, event_data
		FROM events
		WHERE workflow_id = %s
		  AND run_id = (
		    SELECT run_id FROM events
		    WHERE workflow_id = %s
		    ORDER BY _valid_from DESC
		    LIMIT 1
		  )
		ORDER BY event_id
	`, quote(workflowID), quote(workflowID))

	rows, err := db.Query(ctx, sql)
	if err != nil {
		jsonError(w, err, 500)
		return
	}
	defer rows.Close()

	var events []map[string]any
	for rows.Next() {
		var eventID int64
		var eventType, workflowType, taskQueue string
		var eventTime time.Time
		var eventData any

		if err := rows.Scan(&eventID, &eventType, &eventTime, &workflowType, &taskQueue, &eventData); err != nil {
			continue
		}

		evt := map[string]any{
			"event_id":      eventID,
			"event_type":    eventType,
			"event_time":    eventTime.Format(time.RFC3339),
			"workflow_type": workflowType,
			"task_queue":    taskQueue,
		}
		if eventData != nil {
			evt["event_data"] = eventData
		}
		events = append(events, evt)
	}

	jsonResponse(w, events)
}

func handleMisroutedAudit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Bitemporal audit query: find workflows where the customer was already flagged
	// as high churn risk at the time the workflow started (decision process began)
	// workflow_id format: cs-workflow-TKT-001 -> customer_id: CUST-001
	// Note: event_time is stored as JSON string, CAST to TIMESTAMPTZ for comparisons
	sql := `
		SELECT
			e.workflow_id,
			CAST(e.event_time AS TIMESTAMPTZ) as decision_time,
			c.customer_id,
			c.churn_score,
			c.risk_level,
			c._valid_from as churn_flagged_at
		FROM temporal_events e
		JOIN churn_predictions FOR VALID_TIME ALL AS c
			ON c.customer_id = 'CUST-' || REPLACE(e.workflow_id, 'cs-workflow-TKT-', '')
		WHERE e.event_type = 'WorkflowExecutionStarted'
		  AND e.workflow_id LIKE 'cs-workflow-TKT-%'
		  AND c.churn_score > 0.7
		  AND c._valid_from <= CAST(e.event_time AS TIMESTAMPTZ)
		  AND (c._valid_to IS NULL OR c._valid_to > CAST(e.event_time AS TIMESTAMPTZ))
		ORDER BY CAST(e.event_time AS TIMESTAMPTZ) DESC
	`

	rows, err := db.Query(ctx, sql)
	if err != nil {
		jsonError(w, err, 500)
		return
	}
	defer rows.Close()

	var results []map[string]any
	for rows.Next() {
		var workflowID, customerID, riskLevel string
		var decisionTime, churnFlaggedAt time.Time
		var churnScore float64

		if err := rows.Scan(&workflowID, &decisionTime, &customerID, &churnScore, &riskLevel, &churnFlaggedAt); err != nil {
			continue
		}

		results = append(results, map[string]any{
			"workflow_id":      workflowID,
			"decision_time":    decisionTime.Format(time.RFC3339),
			"customer_id":      customerID,
			"churn_score":      churnScore,
			"risk_level":       riskLevel,
			"churn_flagged_at": churnFlaggedAt.Format(time.RFC3339),
			"lag_seconds":      decisionTime.Sub(churnFlaggedAt).Seconds(),
		})
	}

	jsonResponse(w, map[string]any{
		"description": "Customers routed to standard queue who were already flagged as high churn risk",
		"count":       len(results),
		"results":     results,
	})
}

func handleCounterfactual(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	sql := `
		SELECT
			c.customer_id,
			c.churn_score as score_at_model_time,
			o.churned,
			o.churn_reason,
			o.final_ltv
		FROM churn_predictions c
		JOIN customer_outcomes o ON o.customer_id = c.customer_id
		WHERE c.churn_score > 0.7
		ORDER BY c.churn_score DESC
	`

	rows, err := db.Query(ctx, sql)
	if err != nil {
		jsonError(w, err, 500)
		return
	}
	defer rows.Close()

	var results []map[string]any
	var totalLostLTV float64
	var churned, retained int

	for rows.Next() {
		var customerID string
		var churnScore, finalLTV float64
		var didChurn bool
		var churnReason *string

		if err := rows.Scan(&customerID, &churnScore, &didChurn, &churnReason, &finalLTV); err != nil {
			continue
		}

		reason := ""
		if churnReason != nil {
			reason = *churnReason
		}

		if didChurn {
			churned++
			totalLostLTV += finalLTV
		} else {
			retained++
		}

		results = append(results, map[string]any{
			"customer_id":  customerID,
			"churn_score":  churnScore,
			"churned":      didChurn,
			"churn_reason": reason,
			"final_ltv":    finalLTV,
		})
	}

	jsonResponse(w, map[string]any{
		"description":    "High-risk customers (score > 0.7) and their actual outcomes",
		"high_risk_total": len(results),
		"churned":        churned,
		"retained":       retained,
		"total_lost_ltv": totalLostLTV,
		"results":        results,
	})
}

func handleTimeline(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	sql := `
		SELECT
			'workflow' as source,
			workflow_id as id,
			CAST(start_time AS TIMESTAMPTZ) as event_time,
			status as description,
			_system_from as recorded_at
		FROM temporal_workflows
		UNION ALL
		SELECT
			'churn_prediction' as source,
			customer_id as id,
			_valid_from as event_time,
			'Churn score: ' || CAST(churn_score AS VARCHAR) as description,
			_system_from as recorded_at
		FROM churn_predictions
		UNION ALL
		SELECT
			'outcome' as source,
			customer_id as id,
			_valid_from as event_time,
			CASE WHEN churned THEN 'CHURNED: ' || COALESCE(churn_reason, 'unknown') ELSE 'Retained' END as description,
			_system_from as recorded_at
		FROM customer_outcomes
		ORDER BY event_time DESC
		LIMIT 100
	`

	rows, err := db.Query(ctx, sql)
	if err != nil {
		jsonError(w, err, 500)
		return
	}
	defer rows.Close()

	var timeline []map[string]any
	for rows.Next() {
		var source, id, description string
		var eventTime, recordedAt time.Time

		if err := rows.Scan(&source, &id, &eventTime, &description, &recordedAt); err != nil {
			continue
		}

		timeline = append(timeline, map[string]any{
			"source":      source,
			"id":          id,
			"event_time":  eventTime.Format(time.RFC3339),
			"description": description,
			"recorded_at": recordedAt.Format(time.RFC3339),
		})
	}

	jsonResponse(w, timeline)
}

func handleBeforeAfter(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Step 1: Get the pre-correction timestamp from the marker record
	var preCorrectionTime string
	err := db.QueryRow(ctx, `
		SELECT pre_correction_time FROM correction_markers
		WHERE _id = 'corrections:churn-v2.2'
	`).Scan(&preCorrectionTime)
	if err != nil {
		jsonResponse(w, map[string]any{
			"error":       "Corrections not loaded yet. Run: go run ./demo/loaders/corrections.go",
			"description": "Load model corrections to see before/after comparison",
		})
		return
	}

	// The misrouted audit query (shared between before and after)
	auditQuery := `
		SELECT
			e.workflow_id,
			CAST(e.event_time AS TIMESTAMPTZ) as decision_time,
			c.customer_id,
			c.churn_score,
			c.risk_level,
			c._valid_from as churn_flagged_at
		FROM temporal_events e
		JOIN churn_predictions FOR VALID_TIME ALL AS c
			ON c.customer_id = 'CUST-' || REPLACE(e.workflow_id, 'cs-workflow-TKT-', '')
		WHERE e.event_type = 'WorkflowExecutionStarted'
		  AND e.workflow_id LIKE 'cs-workflow-TKT-%%'
		  AND c.churn_score > 0.7
		  AND c._valid_from <= CAST(e.event_time AS TIMESTAMPTZ)
		  AND (c._valid_to IS NULL OR c._valid_to > CAST(e.event_time AS TIMESTAMPTZ))
		ORDER BY CAST(e.event_time AS TIMESTAMPTZ) DESC
	`

	// Step 2: Run the "before" query with SETTING DEFAULT SYSTEM_TIME
	beforeSQL := fmt.Sprintf(
		"SETTING DEFAULT SYSTEM_TIME TO AS OF TIMESTAMP %s %s",
		quote(preCorrectionTime),
		auditQuery,
	)

	beforeResults, err := runAuditQuery(ctx, beforeSQL)
	if err != nil {
		jsonError(w, fmt.Errorf("before query: %w", err), 500)
		return
	}

	// Step 3: Run the "after" query (current state)
	afterResults, err := runAuditQuery(ctx, auditQuery)
	if err != nil {
		jsonError(w, fmt.Errorf("after query: %w", err), 500)
		return
	}

	// Step 4: Compute new detections (in after but not in before)
	beforeIDs := make(map[string]bool)
	for _, r := range beforeResults {
		if wfID, ok := r["workflow_id"].(string); ok {
			beforeIDs[wfID] = true
		}
	}

	var newDetections []map[string]any
	for _, r := range afterResults {
		if wfID, ok := r["workflow_id"].(string); ok {
			if !beforeIDs[wfID] {
				newDetections = append(newDetections, r)
			}
		}
	}

	jsonResponse(w, map[string]any{
		"description":         "Before/after comparison using SETTING DEFAULT SYSTEM_TIME AS OF",
		"pre_correction_time": preCorrectionTime,
		"before": map[string]any{
			"count":   len(beforeResults),
			"results": beforeResults,
		},
		"after": map[string]any{
			"count":   len(afterResults),
			"results": afterResults,
		},
		"new_detections": map[string]any{
			"count":   len(newDetections),
			"results": newDetections,
		},
	})
}

func handleActivityChurnSignals(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	rows, err := db.Query(ctx, `
		SELECT _id, workflow_id, run_id, activity_id, customer_id,
			churn_score, risk_level, signal_source, last_activity_at, _valid_from
		FROM activity_churn_signals
		ORDER BY _valid_from DESC
		LIMIT 50
	`)
	if err != nil {
		// Table might not exist yet
		jsonResponse(w, []map[string]any{})
		return
	}
	defer rows.Close()

	var results []map[string]any
	for rows.Next() {
		var id string
		var workflowID, runID, activityID, customerID *string
		var churnScore *float64
		var riskLevel, signalSource, lastActivityAt *string
		var validFrom time.Time

		if err := rows.Scan(&id, &workflowID, &runID, &activityID, &customerID,
			&churnScore, &riskLevel, &signalSource, &lastActivityAt, &validFrom); err != nil {
			continue
		}

		result := map[string]any{
			"id":         id,
			"valid_from": validFrom.Format(time.RFC3339),
		}
		if workflowID != nil {
			result["workflow_id"] = *workflowID
		}
		if runID != nil {
			result["run_id"] = *runID
		}
		if activityID != nil {
			result["activity_id"] = *activityID
		}
		if customerID != nil {
			result["customer_id"] = *customerID
		}
		if churnScore != nil {
			result["churn_score"] = *churnScore
		}
		if riskLevel != nil {
			result["risk_level"] = *riskLevel
		}
		if signalSource != nil {
			result["signal_source"] = *signalSource
		}
		if lastActivityAt != nil {
			result["last_activity_at"] = *lastActivityAt
		}

		results = append(results, result)
	}

	jsonResponse(w, results)
}

func handleActivityRoutingDecisions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	rows, err := db.Query(ctx, `
		SELECT _id, workflow_id, run_id, activity_id, ticket_id, customer_id,
			queue, priority, reason_codes, escalation_level,
			input_churn_score, input_churn_risk, input_sentiment, input_urgency,
			input_customer_tier, input_customer_ltv, _valid_from
		FROM activity_routing_decisions
		ORDER BY _valid_from DESC
		LIMIT 50
	`)
	if err != nil {
		// Table might not exist yet
		jsonResponse(w, []map[string]any{})
		return
	}
	defer rows.Close()

	var results []map[string]any
	for rows.Next() {
		var id string
		var workflowID, runID, activityID, ticketID, customerID *string
		var queue *string
		var priority, escalationLevel *int
		var reasonCodes *string
		var inputChurnScore, inputCustomerLTV *float64
		var inputChurnRisk, inputSentiment, inputUrgency, inputCustomerTier *string
		var validFrom time.Time

		if err := rows.Scan(&id, &workflowID, &runID, &activityID, &ticketID, &customerID,
			&queue, &priority, &reasonCodes, &escalationLevel,
			&inputChurnScore, &inputChurnRisk, &inputSentiment, &inputUrgency,
			&inputCustomerTier, &inputCustomerLTV, &validFrom); err != nil {
			continue
		}

		result := map[string]any{
			"id":         id,
			"valid_from": validFrom.Format(time.RFC3339),
		}
		if workflowID != nil {
			result["workflow_id"] = *workflowID
		}
		if runID != nil {
			result["run_id"] = *runID
		}
		if activityID != nil {
			result["activity_id"] = *activityID
		}
		if ticketID != nil {
			result["ticket_id"] = *ticketID
		}
		if customerID != nil {
			result["customer_id"] = *customerID
		}
		if queue != nil {
			result["queue"] = *queue
		}
		if priority != nil {
			result["priority"] = *priority
		}
		if reasonCodes != nil {
			result["reason_codes"] = *reasonCodes
		}
		if escalationLevel != nil {
			result["escalation_level"] = *escalationLevel
		}
		if inputChurnScore != nil {
			result["input_churn_score"] = *inputChurnScore
		}
		if inputChurnRisk != nil {
			result["input_churn_risk"] = *inputChurnRisk
		}
		if inputSentiment != nil {
			result["input_sentiment"] = *inputSentiment
		}
		if inputUrgency != nil {
			result["input_urgency"] = *inputUrgency
		}
		if inputCustomerTier != nil {
			result["input_customer_tier"] = *inputCustomerTier
		}
		if inputCustomerLTV != nil {
			result["input_customer_ltv"] = *inputCustomerLTV
		}

		results = append(results, result)
	}

	jsonResponse(w, results)
}

func runAuditQuery(ctx context.Context, sql string) ([]map[string]any, error) {
	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]any
	for rows.Next() {
		var workflowID, customerID, riskLevel string
		var decisionTime, churnFlaggedAt time.Time
		var churnScore float64

		if err := rows.Scan(&workflowID, &decisionTime, &customerID, &churnScore, &riskLevel, &churnFlaggedAt); err != nil {
			continue
		}

		results = append(results, map[string]any{
			"workflow_id":      workflowID,
			"decision_time":    decisionTime.Format(time.RFC3339),
			"customer_id":      customerID,
			"churn_score":      churnScore,
			"risk_level":       riskLevel,
			"churn_flagged_at": churnFlaggedAt.Format(time.RFC3339),
			"lag_seconds":      decisionTime.Sub(churnFlaggedAt).Seconds(),
		})
	}

	return results, nil
}

func jsonResponse(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func jsonError(w http.ResponseWriter, err error, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func quote(s string) string {
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'"
}

//go:embed architecture.svg
var architectureSVG []byte

func handleArchitectureSVG(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "image/svg+xml")
	w.Write(architectureSVG)
}

func handleExport(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Gather all data from the various API endpoints
	exportData := make(map[string]any)

	// Get workflows
	workflows, err := getWorkflowsData(ctx)
	if err != nil {
		jsonError(w, fmt.Errorf("workflows: %w", err), 500)
		return
	}
	exportData["workflows"] = workflows

	// Get timeline
	timeline, err := getTimelineData(ctx)
	if err != nil {
		jsonError(w, fmt.Errorf("timeline: %w", err), 500)
		return
	}
	exportData["timeline"] = timeline

	// Get misrouted audit
	misrouted, err := getMisroutedData(ctx)
	if err != nil {
		jsonError(w, fmt.Errorf("misrouted: %w", err), 500)
		return
	}
	exportData["misrouted"] = misrouted

	// Get counterfactual
	counterfactual, err := getCounterfactualData(ctx)
	if err != nil {
		jsonError(w, fmt.Errorf("counterfactual: %w", err), 500)
		return
	}
	exportData["counterfactual"] = counterfactual

	// Get before/after
	beforeAfter, err := getBeforeAfterData(ctx)
	if err != nil {
		// Non-fatal - corrections might not be loaded
		exportData["beforeAfter"] = map[string]any{"error": err.Error()}
	} else {
		exportData["beforeAfter"] = beforeAfter
	}

	// Get events for each workflow
	workflowEvents := make(map[string]any)
	for _, wf := range workflows {
		if wfID, ok := wf["workflow_id"].(string); ok {
			events, err := getWorkflowEventsData(ctx, wfID)
			if err == nil {
				workflowEvents[wfID] = events
			}
		}
	}
	exportData["workflowEvents"] = workflowEvents

	// Generate static HTML using the actual template
	dataJSON, _ := json.Marshal(exportData)

	// Read the template file
	templateContent, err := templates.ReadFile("templates/index.html")
	if err != nil {
		jsonError(w, fmt.Errorf("read template: %w", err), 500)
		return
	}

	html := string(templateContent)

	// Replace the architecture.svg reference with inline SVG
	html = strings.Replace(html,
		`<img src="/architecture.svg" alt="Decision Observability Architecture"
                         style="width: 100%; height: auto; display: block;"
                         onerror="this.parentElement.innerHTML='<p style=\'color: var(--text-muted); padding: 2em; text-align: center;\'>Architecture diagram loading...</p>'">`,
		string(architectureSVG),
		1)

	// Remove the export tab
	html = strings.Replace(html,
		`<div class="tab" data-tab="export" style="margin-left: auto; background: var(--dark); color: var(--accent);">Export Static HTML</div>`,
		``,
		1)

	// Remove the export tab content
	exportTabStart := strings.Index(html, `<div id="tab-export"`)
	if exportTabStart != -1 {
		exportTabEnd := strings.Index(html[exportTabStart:], `<div id="tab-events"`)
		if exportTabEnd != -1 {
			html = html[:exportTabStart] + html[exportTabStart+exportTabEnd:]
		}
	}


	// Inject the pre-loaded data and modify JavaScript to use it
	// Find the loadData function and replace with pre-loaded data
	jsInjection := fmt.Sprintf(`
    <script>
        // Pre-loaded data for static export
        const STATIC_EXPORT = true;
        const PRELOADED_DATA = %s;
        const PRELOADED_WORKFLOW_EVENTS = PRELOADED_DATA.workflowEvents || {};
    </script>
    <script>`, string(dataJSON))

	html = strings.Replace(html, `<script>`, jsInjection, 1)

	// Replace the loadData function to use pre-loaded data
	html = strings.Replace(html,
		`async function loadData() {
            try {
                const [workflows, misrouted, counterfactual, timeline, beforeAfter] = await Promise.all([
                    fetch('/api/workflows').then(r => r.json()),
                    fetch('/api/audit/misrouted').then(r => r.json()),
                    fetch('/api/audit/counterfactual').then(r => r.json()),
                    fetch('/api/timeline').then(r => r.json()),
                    fetch('/api/audit/before-after').then(r => r.json())
                ]);`,
		`async function loadData() {
            try {
                // Use pre-loaded data in static export
                const workflows = PRELOADED_DATA.workflows || [];
                const misrouted = PRELOADED_DATA.misrouted || {};
                const counterfactual = PRELOADED_DATA.counterfactual || {};
                const timeline = PRELOADED_DATA.timeline || [];
                const beforeAfter = PRELOADED_DATA.beforeAfter || {};`,
		1)

	// Replace workflow options loading to use pre-loaded data
	html = strings.Replace(html,
		`async function loadWorkflowOptions() {
            try {
                const workflows = await fetch('/api/workflows').then(r => r.json());`,
		`async function loadWorkflowOptions() {
            try {
                const workflows = PRELOADED_DATA.workflows || [];`,
		1)

	// Replace events fetching to use pre-loaded data
	html = strings.Replace(html,
		`const events = await fetch(\x60/api/events/${encodeURIComponent(workflowId)}\x60).then(r => r.json());`,
		`const events = PRELOADED_WORKFLOW_EVENTS[workflowId] || [];`,
		1)

	// Disable the auto-refresh interval
	html = strings.Replace(html,
		`setInterval(loadData, 5000);`,
		`// Auto-refresh disabled in static export`,
		1)

	// Update title
	html = strings.Replace(html,
		`<title>Decision Observability - Temporal + XTDB</title>`,
		`<title>Decision Observability Export - Temporal + XTDB</title>`,
		1)

	w.Header().Set("Content-Type", "text/html")
	w.Header().Set("Content-Disposition", "attachment; filename=decision-observability-export.html")
	w.Write([]byte(html))
}

func getWorkflowsData(ctx context.Context) ([]map[string]any, error) {
	rows, err := db.Query(ctx, `
		SELECT workflow_id, namespace, run_id, workflow_type, status,
			start_time, close_time
		FROM workflows
		ORDER BY _valid_from DESC
		LIMIT 50
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var workflows []map[string]any
	for rows.Next() {
		var workflowID, namespace, runID, workflowType, status string
		var startTime string
		var closeTime *string

		if err := rows.Scan(&workflowID, &namespace, &runID, &workflowType, &status, &startTime, &closeTime); err != nil {
			continue
		}

		wf := map[string]any{
			"workflow_id":   workflowID,
			"namespace":     namespace,
			"run_id":        runID,
			"workflow_type": workflowType,
			"status":        status,
			"start_time":    startTime,
		}
		if closeTime != nil {
			wf["close_time"] = *closeTime
		}
		workflows = append(workflows, wf)
	}
	return workflows, nil
}

func getWorkflowEventsData(ctx context.Context, workflowID string) ([]map[string]any, error) {
	sql := fmt.Sprintf(`
		SELECT event_id, event_type, CAST(event_time AS TIMESTAMPTZ),
			workflow_type, task_queue, event_data
		FROM events
		WHERE workflow_id = %s
		  AND run_id = (
		    SELECT run_id FROM events
		    WHERE workflow_id = %s
		    ORDER BY _valid_from DESC
		    LIMIT 1
		  )
		ORDER BY event_id
	`, quote(workflowID), quote(workflowID))

	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []map[string]any
	for rows.Next() {
		var eventID int64
		var eventType, workflowType, taskQueue string
		var eventTime time.Time
		var eventData any

		if err := rows.Scan(&eventID, &eventType, &eventTime, &workflowType, &taskQueue, &eventData); err != nil {
			continue
		}

		evt := map[string]any{
			"event_id":      eventID,
			"event_type":    eventType,
			"event_time":    eventTime.Format(time.RFC3339),
			"workflow_type": workflowType,
			"task_queue":    taskQueue,
		}
		if eventData != nil {
			evt["event_data"] = eventData
		}
		events = append(events, evt)
	}
	return events, nil
}

func getTimelineData(ctx context.Context) ([]map[string]any, error) {
	rows, err := db.Query(ctx, `
		SELECT
			'workflow' as source,
			workflow_id as id,
			CAST(start_time AS TIMESTAMPTZ) as event_time,
			status as description,
			_system_from as recorded_at
		FROM temporal_workflows
		UNION ALL
		SELECT
			'churn_prediction' as source,
			customer_id as id,
			_valid_from as event_time,
			'Churn score: ' || CAST(churn_score AS VARCHAR) as description,
			_system_from as recorded_at
		FROM churn_predictions
		UNION ALL
		SELECT
			'outcome' as source,
			customer_id as id,
			_valid_from as event_time,
			CASE WHEN churned THEN 'CHURNED: ' || COALESCE(churn_reason, 'unknown') ELSE 'Retained' END as description,
			_system_from as recorded_at
		FROM customer_outcomes
		ORDER BY event_time DESC
		LIMIT 100
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var timeline []map[string]any
	for rows.Next() {
		var source, id, description string
		var eventTime, recordedAt time.Time

		if err := rows.Scan(&source, &id, &eventTime, &description, &recordedAt); err != nil {
			continue
		}

		timeline = append(timeline, map[string]any{
			"source":      source,
			"id":          id,
			"event_time":  eventTime.Format(time.RFC3339),
			"description": description,
			"recorded_at": recordedAt.Format(time.RFC3339),
		})
	}
	return timeline, nil
}

func getMisroutedData(ctx context.Context) (map[string]any, error) {
	sql := `
		SELECT
			e.workflow_id,
			CAST(e.event_time AS TIMESTAMPTZ) as decision_time,
			c.customer_id,
			c.churn_score,
			c.risk_level,
			c._valid_from as churn_flagged_at
		FROM temporal_events e
		JOIN churn_predictions FOR VALID_TIME ALL AS c
			ON c.customer_id = 'CUST-' || REPLACE(e.workflow_id, 'cs-workflow-TKT-', '')
		WHERE e.event_type = 'WorkflowExecutionStarted'
		  AND e.workflow_id LIKE 'cs-workflow-TKT-%'
		  AND c.churn_score > 0.7
		  AND c._valid_from <= CAST(e.event_time AS TIMESTAMPTZ)
		  AND (c._valid_to IS NULL OR c._valid_to > CAST(e.event_time AS TIMESTAMPTZ))
		ORDER BY CAST(e.event_time AS TIMESTAMPTZ) DESC
	`

	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]any
	for rows.Next() {
		var workflowID, customerID, riskLevel string
		var decisionTime, churnFlaggedAt time.Time
		var churnScore float64

		if err := rows.Scan(&workflowID, &decisionTime, &customerID, &churnScore, &riskLevel, &churnFlaggedAt); err != nil {
			continue
		}

		results = append(results, map[string]any{
			"workflow_id":      workflowID,
			"decision_time":    decisionTime.Format(time.RFC3339),
			"customer_id":      customerID,
			"churn_score":      churnScore,
			"risk_level":       riskLevel,
			"churn_flagged_at": churnFlaggedAt.Format(time.RFC3339),
			"lag_seconds":      decisionTime.Sub(churnFlaggedAt).Seconds(),
		})
	}

	return map[string]any{
		"description": "Customers routed to standard queue who were already flagged as high churn risk",
		"count":       len(results),
		"results":     results,
	}, nil
}

func getCounterfactualData(ctx context.Context) (map[string]any, error) {
	rows, err := db.Query(ctx, `
		SELECT
			c.customer_id,
			c.churn_score as score_at_model_time,
			o.churned,
			o.churn_reason,
			o.final_ltv
		FROM churn_predictions c
		JOIN customer_outcomes o ON o.customer_id = c.customer_id
		WHERE c.churn_score > 0.7
		ORDER BY c.churn_score DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]any
	var totalLostLTV float64
	var churned, retained int

	for rows.Next() {
		var customerID string
		var churnScore, finalLTV float64
		var didChurn bool
		var churnReason *string

		if err := rows.Scan(&customerID, &churnScore, &didChurn, &churnReason, &finalLTV); err != nil {
			continue
		}

		reason := ""
		if churnReason != nil {
			reason = *churnReason
		}

		if didChurn {
			churned++
			totalLostLTV += finalLTV
		} else {
			retained++
		}

		results = append(results, map[string]any{
			"customer_id":  customerID,
			"churn_score":  churnScore,
			"churned":      didChurn,
			"churn_reason": reason,
			"final_ltv":    finalLTV,
		})
	}

	return map[string]any{
		"description":     "High-risk customers (score > 0.7) and their actual outcomes",
		"high_risk_total": len(results),
		"churned":         churned,
		"retained":        retained,
		"total_lost_ltv":  totalLostLTV,
		"results":         results,
	}, nil
}

func getBeforeAfterData(ctx context.Context) (map[string]any, error) {
	var preCorrectionTime string
	err := db.QueryRow(ctx, `
		SELECT pre_correction_time FROM correction_markers
		WHERE _id = 'corrections:churn-v2.2'
	`).Scan(&preCorrectionTime)
	if err != nil {
		return nil, fmt.Errorf("corrections not loaded")
	}

	auditQuery := `
		SELECT
			e.workflow_id,
			CAST(e.event_time AS TIMESTAMPTZ) as decision_time,
			c.customer_id,
			c.churn_score,
			c.risk_level,
			c._valid_from as churn_flagged_at
		FROM temporal_events e
		JOIN churn_predictions FOR VALID_TIME ALL AS c
			ON c.customer_id = 'CUST-' || REPLACE(e.workflow_id, 'cs-workflow-TKT-', '')
		WHERE e.event_type = 'WorkflowExecutionStarted'
		  AND e.workflow_id LIKE 'cs-workflow-TKT-%%'
		  AND c.churn_score > 0.7
		  AND c._valid_from <= CAST(e.event_time AS TIMESTAMPTZ)
		  AND (c._valid_to IS NULL OR c._valid_to > CAST(e.event_time AS TIMESTAMPTZ))
		ORDER BY CAST(e.event_time AS TIMESTAMPTZ) DESC
	`

	beforeSQL := fmt.Sprintf(
		"SETTING DEFAULT SYSTEM_TIME TO AS OF TIMESTAMP %s %s",
		quote(preCorrectionTime),
		auditQuery,
	)

	beforeResults, err := runAuditQuery(ctx, beforeSQL)
	if err != nil {
		return nil, fmt.Errorf("before query: %w", err)
	}

	afterResults, err := runAuditQuery(ctx, auditQuery)
	if err != nil {
		return nil, fmt.Errorf("after query: %w", err)
	}

	beforeIDs := make(map[string]bool)
	for _, r := range beforeResults {
		if wfID, ok := r["workflow_id"].(string); ok {
			beforeIDs[wfID] = true
		}
	}

	var newDetections []map[string]any
	for _, r := range afterResults {
		if wfID, ok := r["workflow_id"].(string); ok {
			if !beforeIDs[wfID] {
				newDetections = append(newDetections, r)
			}
		}
	}

	return map[string]any{
		"description":         "Before/after comparison using SETTING DEFAULT SYSTEM_TIME AS OF",
		"pre_correction_time": preCorrectionTime,
		"before": map[string]any{
			"count":   len(beforeResults),
			"results": beforeResults,
		},
		"after": map[string]any{
			"count":   len(afterResults),
			"results": afterResults,
		},
		"new_detections": map[string]any{
			"count":   len(newDetections),
			"results": newDetections,
		},
	}, nil
}


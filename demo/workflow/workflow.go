package workflow

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

type TicketInput struct {
	TicketID    string `json:"ticket_id"`
	CustomerID  string `json:"customer_id"`
	Subject     string `json:"subject"`
	Body        string `json:"body"`
	Channel     string `json:"channel"`
	SubmittedAt string `json:"submitted_at"`
}

type TicketResult struct {
	TicketID        string `json:"ticket_id"`
	CustomerID      string `json:"customer_id"`
	RoutingDecision string `json:"routing_decision"`
	ResponseDraft   string `json:"response_draft"`
	ProcessedAt     string `json:"processed_at"`
}

type SentimentResult struct {
	Sentiment  string  `json:"sentiment"`
	Confidence float64 `json:"confidence"`
	Urgency    string  `json:"urgency"`
}

type CustomerProfile struct {
	CustomerID     string  `json:"customer_id"`
	Tier           string  `json:"tier"`
	LTV            float64 `json:"ltv"`
	AccountAgeDays int     `json:"account_age_days"`
	OpenTickets    int     `json:"open_tickets"`
}

type ChurnSignals struct {
	CustomerID     string  `json:"customer_id"`
	ChurnScore     float64 `json:"churn_score"`
	RiskLevel      string  `json:"risk_level"`
	LastActivityAt string  `json:"last_activity_at"`
	SignalSource   string  `json:"signal_source"`
}

type RoutingDecision struct {
	Queue           string   `json:"queue"`
	Priority        int      `json:"priority"`
	ReasonCodes     []string `json:"reason_codes"`
	EscalationLevel int      `json:"escalation_level"`
}

type ResponseDraft struct {
	Body       string `json:"body"`
	Tone       string `json:"tone"`
	SuggestHuman bool   `json:"suggest_human"`
}

func CustomerServiceWorkflow(ctx workflow.Context, input TicketInput) (*TicketResult, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var sentiment SentimentResult
	if err := workflow.ExecuteActivity(ctx, "AnalyzeSentiment", input).Get(ctx, &sentiment); err != nil {
		return nil, err
	}

	var profile CustomerProfile
	if err := workflow.ExecuteActivity(ctx, "LookupCustomerProfile", input.CustomerID).Get(ctx, &profile); err != nil {
		return nil, err
	}

	var churnSignals ChurnSignals
	if err := workflow.ExecuteActivity(ctx, "CheckChurnSignals", input.CustomerID).Get(ctx, &churnSignals); err != nil {
		return nil, err
	}

	routingInput := struct {
		Ticket   TicketInput
		Sentiment SentimentResult
		Profile   CustomerProfile
		Churn     ChurnSignals
	}{input, sentiment, profile, churnSignals}

	var routing RoutingDecision
	if err := workflow.ExecuteActivity(ctx, "DecideRouting", routingInput).Get(ctx, &routing); err != nil {
		return nil, err
	}

	responseInput := struct {
		Ticket   TicketInput
		Sentiment SentimentResult
		Profile   CustomerProfile
		Routing   RoutingDecision
	}{input, sentiment, profile, routing}

	var response ResponseDraft
	if err := workflow.ExecuteActivity(ctx, "GenerateResponse", responseInput).Get(ctx, &response); err != nil {
		return nil, err
	}

	return &TicketResult{
		TicketID:        input.TicketID,
		CustomerID:      input.CustomerID,
		RoutingDecision: routing.Queue,
		ResponseDraft:   response.Body,
		ProcessedAt:     workflow.Now(ctx).Format(time.RFC3339),
	}, nil
}

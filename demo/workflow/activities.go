package workflow

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/refset/temporal-decision-observability/demo/xtdb"
)

type Activities struct {
	churnScores map[string]float64
	xtdb        *xtdb.Client
}

func NewActivities() *Activities {
	return &Activities{
		churnScores: make(map[string]float64),
	}
}

func NewActivitiesWithXTDB(xtdbClient *xtdb.Client) *Activities {
	return &Activities{
		churnScores: make(map[string]float64),
		xtdb:        xtdbClient,
	}
}

func (a *Activities) SetChurnScore(customerID string, score float64) {
	a.churnScores[customerID] = score
}

func (a *Activities) AnalyzeSentiment(ctx context.Context, input TicketInput) (*SentimentResult, error) {
	time.Sleep(100 * time.Millisecond)

	body := strings.ToLower(input.Body)
	sentiment := "neutral"
	confidence := 0.7 + rand.Float64()*0.2
	urgency := "normal"

	negativeWords := []string{"angry", "frustrated", "disappointed", "terrible", "awful", "worst", "cancel", "refund"}
	positiveWords := []string{"thank", "great", "love", "excellent", "happy", "appreciate"}
	urgentWords := []string{"urgent", "asap", "immediately", "emergency", "critical"}

	negCount := 0
	posCount := 0
	for _, word := range negativeWords {
		if strings.Contains(body, word) {
			negCount++
		}
	}
	for _, word := range positiveWords {
		if strings.Contains(body, word) {
			posCount++
		}
	}
	for _, word := range urgentWords {
		if strings.Contains(body, word) {
			urgency = "high"
		}
	}

	if negCount > posCount {
		sentiment = "negative"
	} else if posCount > negCount {
		sentiment = "positive"
	}

	return &SentimentResult{
		Sentiment:  sentiment,
		Confidence: confidence,
		Urgency:    urgency,
	}, nil
}

func (a *Activities) LookupCustomerProfile(ctx context.Context, customerID string) (*CustomerProfile, error) {
	time.Sleep(50 * time.Millisecond)

	tiers := []string{"bronze", "silver", "gold", "platinum"}
	tier := tiers[rand.Intn(len(tiers))]

	ltvBase := map[string]float64{
		"bronze":   100,
		"silver":   500,
		"gold":     2000,
		"platinum": 10000,
	}

	return &CustomerProfile{
		CustomerID:     customerID,
		Tier:           tier,
		LTV:            ltvBase[tier] + rand.Float64()*1000,
		AccountAgeDays: rand.Intn(1000) + 30,
		OpenTickets:    rand.Intn(3),
	}, nil
}

func (a *Activities) CheckChurnSignals(ctx context.Context, customerID string) (*ChurnSignals, error) {
	time.Sleep(75 * time.Millisecond)

	churnScore := 0.2 + rand.Float64()*0.3
	if score, ok := a.churnScores[customerID]; ok {
		churnScore = score
	}

	riskLevel := "low"
	if churnScore > 0.7 {
		riskLevel = "high"
	} else if churnScore > 0.4 {
		riskLevel = "medium"
	}

	result := &ChurnSignals{
		CustomerID:     customerID,
		ChurnScore:     churnScore,
		RiskLevel:      riskLevel,
		LastActivityAt: time.Now().Add(-time.Duration(rand.Intn(30)) * 24 * time.Hour).Format(time.RFC3339),
		SignalSource:   "internal_model_v2",
	}

	if a.xtdb != nil {
		a.xtdb.Save(ctx, xtdb.DecisionContext{
			Table:     "activity_churn_signals",
			ID:        fmt.Sprintf("churn:%s:%d", customerID, time.Now().UnixNano()),
			ValidFrom: time.Now(),
			Data: map[string]any{
				"customer_id":      customerID,
				"churn_score":      churnScore,
				"risk_level":       riskLevel,
				"signal_source":    result.SignalSource,
				"last_activity_at": result.LastActivityAt,
			},
		})
	}

	return result, nil
}

func (a *Activities) DecideRouting(ctx context.Context, input struct {
	Ticket    TicketInput
	Sentiment SentimentResult
	Profile   CustomerProfile
	Churn     ChurnSignals
}) (*RoutingDecision, error) {
	time.Sleep(50 * time.Millisecond)

	queue := "standard"
	priority := 3
	var reasons []string
	escalation := 0

	if input.Profile.Tier == "platinum" {
		queue = "premium"
		priority = 1
		reasons = append(reasons, "platinum_customer")
	} else if input.Profile.Tier == "gold" {
		queue = "priority"
		priority = 2
		reasons = append(reasons, "gold_customer")
	}

	if input.Churn.ChurnScore > 0.7 {
		if queue == "standard" {
			queue = "priority"
		}
		priority = min(priority, 2)
		reasons = append(reasons, "high_churn_risk")
		escalation = 1
	}

	if input.Sentiment.Sentiment == "negative" {
		priority = max(1, priority-1)
		reasons = append(reasons, "negative_sentiment")
		if input.Sentiment.Urgency == "high" {
			escalation = max(escalation, 1)
			reasons = append(reasons, "urgent_request")
		}
	}

	if input.Profile.LTV > 5000 && queue == "standard" {
		queue = "priority"
		reasons = append(reasons, "high_ltv")
	}

	if len(reasons) == 0 {
		reasons = []string{"default_routing"}
	}

	result := &RoutingDecision{
		Queue:           queue,
		Priority:        priority,
		ReasonCodes:     reasons,
		EscalationLevel: escalation,
	}

	if a.xtdb != nil {
		reasonsStr := strings.Join(reasons, ",")
		a.xtdb.Save(ctx, xtdb.DecisionContext{
			Table:     "activity_routing_decisions",
			ID:        fmt.Sprintf("routing:%s:%d", input.Ticket.TicketID, time.Now().UnixNano()),
			ValidFrom: time.Now(),
			Data: map[string]any{
				"ticket_id":        input.Ticket.TicketID,
				"customer_id":      input.Ticket.CustomerID,
				"queue":            queue,
				"priority":         priority,
				"reason_codes":     reasonsStr,
				"escalation_level": escalation,
				"input_churn_score":    input.Churn.ChurnScore,
				"input_churn_risk":     input.Churn.RiskLevel,
				"input_sentiment":      input.Sentiment.Sentiment,
				"input_urgency":        input.Sentiment.Urgency,
				"input_customer_tier":  input.Profile.Tier,
				"input_customer_ltv":   input.Profile.LTV,
			},
		})
	}

	return result, nil
}

func (a *Activities) GenerateResponse(ctx context.Context, input struct {
	Ticket    TicketInput
	Sentiment SentimentResult
	Profile   CustomerProfile
	Routing   RoutingDecision
}) (*ResponseDraft, error) {
	time.Sleep(100 * time.Millisecond)

	tone := "professional"
	suggestHuman := false

	if input.Sentiment.Sentiment == "negative" {
		tone = "empathetic"
		suggestHuman = true
	} else if input.Profile.Tier == "platinum" {
		tone = "personalized"
	}

	greeting := "Dear Customer"
	if input.Profile.Tier == "platinum" {
		greeting = "Dear Valued Platinum Member"
	} else if input.Profile.Tier == "gold" {
		greeting = "Dear Gold Member"
	}

	body := greeting + ",\n\n"
	body += "Thank you for contacting us regarding: " + input.Ticket.Subject + "\n\n"

	if input.Sentiment.Sentiment == "negative" {
		body += "We sincerely apologize for any inconvenience you've experienced. "
		body += "Your satisfaction is our top priority.\n\n"
	}

	body += "Your request has been routed to our " + input.Routing.Queue + " support team "
	body += "and will be addressed promptly.\n\n"
	body += "Best regards,\nCustomer Support Team"

	return &ResponseDraft{
		Body:         body,
		Tone:         tone,
		SuggestHuman: suggestHuman,
	}, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

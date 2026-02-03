package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/refset/temporal-decision-observability/demo/workflow"
	"go.temporal.io/sdk/client"
)

const TaskQueue = "customer-service"

func main() {
	hostPort := os.Getenv("TEMPORAL_HOST_PORT")
	if hostPort == "" {
		hostPort = "localhost:7233"
	}

	c, err := client.Dial(client.Options{
		HostPort: hostPort,
	})
	if err != nil {
		log.Fatalln("Unable to create Temporal client:", err)
	}
	defer c.Close()

	tickets := []workflow.TicketInput{
		{
			TicketID:    "TKT-001",
			CustomerID:  "CUST-001",
			Subject:     "Billing Issue - Overcharged",
			Body:        "I am very frustrated! I was charged twice for my subscription. Please fix this immediately.",
			Channel:     "email",
			SubmittedAt: time.Now().Format(time.RFC3339),
		},
		{
			TicketID:    "TKT-002",
			CustomerID:  "CUST-002",
			Subject:     "Feature Question",
			Body:        "Hello, I was wondering if your product supports integration with Slack? Thank you!",
			Channel:     "chat",
			SubmittedAt: time.Now().Format(time.RFC3339),
		},
		{
			TicketID:    "TKT-003",
			CustomerID:  "CUST-003",
			Subject:     "Account Cancellation Request",
			Body:        "I want to cancel my account. Your service is terrible and I've had the worst experience.",
			Channel:     "email",
			SubmittedAt: time.Now().Format(time.RFC3339),
		},
		{
			TicketID:    "TKT-004",
			CustomerID:  "CUST-004",
			Subject:     "Password Reset",
			Body:        "Hi, I need help resetting my password. Thanks!",
			Channel:     "chat",
			SubmittedAt: time.Now().Format(time.RFC3339),
		},
		{
			TicketID:    "TKT-005",
			CustomerID:  "CUST-005",
			Subject:     "Urgent: System Down",
			Body:        "Our entire team cannot access the system. This is URGENT! We are losing money every minute.",
			Channel:     "phone",
			SubmittedAt: time.Now().Format(time.RFC3339),
		},
		{
			TicketID:    "TKT-006",
			CustomerID:  "CUST-006",
			Subject:     "API Documentation Question",
			Body:        "Where can I find documentation for the REST API? I appreciate your help.",
			Channel:     "email",
			SubmittedAt: time.Now().Format(time.RFC3339),
		},
		{
			TicketID:    "TKT-007",
			CustomerID:  "CUST-007",
			Subject:     "Disappointed with Recent Changes",
			Body:        "The recent update broke everything I relied on. I'm very disappointed and considering alternatives.",
			Channel:     "email",
			SubmittedAt: time.Now().Format(time.RFC3339),
		},
		{
			TicketID:    "TKT-008",
			CustomerID:  "CUST-008",
			Subject:     "Great Product!",
			Body:        "Just wanted to say thank you for an excellent product. Love using it every day!",
			Channel:     "email",
			SubmittedAt: time.Now().Format(time.RFC3339),
		},
		{
			TicketID:    "TKT-009",
			CustomerID:  "CUST-009",
			Subject:     "Subscription Downgrade",
			Body:        "I need to downgrade my subscription. The current plan is too expensive and I'm not getting value.",
			Channel:     "email",
			SubmittedAt: time.Now().Format(time.RFC3339),
		},
		{
			TicketID:    "TKT-010",
			CustomerID:  "CUST-010",
			Subject:     "Training Resources",
			Body:        "Do you offer any training webinars or tutorials? Our team would love to learn more.",
			Channel:     "chat",
			SubmittedAt: time.Now().Format(time.RFC3339),
		},
	}

	log.Println("Starting customer service workflows...")

	for _, ticket := range tickets {
		workflowID := fmt.Sprintf("cs-workflow-%s", ticket.TicketID)

		opts := client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: TaskQueue,
		}

		we, err := c.ExecuteWorkflow(context.Background(), opts, workflow.CustomerServiceWorkflow, ticket)
		if err != nil {
			log.Printf("Failed to start workflow for %s: %v", ticket.TicketID, err)
			continue
		}

		log.Printf("Started workflow %s for ticket %s (customer: %s)",
			we.GetID(), ticket.TicketID, ticket.CustomerID)

		time.Sleep(500 * time.Millisecond)
	}

	log.Println("All workflows started. Check Temporal UI at http://localhost:8080")
}

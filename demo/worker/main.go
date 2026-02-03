package main

import (
	"context"
	"log"
	"os"

	"github.com/refset/temporal-decision-observability/demo/workflow"
	"github.com/refset/temporal-decision-observability/demo/xtdb"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
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

	w := worker.New(c, TaskQueue, worker.Options{})

	var activities *workflow.Activities

	xtdbConnString := os.Getenv("XTDB_CONN_STRING")
	if xtdbConnString != "" {
		xtdbClient, err := xtdb.NewClientFromConnString(context.Background(), xtdbConnString)
		if err != nil {
			log.Println("Warning: Could not connect to XTDB, activities will not persist decision context:", err)
			activities = workflow.NewActivities()
		} else {
			defer xtdbClient.Close()
			activities = workflow.NewActivitiesWithXTDB(xtdbClient)
			log.Println("XTDB integration enabled - activities will persist decision context")
		}
	} else {
		activities = workflow.NewActivities()
		log.Println("XTDB_CONN_STRING not set - activities will not persist decision context")
	}

	w.RegisterWorkflow(workflow.CustomerServiceWorkflow)
	w.RegisterActivityWithOptions(activities.AnalyzeSentiment, activity.RegisterOptions{Name: "AnalyzeSentiment"})
	w.RegisterActivityWithOptions(activities.LookupCustomerProfile, activity.RegisterOptions{Name: "LookupCustomerProfile"})
	w.RegisterActivityWithOptions(activities.CheckChurnSignals, activity.RegisterOptions{Name: "CheckChurnSignals"})
	w.RegisterActivityWithOptions(activities.DecideRouting, activity.RegisterOptions{Name: "DecideRouting"})
	w.RegisterActivityWithOptions(activities.GenerateResponse, activity.RegisterOptions{Name: "GenerateResponse"})

	log.Println("Starting worker on task queue:", TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("Unable to start worker:", err)
	}
}

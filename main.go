package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/refset/temporal-decision-observability/internal/config"
	"github.com/refset/temporal-decision-observability/internal/kafka"
	"github.com/refset/temporal-decision-observability/internal/pipeline"
	"github.com/refset/temporal-decision-observability/internal/temporal"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}

	if cfg.Logging.Level == "debug" {
		logger, _ = zap.NewDevelopment()
	}

	logger.Info("starting temporal-cdc-kafka",
		zap.String("temporal_host", cfg.Temporal.HostPort),
		zap.String("temporal_namespace", cfg.Temporal.Namespace),
		zap.Strings("kafka_brokers", cfg.Kafka.Brokers),
		zap.Duration("poll_interval", cfg.Pipeline.PollInterval),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	temporalClient, err := temporal.NewClient(cfg.Temporal.HostPort, cfg.Temporal.Namespace)
	if err != nil {
		logger.Fatal("failed to connect to Temporal", zap.Error(err))
	}
	defer temporalClient.Close()

	logger.Info("connected to Temporal")

	producer := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:      cfg.Kafka.Brokers,
		BatchSize:    cfg.Pipeline.BatchSize,
		BatchTimeout: 100 * time.Millisecond,
		Async:        false,
	}, logger)
	defer producer.Close()

	if err := kafka.EnsureTopics(ctx, cfg.Kafka.Brokers, logger); err != nil {
		logger.Fatal("failed to ensure Kafka topics", zap.Error(err))
	}
	logger.Info("connected to Kafka", zap.Strings("brokers", cfg.Kafka.Brokers))

	checkpointStore := kafka.NewCheckpointStore(cfg.Kafka.Brokers, logger)

	poller := temporal.NewPoller(temporalClient)

	pipe := pipeline.New(poller, producer, checkpointStore, pipeline.Config{
		PollInterval: cfg.Pipeline.PollInterval,
		BatchSize:    cfg.Pipeline.BatchSize,
		Namespace:    cfg.Temporal.Namespace,
	}, logger)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("received shutdown signal")
		cancel()
	}()

	if err := pipe.Run(ctx); err != nil && err != context.Canceled {
		logger.Fatal("pipeline error", zap.Error(err))
	}

	logger.Info("shutdown complete")
}

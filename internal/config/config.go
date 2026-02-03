package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Temporal TemporalConfig `yaml:"temporal"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	Pipeline PipelineConfig `yaml:"pipeline"`
	Logging  LoggingConfig  `yaml:"logging"`
}

type TemporalConfig struct {
	HostPort  string `yaml:"host_port"`
	Namespace string `yaml:"namespace"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
}

type PipelineConfig struct {
	PollInterval time.Duration `yaml:"poll_interval"`
	BatchSize    int           `yaml:"batch_size"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func Load(path string) (*Config, error) {
	cfg := &Config{
		Temporal: TemporalConfig{
			HostPort:  "localhost:7233",
			Namespace: "default",
		},
		Kafka: KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
		Pipeline: PipelineConfig{
			PollInterval: 10 * time.Second,
			BatchSize:    100,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
		},
	}

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, err
			}
		} else {
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return nil, err
			}
		}
	}

	cfg.applyEnvOverrides()
	return cfg, nil
}

func (c *Config) applyEnvOverrides() {
	if v := os.Getenv("TEMPORAL_HOST_PORT"); v != "" {
		c.Temporal.HostPort = v
	}
	if v := os.Getenv("TEMPORAL_NAMESPACE"); v != "" {
		c.Temporal.Namespace = v
	}
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		c.Kafka.Brokers = []string{v}
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		c.Logging.Level = v
	}
	if v := os.Getenv("POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			c.Pipeline.PollInterval = d
		}
	}
}

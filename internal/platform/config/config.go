package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Region      string
	HTTPAddr    string
	DatabaseURL string

	KafkaBrokers []string // this region's cluster
	KafkaTopic   string   // topic this region publishes to

	RemoteBrokers   []string // the other region's cluster (replication source)
	RemoteTopic     string
	ReplicatorGroup string

	HealthRequireKafka bool // if true, local Kafka down => /health 503 => router leaves the region
	RelayInterval      time.Duration
	PublishTimeout     time.Duration
}

func Load() (Config, error) {
	c := Config{
		Region:             os.Getenv("REGION"),
		HTTPAddr:           env("HTTP_ADDR", ":8080"),
		DatabaseURL:        os.Getenv("DATABASE_URL"),
		KafkaBrokers:       split(os.Getenv("KAFKA_BROKERS")),
		RemoteBrokers:      split(os.Getenv("REMOTE_KAFKA_BROKERS")),
		RemoteTopic:        os.Getenv("REMOTE_KAFKA_TOPIC"),
		HealthRequireKafka: envBool("HEALTH_REQUIRE_KAFKA", false),
		RelayInterval:      envDuration("RELAY_INTERVAL", 2*time.Second),
		PublishTimeout:     envDuration("PUBLISH_TIMEOUT", 2*time.Second),
	}
	c.KafkaTopic = env("KAFKA_TOPIC", c.Region+".customers")
	c.ReplicatorGroup = env("REPLICATOR_GROUP", "replicator-"+c.Region)

	switch {
	case c.Region == "":
		return c, fmt.Errorf("REGION is required")
	case c.DatabaseURL == "":
		return c, fmt.Errorf("DATABASE_URL is required")
	case len(c.KafkaBrokers) == 0:
		return c, fmt.Errorf("KAFKA_BROKERS is required")
	}
	return c, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envBool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

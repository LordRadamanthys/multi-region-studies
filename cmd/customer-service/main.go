package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"multi-region/internal/adapters/in/httpapi"
	"multi-region/internal/adapters/in/replicator"
	"multi-region/internal/adapters/out/kafkapub"
	"multi-region/internal/adapters/out/metrics"
	"multi-region/internal/adapters/out/postgres"
	"multi-region/internal/application"
	"multi-region/internal/health"
	"multi-region/internal/platform/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("region", cfg.Region)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ---- outbound adapters ----
	repo, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("cannot configure postgres", "err", err)
		os.Exit(1)
	}
	defer repo.Close()
	migrateWithRetry(ctx, repo, log)

	publisher := kafkapub.New(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer publisher.Close()
	m := metrics.New()

	// ---- application core ----
	svc := application.NewService(repo, publisher, m, cfg.Region, cfg.PublishTimeout, log)

	checks := []health.Check{
		{Name: "db", Required: true, Fn: repo.Ping},
		{Name: "kafka", Required: cfg.HealthRequireKafka, Fn: kafkapub.Ping(cfg.KafkaBrokers)},
	}
	if len(cfg.RemoteBrokers) > 0 {
		// informational only: tells us whether the WAN / the other region's Kafka is reachable
		checks = append(checks, health.Check{Name: "remote_kafka", Fn: kafkapub.Ping(cfg.RemoteBrokers)})
	}

	// ---- background workers ----
	go dependencyLoop(ctx, checks, m)
	go relayLoop(ctx, svc, cfg.RelayInterval, log)
	if len(cfg.RemoteBrokers) > 0 && cfg.RemoteTopic != "" {
		go replicator.New(cfg.RemoteBrokers, cfg.RemoteTopic, cfg.ReplicatorGroup, svc, log).Run(ctx)
	}

	// ---- inbound adapter ----
	api := httpapi.New(svc, cfg.Region, checks, m, m.Handler(), log)
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("customer-service started", "addr", cfg.HTTPAddr, "topic", cfg.KafkaTopic,
		"replicating_from", cfg.RemoteTopic, "health_requires_kafka", cfg.HealthRequireKafka)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http server stopped", "err", err)
		os.Exit(1)
	}
}

func migrateWithRetry(ctx context.Context, repo *postgres.Repository, log *slog.Logger) {
	for ctx.Err() == nil {
		mctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := repo.Migrate(mctx)
		cancel()
		if err == nil {
			return
		}
		log.Warn("waiting for database", "err", err)
		time.Sleep(2 * time.Second)
	}
}

func dependencyLoop(ctx context.Context, checks []health.Check, m *metrics.Metrics) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		for _, r := range health.Run(ctx, checks, 1500*time.Millisecond) {
			m.SetDependency(r.Name, r.Err == nil)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func relayLoop(ctx context.Context, svc *application.Service, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	failing := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		n, err := svc.RelayOutbox(rctx, 100)
		if err == nil {
			err = svc.RefreshStatusCounts(rctx)
		}
		cancel()
		switch {
		case err != nil && !failing:
			log.Warn("outbox relay cannot reach the database", "err", err)
			failing = true
		case err == nil && failing:
			log.Info("outbox relay recovered")
			failing = false
		}
		if n > 0 {
			log.Info("outbox relay published pending events", "count", n)
		}
	}
}

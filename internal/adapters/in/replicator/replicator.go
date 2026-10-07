// Package replicator is the inbound adapter that consumes the OTHER region's topic
// and feeds it to the core (stand-in for MirrorMaker 2 / MSK Replicator).
package replicator

import (
	"context"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"multi-region/internal/adapters/wire"
	"multi-region/internal/ports"
)

type Replicator struct {
	reader *kafkago.Reader
	uc     ports.ReplicationUseCase
	log    *slog.Logger
}

func New(brokers []string, topic, group string, uc ports.ReplicationUseCase, log *slog.Logger) *Replicator {
	return &Replicator{
		uc:  uc,
		log: log,
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     group,
			MinBytes:    1,
			MaxBytes:    5 << 20,
			MaxWait:     500 * time.Millisecond,
			StartOffset: kafkago.FirstOffset,
			Dialer:      &kafkago.Dialer{Timeout: 3 * time.Second, DualStack: true},
		}),
	}
}

// Run blocks until ctx is cancelled. Offsets are committed only after the event was applied,
// so after an outage the replicator resumes exactly where it stopped (at-least-once + idempotent apply).
func (r *Replicator) Run(ctx context.Context) {
	defer r.reader.Close()
	lastErr := ""
	for ctx.Err() == nil {
		msg, err := r.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if err.Error() != lastErr { // log state changes, not every retry
				r.log.Warn("remote region unreachable, retrying", "err", err)
				lastErr = err.Error()
			}
			sleep(ctx, time.Second)
			continue
		}
		if lastErr != "" {
			r.log.Info("remote region reachable again, replication resumed")
			lastErr = ""
		}

		c, err := wire.Decode(msg.Value)
		if err != nil {
			r.log.Error("dropping undecodable event", "offset", msg.Offset, "err", err)
		} else {
			for ctx.Err() == nil {
				if err := r.uc.ApplyReplicated(ctx, c); err == nil {
					break
				} else {
					r.log.Warn("apply failed (local DB down?), retrying same event", "customer_id", c.ID, "err", err)
					sleep(ctx, time.Second)
				}
			}
		}
		for ctx.Err() == nil {
			if err := r.reader.CommitMessages(ctx, msg); err == nil {
				break
			} else {
				r.log.Warn("offset commit failed, retrying", "err", err)
				sleep(ctx, time.Second)
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

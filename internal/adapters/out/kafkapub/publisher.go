// Package kafkapub is the outbound adapter that publishes customer events to this region's topic.
package kafkapub

import (
	"context"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"multi-region/internal/adapters/wire"
	"multi-region/internal/domain"
	"multi-region/internal/ports"
)

var _ ports.EventPublisher = (*Publisher)(nil)

type Publisher struct{ w *kafkago.Writer }

func New(brokers []string, topic string) *Publisher {
	return &Publisher{w: &kafkago.Writer{
		Addr:            kafkago.TCP(brokers...),
		Topic:           topic,
		Balancer:        &kafkago.Hash{}, // same customer => same partition => ordered
		RequiredAcks:    kafkago.RequireAll,
		MaxAttempts:     2,
		WriteBackoffMin: 50 * time.Millisecond,
		WriteBackoffMax: 200 * time.Millisecond,
		BatchTimeout:    5 * time.Millisecond,
		ReadTimeout:     2 * time.Second,
		WriteTimeout:    2 * time.Second,
	}}
}

func (p *Publisher) Publish(ctx context.Context, c domain.Customer) error {
	value, err := wire.Encode(c)
	if err != nil {
		return err
	}
	return p.w.WriteMessages(ctx, kafkago.Message{
		Key:     []byte(c.ID),
		Value:   value,
		Headers: []kafkago.Header{{Key: "origin_region", Value: []byte(c.OriginRegion)}},
	})
}

func (p *Publisher) Close() error { return p.w.Close() }

// Ping returns a health check that asks the first reachable broker for the cluster controller.
func Ping(brokers []string) func(context.Context) error {
	return func(ctx context.Context) error {
		d := &kafkago.Dialer{Timeout: 1500 * time.Millisecond}
		var lastErr error
		for _, b := range brokers {
			conn, err := d.DialContext(ctx, "tcp", b)
			if err != nil {
				lastErr = err
				continue
			}
			_ = conn.SetDeadline(time.Now().Add(1500 * time.Millisecond))
			_, err = conn.Controller()
			_ = conn.Close()
			if err == nil {
				return nil
			}
			lastErr = err
		}
		return lastErr
	}
}

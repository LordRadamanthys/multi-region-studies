// Package ports declares the contracts between the application core and the outside world.
package ports

import (
	"context"
	"time"

	"multi-region/internal/domain"
)

// ---- driving (inbound) ports ----

type CreateCustomerCommand struct {
	ID       string // optional; acts as idempotency key
	Name     string
	Address  string
	Email    string
	Nickname string
}

type UpdateCustomerCommand struct {
	ID       string
	Name     string
	Address  string
	Email    string
	Nickname string
}

type CustomerUseCase interface {
	// Create returns created=false when the ID already existed (idempotent replay).
	Create(ctx context.Context, cmd CreateCustomerCommand) (c domain.Customer, created bool, err error)
	Get(ctx context.Context, id string) (domain.Customer, error)
	List(ctx context.Context, limit, offset int) ([]domain.Customer, error)
	Update(ctx context.Context, cmd UpdateCustomerCommand) (domain.Customer, error)
	Delete(ctx context.Context, id string) error
}

// ReplicationUseCase applies changes that happened in another region.
type ReplicationUseCase interface {
	ApplyReplicated(ctx context.Context, c domain.Customer) error
}

// ---- driven (outbound) ports ----

type CustomerRepository interface {
	Create(ctx context.Context, c domain.Customer) (created bool, err error)
	FindByID(ctx context.Context, id string) (domain.Customer, error)
	List(ctx context.Context, limit, offset int) ([]domain.Customer, error)
	Update(ctx context.Context, c domain.Customer) error
	// SetStatus only touches the row if it is still at the given version.
	SetStatus(ctx context.Context, id string, version int64, st domain.Status) error
	ListUnpublished(ctx context.Context, region string, limit int) ([]domain.Customer, error)
	CountByStatus(ctx context.Context) (map[domain.Status]int, error)
	// ApplyReplicated upserts using last-write-wins; applied=false means the change was stale/duplicate.
	ApplyReplicated(ctx context.Context, c domain.Customer) (applied bool, err error)
}

type EventPublisher interface {
	Publish(ctx context.Context, c domain.Customer) error
}

type Telemetry interface {
	EventPublished(ok bool)
	Replicated(originRegion, result string)
	ReplicationLag(d time.Duration)
	StatusCounts(counts map[domain.Status]int)
}

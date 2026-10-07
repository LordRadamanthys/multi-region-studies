// Package application implements the use cases on top of the ports.
package application

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"multi-region/internal/domain"
	"multi-region/internal/ports"
)

type Service struct {
	repo           ports.CustomerRepository
	pub            ports.EventPublisher
	tel            ports.Telemetry
	region         string
	publishTimeout time.Duration
	log            *slog.Logger
	now            func() time.Time
}

var (
	_ ports.CustomerUseCase    = (*Service)(nil)
	_ ports.ReplicationUseCase = (*Service)(nil)
)

func NewService(repo ports.CustomerRepository, pub ports.EventPublisher, tel ports.Telemetry,
	region string, publishTimeout time.Duration, log *slog.Logger) *Service {
	return &Service{repo: repo, pub: pub, tel: tel, region: region, publishTimeout: publishTimeout, log: log, now: time.Now}
}

func (s *Service) Create(ctx context.Context, cmd ports.CreateCustomerCommand) (domain.Customer, bool, error) {
	id := cmd.ID
	if id == "" {
		id = domain.NewID()
	} else {
		if !domain.IsValidID(id) {
			return domain.Customer{}, false, errors.New("invalid customer: id must be a lowercase UUID")
		}
		// Idempotent replay: the same key returns the stored customer.
		if existing, err := s.repo.FindByID(ctx, id); err == nil {
			return existing, false, nil
		} else if !errors.Is(err, domain.ErrNotFound) {
			return domain.Customer{}, false, err
		}
	}

	c, err := domain.NewCustomer(id, cmd.Name, cmd.Address, cmd.Email, cmd.Nickname, s.region, s.now())
	if err != nil {
		return domain.Customer{}, false, err
	}
	created, err := s.repo.Create(ctx, c)
	if err != nil {
		return domain.Customer{}, false, err
	}
	if !created { // lost a race with the same key
		existing, err := s.repo.FindByID(ctx, id)
		return existing, false, err
	}
	s.publish(ctx, &c)
	return c, true, nil
}

func (s *Service) Get(ctx context.Context, id string) (domain.Customer, error) {
	c, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return domain.Customer{}, err
	}
	if c.Deleted {
		return domain.Customer{}, domain.ErrNotFound
	}
	return c, nil
}

func (s *Service) List(ctx context.Context, limit, offset int) ([]domain.Customer, error) {
	return s.repo.List(ctx, limit, offset)
}

func (s *Service) Update(ctx context.Context, cmd ports.UpdateCustomerCommand) (domain.Customer, error) {
	c, err := s.Get(ctx, cmd.ID)
	if err != nil {
		return domain.Customer{}, err
	}
	if err := c.Update(cmd.Name, cmd.Address, cmd.Email, cmd.Nickname, s.region, s.now()); err != nil {
		return domain.Customer{}, err
	}
	if err := s.repo.Update(ctx, c); err != nil {
		return domain.Customer{}, err
	}
	s.publish(ctx, &c)
	return c, nil
}

func (s *Service) Delete(ctx context.Context, id string) error {
	c, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	c.MarkDeleted(s.region, s.now())
	if err := s.repo.Update(ctx, c); err != nil {
		return err
	}
	s.publish(ctx, &c)
	return nil
}

// ApplyReplicated is called by the replicator for events produced by the other region.
func (s *Service) ApplyReplicated(ctx context.Context, c domain.Customer) error {
	if c.OriginRegion == s.region {
		s.tel.Replicated(c.OriginRegion, "ignored_own")
		return nil
	}
	applied, err := s.repo.ApplyReplicated(ctx, c)
	if err != nil {
		s.tel.Replicated(c.OriginRegion, "error")
		return err
	}
	result := "applied"
	if !applied {
		result = "skipped_stale"
	}
	s.tel.Replicated(c.OriginRegion, result)
	s.tel.ReplicationLag(s.now().Sub(c.UpdatedAt))
	return nil
}

// RelayOutbox retries every event that is not PUBLISHED yet (outbox pattern: the row status is the outbox).
func (s *Service) RelayOutbox(ctx context.Context, batch int) (int, error) {
	pending, err := s.repo.ListUnpublished(ctx, s.region, batch)
	if err != nil {
		return 0, err
	}
	published := 0
	for i := range pending {
		if s.publish(ctx, &pending[i]) == domain.StatusPublished {
			published++
		}
	}
	return published, nil
}

func (s *Service) RefreshStatusCounts(ctx context.Context) error {
	counts, err := s.repo.CountByStatus(ctx)
	if err != nil {
		return err
	}
	s.tel.StatusCounts(counts)
	return nil
}

// publish sends the event to THIS region's topic and records the outcome in the row status.
// The region that committed the write is always the one that publishes it.
func (s *Service) publish(ctx context.Context, c *domain.Customer) domain.Status {
	bg := context.WithoutCancel(ctx) // the client may give up; the status update must still happen
	pctx, cancel := context.WithTimeout(bg, s.publishTimeout)
	defer cancel()

	status := domain.StatusPublished
	if err := s.pub.Publish(pctx, *c); err != nil {
		status = domain.StatusPublishFailed
		s.log.Warn("publish failed, will retry via relay", "customer_id", c.ID, "err", err)
	}
	s.tel.EventPublished(status == domain.StatusPublished)

	sctx, cancel2 := context.WithTimeout(bg, s.publishTimeout)
	defer cancel2()
	if err := s.repo.SetStatus(sctx, c.ID, c.Version, status); err != nil {
		s.log.Warn("could not update status", "customer_id", c.ID, "status", status, "err", err)
	}
	c.Status = status
	return status
}

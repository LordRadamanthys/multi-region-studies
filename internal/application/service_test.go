package application

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"multi-region/internal/domain"
	"multi-region/internal/ports"
)

// ---- fakes ----

type memRepo struct {
	mu   sync.Mutex
	rows map[string]domain.Customer
}

func newMemRepo() *memRepo { return &memRepo{rows: map[string]domain.Customer{}} }

func (r *memRepo) Create(_ context.Context, c domain.Customer) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.rows[c.ID]; ok {
		return false, nil
	}
	r.rows[c.ID] = c
	return true, nil
}
func (r *memRepo) FindByID(_ context.Context, id string) (domain.Customer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.rows[id]
	if !ok {
		return domain.Customer{}, domain.ErrNotFound
	}
	return c, nil
}
func (r *memRepo) List(context.Context, int, int) ([]domain.Customer, error) { return nil, nil }
func (r *memRepo) Update(_ context.Context, c domain.Customer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows[c.ID] = c
	return nil
}
func (r *memRepo) SetStatus(_ context.Context, id string, version int64, st domain.Status) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.rows[id]; ok && c.Version == version {
		c.Status = st
		r.rows[id] = c
	}
	return nil
}
func (r *memRepo) ListUnpublished(_ context.Context, region string, limit int) ([]domain.Customer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []domain.Customer
	for _, c := range r.rows {
		if c.Status != domain.StatusPublished && c.OriginRegion == region {
			out = append(out, c)
		}
	}
	return out, nil
}
func (r *memRepo) CountByStatus(context.Context) (map[domain.Status]int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := map[domain.Status]int{}
	for _, c := range r.rows {
		m[c.Status]++
	}
	return m, nil
}
func (r *memRepo) ApplyReplicated(_ context.Context, c domain.Customer) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.rows[c.ID]; ok && cur.Version >= c.Version {
		return false, nil
	}
	c.Status = domain.StatusPublished
	r.rows[c.ID] = c
	return true, nil
}

type fakePub struct {
	mu     sync.Mutex
	fail   bool
	events []domain.Customer
}

func (p *fakePub) Publish(_ context.Context, c domain.Customer) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		return errors.New("broker down")
	}
	p.events = append(p.events, c)
	return nil
}

type nopTel struct{}

func (nopTel) EventPublished(bool)                {}
func (nopTel) Replicated(string, string)          {}
func (nopTel) ReplicationLag(time.Duration)       {}
func (nopTel) StatusCounts(map[domain.Status]int) {}

func newSvc(region string) (*Service, *memRepo, *fakePub) {
	repo, pub := newMemRepo(), &fakePub{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewService(repo, pub, nopTel{}, region, time.Second, log), repo, pub
}

var cmd = ports.CreateCustomerCommand{Name: "Ana", Address: "Rua A, 1", Email: "ana@example.com", Nickname: "ana"}

// ---- tests ----

func TestCreatePublishesToRegionTopicAndMarksPublished(t *testing.T) {
	svc, repo, pub := newSvc("a")
	c, created, err := svc.Create(context.Background(), cmd)
	if err != nil || !created {
		t.Fatalf("create: created=%v err=%v", created, err)
	}
	if c.Status != domain.StatusPublished || c.OriginRegion != "a" {
		t.Fatalf("unexpected customer: %+v", c)
	}
	stored, _ := repo.FindByID(context.Background(), c.ID)
	if stored.Status != domain.StatusPublished || len(pub.events) != 1 {
		t.Fatalf("status=%s events=%d", stored.Status, len(pub.events))
	}
}

func TestPublishFailureKeepsCustomerAndRelayRecovers(t *testing.T) {
	svc, repo, pub := newSvc("a")
	pub.fail = true
	c, _, err := svc.Create(context.Background(), cmd)
	if err != nil {
		t.Fatalf("the write must succeed even with Kafka down: %v", err)
	}
	if got, _ := repo.FindByID(context.Background(), c.ID); got.Status != domain.StatusPublishFailed {
		t.Fatalf("want PUBLISH_FAILED, got %s", got.Status)
	}

	pub.fail = false // Kafka is back
	n, err := svc.RelayOutbox(context.Background(), 10)
	if err != nil || n != 1 {
		t.Fatalf("relay published=%d err=%v", n, err)
	}
	if got, _ := repo.FindByID(context.Background(), c.ID); got.Status != domain.StatusPublished {
		t.Fatalf("want PUBLISHED, got %s", got.Status)
	}
}

func TestCreateWithSameIdempotencyKeyReturnsExisting(t *testing.T) {
	svc, _, pub := newSvc("a")
	k := cmd
	k.ID = domain.NewID()
	first, created1, _ := svc.Create(context.Background(), k)
	second, created2, err := svc.Create(context.Background(), k)
	if err != nil || !created1 || created2 || first.ID != second.ID || len(pub.events) != 1 {
		t.Fatalf("created1=%v created2=%v err=%v events=%d", created1, created2, err, len(pub.events))
	}
}

func TestUpdateBumpsVersionAndRepublishes(t *testing.T) {
	svc, _, pub := newSvc("a")
	c, _, _ := svc.Create(context.Background(), cmd)
	u, err := svc.Update(context.Background(), ports.UpdateCustomerCommand{
		ID: c.ID, Name: "Ana Maria", Address: c.Address, Email: c.Email, Nickname: c.Nickname})
	if err != nil {
		t.Fatal(err)
	}
	if u.Version <= c.Version || len(pub.events) != 2 || u.Status != domain.StatusPublished {
		t.Fatalf("version %d->%d events=%d status=%s", c.Version, u.Version, len(pub.events), u.Status)
	}
}

func TestDeleteIsSoftAndReplicated(t *testing.T) {
	svc, _, pub := newSvc("a")
	c, _, _ := svc.Create(context.Background(), cmd)
	if err := svc.Delete(context.Background(), c.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Get(context.Background(), c.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
	if last := pub.events[len(pub.events)-1]; !last.Deleted {
		t.Fatal("deletion event must carry deleted=true")
	}
}

func TestApplyReplicatedIgnoresOwnRegionAndStaleEvents(t *testing.T) {
	svc, repo, _ := newSvc("a")
	now := time.Now()
	remote, _ := domain.NewCustomer(domain.NewID(), "Bia", "Rua B", "bia@example.com", "bia", "b", now)

	if err := svc.ApplyReplicated(context.Background(), remote); err != nil {
		t.Fatal(err)
	}
	stored, err := repo.FindByID(context.Background(), remote.ID)
	if err != nil || stored.Status != domain.StatusPublished || stored.OriginRegion != "b" {
		t.Fatalf("replicated row should be PUBLISHED (nothing to publish): %+v err=%v", stored, err)
	}

	stale := remote
	stale.Name = "old"
	stale.Version--
	_ = svc.ApplyReplicated(context.Background(), stale)
	if got, _ := repo.FindByID(context.Background(), remote.ID); got.Name != "Bia" {
		t.Fatal("stale event must not overwrite newer data")
	}

	own, _ := domain.NewCustomer(domain.NewID(), "Caio", "Rua C", "c@example.com", "c", "a", now)
	_ = svc.ApplyReplicated(context.Background(), own)
	if _, err := repo.FindByID(context.Background(), own.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("events from our own region must be ignored (loop prevention)")
	}
}

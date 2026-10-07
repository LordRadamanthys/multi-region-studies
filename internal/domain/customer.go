// Package domain holds the business rules. It knows nothing about HTTP, SQL or Kafka.
package domain

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"time"
)

// Status tracks whether the change event for the customer reached the regional topic.
//
//	PENDING        saved in the DB, event not published yet
//	PUBLISHED      event delivered to the region's topic
//	PUBLISH_FAILED publish attempt failed; the relay keeps retrying
type Status string

const (
	StatusPending       Status = "PENDING"
	StatusPublished     Status = "PUBLISHED"
	StatusPublishFailed Status = "PUBLISH_FAILED"
)

var AllStatuses = []Status{StatusPending, StatusPublished, StatusPublishFailed}

var (
	ErrNotFound = errors.New("customer not found")
	ErrInvalid  = errors.New("invalid customer")
	ErrConflict = errors.New("concurrent update, retry")
)

type Customer struct {
	ID       string
	Name     string
	Address  string
	Email    string
	Nickname string

	Status       Status
	Deleted      bool   // soft delete, so the deletion can be replicated
	OriginRegion string // region of the last writer
	Version      int64  // unix micros of the last write; used for last-write-wins across regions
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

var idPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func IsValidID(id string) bool { return idPattern.MatchString(id) }

// NewID returns a random UUID v4.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func NewCustomer(id, name, address, email, nickname, region string, now time.Time) (Customer, error) {
	c := Customer{
		ID:       id,
		Name:     strings.TrimSpace(name),
		Address:  strings.TrimSpace(address),
		Email:    strings.TrimSpace(email),
		Nickname: strings.TrimSpace(nickname),
	}
	if err := c.validate(); err != nil {
		return Customer{}, err
	}
	now = now.UTC().Truncate(time.Microsecond)
	c.CreatedAt = now
	c.touch(region, now)
	return c, nil
}

func (c *Customer) Update(name, address, email, nickname, region string, now time.Time) error {
	next := *c
	next.Name = strings.TrimSpace(name)
	next.Address = strings.TrimSpace(address)
	next.Email = strings.TrimSpace(email)
	next.Nickname = strings.TrimSpace(nickname)
	if err := next.validate(); err != nil {
		return err
	}
	next.touch(region, now.UTC().Truncate(time.Microsecond))
	*c = next
	return nil
}

func (c *Customer) MarkDeleted(region string, now time.Time) {
	c.Deleted = true
	c.touch(region, now.UTC().Truncate(time.Microsecond))
}

// touch registers a local write: new version, local region as origin, event still to be published.
func (c *Customer) touch(region string, now time.Time) {
	v := now.UnixMicro()
	if v <= c.Version {
		v = c.Version + 1
	}
	c.Version = v
	c.UpdatedAt = now
	c.OriginRegion = region
	c.Status = StatusPending
}

func (c Customer) validate() error {
	switch {
	case c.Name == "":
		return fmt.Errorf("%w: name is required", ErrInvalid)
	case c.Address == "":
		return fmt.Errorf("%w: address is required", ErrInvalid)
	case c.Nickname == "":
		return fmt.Errorf("%w: nickname is required", ErrInvalid)
	}
	if _, err := mail.ParseAddress(c.Email); err != nil {
		return fmt.Errorf("%w: email is not valid", ErrInvalid)
	}
	return nil
}

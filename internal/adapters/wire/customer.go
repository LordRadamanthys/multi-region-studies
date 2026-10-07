// Package wire is the JSON contract of the customer events that travel between regions.
package wire

import (
	"encoding/json"
	"errors"
	"time"

	"multi-region/internal/domain"
)

type customer struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Address      string    `json:"address"`
	Email        string    `json:"email"`
	Nickname     string    `json:"nickname"`
	Deleted      bool      `json:"deleted"`
	OriginRegion string    `json:"origin_region"`
	Version      int64     `json:"version"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func Encode(c domain.Customer) ([]byte, error) {
	return json.Marshal(customer{
		ID: c.ID, Name: c.Name, Address: c.Address, Email: c.Email, Nickname: c.Nickname,
		Deleted: c.Deleted, OriginRegion: c.OriginRegion, Version: c.Version,
		CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt,
	})
}

func Decode(b []byte) (domain.Customer, error) {
	var w customer
	if err := json.Unmarshal(b, &w); err != nil {
		return domain.Customer{}, err
	}
	if w.ID == "" || w.OriginRegion == "" || w.Version == 0 {
		return domain.Customer{}, errors.New("event is missing id, origin_region or version")
	}
	return domain.Customer{
		ID: w.ID, Name: w.Name, Address: w.Address, Email: w.Email, Nickname: w.Nickname,
		Deleted: w.Deleted, OriginRegion: w.OriginRegion, Version: w.Version,
		CreatedAt: w.CreatedAt, UpdatedAt: w.UpdatedAt,
	}, nil
}

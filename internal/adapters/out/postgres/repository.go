// Package postgres is the outbound adapter that persists customers.
package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"multi-region/internal/domain"
	"multi-region/internal/ports"
)

var _ ports.CustomerRepository = (*Repository)(nil)

const schema = `
CREATE TABLE IF NOT EXISTS customers (
    id            TEXT PRIMARY KEY,
    name          TEXT        NOT NULL,
    address       TEXT        NOT NULL,
    email         TEXT        NOT NULL,
    nickname      TEXT        NOT NULL,
    status        TEXT        NOT NULL CHECK (status IN ('PENDING','PUBLISHED','PUBLISH_FAILED')),
    deleted       BOOLEAN     NOT NULL DEFAULT FALSE,
    origin_region TEXT        NOT NULL,
    version       BIGINT      NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_customers_unpublished ON customers (updated_at) WHERE status <> 'PUBLISHED';
CREATE INDEX IF NOT EXISTS idx_customers_created ON customers (created_at DESC) WHERE deleted = FALSE;
`

const cols = `id, name, address, email, nickname, status, deleted, origin_region, version, created_at, updated_at`

type Repository struct{ pool *pgxpool.Pool }

func New(ctx context.Context, dsn string) (*Repository, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 20
	cfg.HealthCheckPeriod = 5 * time.Second
	cfg.ConnConfig.ConnectTimeout = 2 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg) // lazy: does not connect yet
	if err != nil {
		return nil, err
	}
	return &Repository{pool: pool}, nil
}

func (r *Repository) Close() { r.pool.Close() }

func (r *Repository) Migrate(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, schema)
	return err
}

func (r *Repository) Ping(ctx context.Context) error { return r.pool.Ping(ctx) }

func (r *Repository) Create(ctx context.Context, c domain.Customer) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`INSERT INTO customers (`+cols+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		 ON CONFLICT (id) DO NOTHING`,
		c.ID, c.Name, c.Address, c.Email, c.Nickname, string(c.Status), c.Deleted,
		c.OriginRegion, c.Version, c.CreatedAt, c.UpdatedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (r *Repository) FindByID(ctx context.Context, id string) (domain.Customer, error) {
	c, err := scan(r.pool.QueryRow(ctx, `SELECT `+cols+` FROM customers WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Customer{}, domain.ErrNotFound
	}
	return c, err
}

func (r *Repository) List(ctx context.Context, limit, offset int) ([]domain.Customer, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT `+cols+` FROM customers WHERE deleted = FALSE ORDER BY created_at DESC LIMIT $1 OFFSET $2`,
		limit, offset)
	return collect(rows, err)
}

func (r *Repository) Update(ctx context.Context, c domain.Customer) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE customers
		    SET name=$2, address=$3, email=$4, nickname=$5, status=$6, deleted=$7,
		        origin_region=$8, version=$9, updated_at=$10
		  WHERE id=$1 AND version < $9`,
		c.ID, c.Name, c.Address, c.Email, c.Nickname, string(c.Status), c.Deleted,
		c.OriginRegion, c.Version, c.UpdatedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrConflict
	}
	return nil
}

func (r *Repository) SetStatus(ctx context.Context, id string, version int64, st domain.Status) error {
	_, err := r.pool.Exec(ctx, `UPDATE customers SET status = $3 WHERE id = $1 AND version = $2`, id, version, string(st))
	return err
}

func (r *Repository) ListUnpublished(ctx context.Context, region string, limit int) ([]domain.Customer, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT `+cols+` FROM customers
		  WHERE status <> 'PUBLISHED' AND origin_region = $1
		  ORDER BY updated_at LIMIT $2`, region, limit)
	return collect(rows, err)
}

func (r *Repository) CountByStatus(ctx context.Context) (map[domain.Status]int, error) {
	rows, err := r.pool.Query(ctx, `SELECT status, count(*) FROM customers GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[domain.Status]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[domain.Status(st)] = n
	}
	return out, rows.Err()
}

// ApplyReplicated is an idempotent last-write-wins upsert. Replicated rows are stored as PUBLISHED
// because the origin region already published them; this region has nothing to send.
func (r *Repository) ApplyReplicated(ctx context.Context, c domain.Customer) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`INSERT INTO customers (`+cols+`) VALUES ($1,$2,$3,$4,$5,'PUBLISHED',$6,$7,$8,$9,$10)
		 ON CONFLICT (id) DO UPDATE SET
		     name=EXCLUDED.name, address=EXCLUDED.address, email=EXCLUDED.email, nickname=EXCLUDED.nickname,
		     status='PUBLISHED', deleted=EXCLUDED.deleted, origin_region=EXCLUDED.origin_region,
		     version=EXCLUDED.version, updated_at=EXCLUDED.updated_at
		 WHERE customers.version < EXCLUDED.version
		    OR (customers.version = EXCLUDED.version AND customers.origin_region < EXCLUDED.origin_region)`,
		c.ID, c.Name, c.Address, c.Email, c.Nickname, c.Deleted,
		c.OriginRegion, c.Version, c.CreatedAt, c.UpdatedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func collect(rows pgx.Rows, err error) ([]domain.Customer, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Customer
	for rows.Next() {
		c, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func scan(row pgx.Row) (domain.Customer, error) {
	var c domain.Customer
	var st string
	err := row.Scan(&c.ID, &c.Name, &c.Address, &c.Email, &c.Nickname, &st, &c.Deleted,
		&c.OriginRegion, &c.Version, &c.CreatedAt, &c.UpdatedAt)
	c.Status = domain.Status(st)
	c.CreatedAt, c.UpdatedAt = c.CreatedAt.UTC(), c.UpdatedAt.UTC()
	return c, err
}

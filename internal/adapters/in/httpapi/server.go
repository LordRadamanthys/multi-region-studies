// Package httpapi is the inbound HTTP adapter (CRUD + health + metrics).
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"multi-region/internal/domain"
	"multi-region/internal/health"
	"multi-region/internal/ports"
)

type Observer interface {
	ObserveRequest(method, route string, code int, d time.Duration)
}

type Server struct {
	uc             ports.CustomerUseCase
	region         string
	checks         []health.Check
	obs            Observer
	metricsHandler http.Handler
	log            *slog.Logger
}

func New(uc ports.CustomerUseCase, region string, checks []health.Check, obs Observer,
	metricsHandler http.Handler, log *slog.Logger) *Server {
	return &Server{uc: uc, region: region, checks: checks, obs: obs, metricsHandler: metricsHandler, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /customers", s.create)
	mux.HandleFunc("GET /customers", s.list)
	mux.HandleFunc("GET /customers/{id}", s.get)
	mux.HandleFunc("PUT /customers/{id}", s.update)
	mux.HandleFunc("DELETE /customers/{id}", s.remove)
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("GET /metrics", s.metricsHandler)
	return s.middleware(mux)
}

// ---- DTOs ----

type customerRequest struct {
	Name     string `json:"name"`
	Address  string `json:"address"`
	Email    string `json:"email"`
	Nickname string `json:"nickname"`
}

type customerResponse struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Address      string    `json:"address"`
	Email        string    `json:"email"`
	Nickname     string    `json:"nickname"`
	Status       string    `json:"status"`
	OriginRegion string    `json:"origin_region"`
	Version      int64     `json:"version"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func toResponse(c domain.Customer) customerResponse {
	return customerResponse{
		ID: c.ID, Name: c.Name, Address: c.Address, Email: c.Email, Nickname: c.Nickname,
		Status: string(c.Status), OriginRegion: c.OriginRegion, Version: c.Version,
		CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt,
	}
}

// ---- handlers ----

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var req customerRequest
	if !decode(w, r, &req) {
		return
	}
	c, created, err := s.uc.Create(r.Context(), ports.CreateCustomerCommand{
		ID: r.Header.Get("Idempotency-Key"), Name: req.Name, Address: req.Address, Email: req.Email, Nickname: req.Nickname,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	code := http.StatusCreated
	if !created {
		code = http.StatusOK
	}
	writeJSON(w, code, toResponse(c))
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	c, err := s.uc.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toResponse(c))
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	limit := clamp(atoi(r.URL.Query().Get("limit"), 20), 1, 200)
	offset := max(atoi(r.URL.Query().Get("offset"), 0), 0)
	items, err := s.uc.List(r.Context(), limit, offset)
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]customerResponse, 0, len(items))
	for _, c := range items {
		out = append(out, toResponse(c))
	}
	writeJSON(w, http.StatusOK, map[string]any{"region": s.region, "items": out})
}

func (s *Server) update(w http.ResponseWriter, r *http.Request) {
	var req customerRequest
	if !decode(w, r, &req) {
		return
	}
	c, err := s.uc.Update(r.Context(), ports.UpdateCustomerCommand{
		ID: r.PathValue("id"), Name: req.Name, Address: req.Address, Email: req.Email, Nickname: req.Nickname,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toResponse(c))
}

func (s *Server) remove(w http.ResponseWriter, r *http.Request) {
	if err := s.uc.Delete(r.Context(), r.PathValue("id")); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// health is what the global router polls. 200 = "send traffic here".
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	results := health.Run(r.Context(), s.checks, 1500*time.Millisecond)
	checks := map[string]string{}
	for _, res := range results {
		if res.Err != nil {
			checks[res.Name] = "down"
		} else {
			checks[res.Name] = "up"
		}
	}
	code, status := http.StatusOK, "ok"
	if !health.Healthy(results) {
		code, status = http.StatusServiceUnavailable, "unavailable"
	}
	writeJSON(w, code, map[string]any{"region": s.region, "status": status, "checks": checks})
}

// ---- plumbing ----

func (s *Server) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalid):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, domain.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
	case errors.Is(err, domain.ErrConflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	default:
		s.log.Error("request failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "storage unavailable in region " + s.region})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		w.Header().Set("X-Region", s.region)
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		r = r.WithContext(ctx)

		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		s.obs.ObserveRequest(r.Method, route, rec.code, time.Since(start))
	})
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func atoi(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

func clamp(v, lo, hi int) int { return min(max(v, lo), hi) }

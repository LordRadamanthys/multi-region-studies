// Package metrics exposes Prometheus metrics and implements ports.Telemetry.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"multi-region/internal/domain"
	"multi-region/internal/ports"
)

var _ ports.Telemetry = (*Metrics)(nil)

type Metrics struct {
	reg         *prometheus.Registry
	httpReqs    *prometheus.CounterVec
	httpDur     *prometheus.HistogramVec
	published   *prometheus.CounterVec
	byStatus    *prometheus.GaugeVec
	replApplied *prometheus.CounterVec
	replLag     prometheus.Gauge
	depUp       *prometheus.GaugeVec
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := promauto.With(reg)

	m := &Metrics{
		reg: reg,
		httpReqs: f.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total", Help: "HTTP requests by route and status code."},
			[]string{"method", "route", "code"}),
		httpDur: f.NewHistogramVec(prometheus.HistogramOpts{
			Name: "http_request_duration_seconds", Help: "HTTP latency.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}},
			[]string{"route"}),
		published: f.NewCounterVec(prometheus.CounterOpts{
			Name: "customer_events_published_total", Help: "Publish attempts to this region's topic."},
			[]string{"result"}),
		byStatus: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "customers_by_status", Help: "Rows per publish status (PENDING/PUBLISH_FAILED = outbox backlog)."},
			[]string{"status"}),
		replApplied: f.NewCounterVec(prometheus.CounterOpts{
			Name: "replication_applied_total", Help: "Events consumed from the other region."},
			[]string{"origin_region", "result"}),
		replLag: f.NewGauge(prometheus.GaugeOpts{
			Name: "replication_lag_seconds", Help: "Age of the last replicated event when it was applied."}),
		depUp: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dependency_up", Help: "1 if the dependency answered its last check."},
			[]string{"dependency"}),
	}
	for _, r := range []string{"ok", "error"} {
		m.published.WithLabelValues(r)
	}
	for _, s := range domain.AllStatuses {
		m.byStatus.WithLabelValues(string(s)).Set(0)
	}
	return m
}

func (m *Metrics) Handler() http.Handler { return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}) }

// ---- ports.Telemetry ----

func (m *Metrics) EventPublished(ok bool) {
	if ok {
		m.published.WithLabelValues("ok").Inc()
	} else {
		m.published.WithLabelValues("error").Inc()
	}
}

func (m *Metrics) Replicated(origin, result string) {
	m.replApplied.WithLabelValues(origin, result).Inc()
}

func (m *Metrics) ReplicationLag(d time.Duration) {
	if d < 0 {
		d = 0
	}
	m.replLag.Set(d.Seconds())
}

func (m *Metrics) StatusCounts(counts map[domain.Status]int) {
	for _, s := range domain.AllStatuses {
		m.byStatus.WithLabelValues(string(s)).Set(float64(counts[s]))
	}
}

// ---- helpers used by the other adapters / main ----

func (m *Metrics) SetDependency(name string, up bool) {
	v := 0.0
	if up {
		v = 1
	}
	m.depUp.WithLabelValues(name).Set(v)
}

func (m *Metrics) ObserveRequest(method, route string, code int, d time.Duration) {
	m.httpReqs.WithLabelValues(method, route, strconv.Itoa(code)).Inc()
	m.httpDur.WithLabelValues(route).Observe(d.Seconds())
}

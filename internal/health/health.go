// Package health runs dependency checks concurrently with a shared deadline.
package health

import (
	"context"
	"sync"
	"time"
)

type Check struct {
	Name     string
	Required bool // a failing required check makes /health return 503
	Fn       func(ctx context.Context) error
}

type Result struct {
	Name     string
	Required bool
	Err      error
}

func Run(ctx context.Context, checks []Check, timeout time.Duration) []Result {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	results := make([]Result, len(checks))
	var wg sync.WaitGroup
	for i, c := range checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = Result{Name: c.Name, Required: c.Required, Err: c.Fn(ctx)}
		}()
	}
	wg.Wait()
	return results
}

func Healthy(rs []Result) bool {
	for _, r := range rs {
		if r.Required && r.Err != nil {
			return false
		}
	}
	return true
}

package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

type Task func(ctx context.Context) (int, error)

type Loop struct {
	name     string
	task     Task
	interval time.Duration
	timeout  time.Duration
	log      *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func NewLoop(name string, interval, timeout time.Duration, task Task, log *slog.Logger) *Loop {
	return &Loop{name: name, task: task, interval: interval, timeout: timeout, log: log.With("worker", name)}
}

func (l *Loop) Start(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done != nil {
		return errors.New(l.name + " already started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	l.done = make(chan struct{})
	go l.run(ctx)
	l.log.Info("worker started")
	return nil
}

func (l *Loop) Stop(ctx context.Context) error {
	l.mu.Lock()
	cancel, done := l.cancel, l.done
	l.mu.Unlock()
	if done == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		l.log.Info("worker stopped")
		return nil
	case <-ctx.Done():
		return errors.New(l.name + " did not stop in time")
	}
}

func (l *Loop) Done() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.done
}

func (l *Loop) run(ctx context.Context) {
	defer close(l.done)
	failures := 0
	for ctx.Err() == nil {
		rctx, cancel := context.WithTimeout(ctx, l.timeout)
		n, err := l.task(rctx)
		cancel()
		wait := l.interval
		switch {
		case err != nil && ctx.Err() == nil:
			failures++
			wait = min(l.interval<<min(failures, 6), 30*time.Second)
			l.log.Warn("worker run failed", "error", err, "retryIn", wait.String())
		case n > 0:
			failures = 0
			continue
		default:
			failures = 0
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
		case <-t.C:
		}
	}
}

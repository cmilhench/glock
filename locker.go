package glock

import (
	"context"
	"time"
)

// Locker defines the interface for distributed lock implementations
type Locker interface {
	// Acquire attempts to acquire the lock
	Acquire(ctx context.Context) (bool, error)
	// Release attempts to release the lock
	Release(ctx context.Context) (bool, error)
	// Renew attempts to renew the lock
	Renew(ctx context.Context) (bool, error)
}

// Options defines configuration for lock operations
type Options struct {
	RetryInterval time.Duration
	Timeout       time.Duration
}

// Config holds common configuration for lock implementations
type Config struct {
	LockID      string
	Owner       string
	LockTimeout time.Duration
}

func WaitAcquire(ctx context.Context, locker Locker, opts Options) (bool, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	ticker := time.NewTicker(opts.RetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			return false, timeoutCtx.Err()
		case <-ticker.C:
			acquired, err := locker.Acquire(ctx)
			if err != nil {
				return false, err
			}
			if acquired {
				return true, nil
			}
		}
	}
}

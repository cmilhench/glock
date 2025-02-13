package glock

import (
	"context"
	"fmt"
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

func WithLock(ctx context.Context, locker Locker, renewInterval time.Duration, work func(context.Context) error) error {
	// Assume lock acquire already?
	// acquired, err := locker.Acquire(ctx)
	// if err != nil {
	// 	return fmt.Errorf("failed to acquire lock: %w", err)
	// }
	// if !acquired {
	// 	return fmt.Errorf("could not acquire lock")
	// }

	defer func() {
		_, _ = locker.Release(ctx)
	}()

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errChan := make(chan error, 1)

	go func() {
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()

		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
				renewed, err := locker.Renew(ctx)
				if err != nil {
					errChan <- fmt.Errorf("failed to renew lock: %w", err)
					cancel()
					return
				}
				if !renewed {
					errChan <- fmt.Errorf("lock renewal failed")
					cancel()
					return
				}
			}
		}
	}()

	go func() {
		if err := work(workCtx); err != nil {
			errChan <- fmt.Errorf("work failed: %w", err)
		}
		close(errChan)
	}()

	if err := <-errChan; err != nil {
		return err
	}

	return nil
}

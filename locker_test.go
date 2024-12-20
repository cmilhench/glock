package glock_test

import (
	"context"
	"testing"
	"time"

	. "github.com/cmilhench/glock"
)

func TestAcquire(t *testing.T) {
	store := make(map[string]Lock)
	lock1 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-1",
		LockTimeout: 2 * time.Second,
	})

	acquired, err := lock1.Acquire(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !acquired {
		t.Fatalf("expected lock to be acquired")
	}
}

func TestAcquireFailure(t *testing.T) {
	store := make(map[string]Lock)
	lock1 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-1",
		LockTimeout: 2 * time.Second,
	})
	lock2 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-2",
		LockTimeout: 2 * time.Second,
	})

	ctx := context.Background()
	acquired1, err := lock1.Acquire(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !acquired1 {
		t.Fatalf("expected lock to be acquired")
	}

	acquired2, err := lock2.Acquire(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if acquired2 {
		t.Fatalf("expected lock to not be acquired")
	}
}

func TestAcquireFailureWithCancelledContext(t *testing.T) {
	store := make(map[string]Lock)
	lock1 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-1",
		LockTimeout: 2 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	acquired, err := lock1.Acquire(ctx)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if acquired {
		t.Fatalf("expected lock to not be acquired")
	}
}

func TestAcquireFailureWithConcurrency(t *testing.T) {
	store := make(map[string]Lock)
	lock1 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-1",
		LockTimeout: 2 * time.Second,
	})
	lock2 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-2",
		LockTimeout: 2 * time.Second,
	})

	ctx := context.Background()

	acquiredCh := make(chan bool)
	go func() {
		acquired, err := lock1.Acquire(ctx)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		acquiredCh <- acquired
	}()

	// Wait for the first lock to be acquired
	firstAcquired := <-acquiredCh
	if !firstAcquired {
		t.Fatalf("expected first lock to be acquired")
	}

	go func() {
		acquired, err := lock2.Acquire(ctx)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		acquiredCh <- acquired
	}()

	// Attempt to acquire the second lock should fail
	secondAcquired := <-acquiredCh
	if secondAcquired {
		t.Fatalf("expected second lock to not be acquired")
	}
}

func TestAcquireWithTimeout(t *testing.T) {
	store := make(map[string]Lock)
	lock1 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-1",
		LockTimeout: 2 * time.Second,
	})
	lock2 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-2",
		LockTimeout: 2 * time.Second,
	})
	ctx := context.Background()

	acquired, err := lock1.Acquire(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !acquired {
		t.Fatalf("expected first lock to be acquired")
	}

	time.Sleep(2 * time.Second)
	time.Sleep(time.Second)

	acquired, err = lock2.Acquire(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !acquired {
		t.Fatalf("expected second lock to be acquired after timeout")
	}
}

// ----------------------------------------------------------------------------

func TestRelease(t *testing.T) {
	store := make(map[string]Lock)
	lock1 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-1",
		LockTimeout: 2 * time.Second,
	})

	released, err := lock1.Release(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !released {
		t.Fatalf("expected lock to be released")
	}
}

func TestReleaseFailure(t *testing.T) {
	store := make(map[string]Lock)
	lock1 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-1",
		LockTimeout: 2 * time.Second,
	})
	lock2 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-2",
		LockTimeout: 2 * time.Second,
	})

	ctx := context.Background()
	acquired, err := lock1.Acquire(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !acquired {
		t.Fatalf("expected lock to be acquired")
	}

	released, err := lock2.Release(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if released {
		t.Fatalf("expected lock to not be released")
	}
}

func TestReleaseFailureWithCancelledContext(t *testing.T) {
	store := make(map[string]Lock)
	lock1 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-1",
		LockTimeout: 2 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	released, err := lock1.Release(ctx)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if released {
		t.Fatalf("expected lock to not be released")
	}
}

// ----------------------------------------------------------------------------

func TestRenew(t *testing.T) {
	store := make(map[string]Lock)
	lock1 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-1",
		LockTimeout: 2 * time.Second,
	})

	ctx := context.Background()
	acquired, err := lock1.Acquire(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !acquired {
		t.Fatalf("expected lock to be acquired")
	}

	renewed, err := lock1.Renew(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !renewed {
		t.Fatalf("expected lock to be renewed")
	}
}

func TestRenewFailure(t *testing.T) {
	store := make(map[string]Lock)
	lock1 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-1",
		LockTimeout: 2 * time.Second,
	})
	lock2 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-2",
		LockTimeout: 2 * time.Second,
	})

	ctx := context.Background()
	acquired, err := lock1.Acquire(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !acquired {
		t.Fatalf("expected lock to be acquired")
	}

	renewed, err := lock2.Renew(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if renewed {
		t.Fatalf("expected lock to not be renewed")
	}
}

func TestRenewFailureWithCancelledContext(t *testing.T) {
	store := make(map[string]Lock)
	lock1 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-1",
		LockTimeout: 2 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	renewed, err := lock1.Renew(ctx)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if renewed {
		t.Fatalf("expected lock to not be renewed")
	}
}

// ----------------------------------------------------------------------------

func TestWaitAcquire(t *testing.T) {
	store := make(map[string]Lock)
	lock1 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-1",
		LockTimeout: 2 * time.Second,
	})
	lock2 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-2",
		LockTimeout: 2 * time.Second,
	})
	ctx := context.Background()

	acquiredCh := make(chan bool)
	go func() {
		acquired, err := lock1.Acquire(ctx)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		acquiredCh <- acquired
	}()

	// Wait for the first lock to be acquired
	firstAcquired := <-acquiredCh
	if !firstAcquired {
		t.Fatalf("expected first lock to be acquired")
	}

	go func() {
		opts := Options{
			RetryInterval: 500 * time.Millisecond,
			Timeout:       5 * time.Second,
		}
		acquired, err := WaitAcquire(ctx, lock2, opts)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		acquiredCh <- acquired
	}()

	// Release the first lock
	released, err := lock1.Release(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !released {
		t.Fatalf("expected lock to be released")
	}

	// Attempt to acquire the second lock should succeed
	secondAcquired := <-acquiredCh
	if !secondAcquired {
		t.Fatalf("expected second lock to be acquired")
	}
}

func TestWaitAcquireFailureWithTimeout(t *testing.T) {
	store := make(map[string]Lock)
	lock1 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-1",
		LockTimeout: 2 * time.Second,
	})
	lock2 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-2",
		LockTimeout: 2 * time.Second,
	})
	ctx := context.Background()

	acquired, err := lock1.Acquire(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !acquired {
		t.Fatalf("expected first lock to be acquired")
	}

	opts := Options{
		RetryInterval: 500 * time.Millisecond,
		Timeout:       1 * time.Second,
	}

	acquired, err = WaitAcquire(context.Background(), lock2, opts)
	if err == nil {
		t.Fatalf("expected error, got %v", err)
	}
	if acquired {
		t.Fatalf("expected lock to not be acquired")
	}
}

func TestWaitAcquireFailureWithCancelledContext(t *testing.T) {
	store := make(map[string]Lock)
	lock1 := NewMemoryLock(store, Config{
		LockID:      "test-lock",
		Owner:       "owner-1",
		LockTimeout: 2 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	opts := Options{
		RetryInterval: 500 * time.Millisecond,
		Timeout:       1 * time.Second,
	}

	acquired, err := WaitAcquire(ctx, lock1, opts)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if acquired {
		t.Fatalf("expected lock to not be renewed")
	}
}

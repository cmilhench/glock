package glock

import (
	"context"
	"sync"
	"time"
)

// Interface assertion
var _ Locker = (*MemoryLock)(nil)

type Lock struct {
	Owner string
	TTL   int64
}

type MemoryLock struct {
	sync.Mutex
	config Config
	store  map[string]Lock
}

func NewMemoryLock(store map[string]Lock, config Config) Locker {
	return &MemoryLock{
		config: config,
		store:  store,
	}
}

// Acquire attempts to acquire the lock
func (m *MemoryLock) Acquire(ctx context.Context) (bool, error) {
	m.Lock()
	defer m.Unlock()
	now := time.Now().Unix()
	ttl := time.Now().Add(m.config.LockTimeout).Unix()
	key := m.config.LockID
	owner := m.config.Owner
	if err := ctx.Err(); err != nil {
		return false, ctx.Err()
	} else if v, ok := m.store[key]; !ok {
		m.store[key] = Lock{owner, ttl}
	} else if v.TTL < now {
		m.store[key] = Lock{owner, ttl}
	} else {
		return false, nil
	}
	return true, nil
}

// Release attempts to release the lock
func (m *MemoryLock) Release(ctx context.Context) (bool, error) {
	m.Lock()
	defer m.Unlock()
	key := m.config.LockID
	owner := m.config.Owner
	if err := ctx.Err(); err != nil {
		return false, ctx.Err()
	} else if v, ok := m.store[key]; ok {
		if v.Owner != owner {
			return false, nil
		}
		delete(m.store, key)
	}
	return true, nil
}

// Renew attempts to renew the lock
func (m *MemoryLock) Renew(ctx context.Context) (bool, error) {
	m.Lock()
	defer m.Unlock()
	ttl := time.Now().Add(m.config.LockTimeout).Unix()
	key := m.config.LockID
	owner := m.config.Owner
	if err := ctx.Err(); err != nil {
		return false, ctx.Err()
	} else if v, ok := m.store[key]; !ok {
		m.store[key] = Lock{owner, ttl}
	} else if v.Owner == owner {
		m.store[key] = Lock{owner, ttl}
	} else {
		return false, nil
	}
	return true, nil
}

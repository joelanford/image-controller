package cache

import (
	"errors"
	"sync"
	"time"

	utilsclock "k8s.io/utils/clock"
)

type Memory[K comparable, V any] struct {
	cacheMu sync.RWMutex
	cache   map[K]timestampedValue[V]
	clock   utilsclock.Clock
}

type timestampedValue[V any] struct {
	value V
	time  *time.Time
}

func NewMemory[K comparable, V any]() Cache[K, V] {
	return &Memory[K, V]{
		cache: make(map[K]timestampedValue[V]),
		clock: &utilsclock.RealClock{},
	}
}

func (m *Memory[K, V]) Get(key K) (V, *time.Time, error) {
	m.cacheMu.RLock()
	defer m.cacheMu.RUnlock()
	v, ok := m.cache[key]
	if !ok {
		return *new(V), nil, NotFoundError(errors.New("key not found"))
	}
	return v.value, v.time, nil
}

func (m *Memory[K, V]) Set(key K, value V) error {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	now := m.clock.Now()
	m.cache[key] = timestampedValue[V]{value: value, time: &now}
	return nil
}

func (m *Memory[K, V]) Delete(key K) error {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	delete(m.cache, key)
	return nil
}

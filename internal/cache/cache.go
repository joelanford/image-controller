package cache

import (
	"errors"
	"time"
)

type Cache[K comparable, V any] interface {
	Get(key K) (V, *time.Time, error)
	Set(key K, value V) error
	Delete(key K) error
}

func NotFoundError(wrapped error) error {
	return &notFoundError{err: wrapped}
}

type notFoundError struct {
	err error
}

// This function will return nil if te.err is nil.
func (te *notFoundError) Unwrap() error {
	return te.err
}

func (te *notFoundError) Error() string {
	if te.err == nil {
		return "nil not found error"
	}
	return "item not found: " + te.err.Error()
}

func (te *notFoundError) Is(target error) bool {
	tp := &notFoundError{}
	return errors.As(target, &tp)
}

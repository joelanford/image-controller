package async

import (
	"context"
	"errors"
	"sync"

	"k8s.io/client-go/util/workqueue"
)

type Service[RequestID comparable] interface {
	Start(context.Context) error
	RegisterCallback(func(context.Context, RequestID) error)
	Do(RequestID, func(context.Context) error) bool
	Cancel(...RequestID)
	WalkRequests(func(RequestID))
}

type SimpleService[RequestID comparable] struct {
	concurrency int

	requestQueue workqueue.TypedRateLimitingInterface[RequestID]
	requestsMu   sync.RWMutex
	requests     map[RequestID]request

	callbacksMu sync.RWMutex
	callbacks   []func(context.Context, RequestID) error

	forceShutdownCtx context.Context
}

func NewService[RequestID comparable](
	concurrency int,
) Service[RequestID] {
	return &SimpleService[RequestID]{
		concurrency:  concurrency,
		requestQueue: workqueue.NewTypedRateLimitingQueue[RequestID](workqueue.DefaultTypedItemBasedRateLimiter[RequestID]()),
		requests:     make(map[RequestID]request),
	}
}

type request struct {
	do      func(context.Context) error
	baseCtx context.Context
	cancel  context.CancelFunc
}

func (s *SimpleService[RequestID]) RegisterCallback(cb func(context.Context, RequestID) error) {
	s.callbacksMu.Lock()
	defer s.callbacksMu.Unlock()
	s.callbacks = append(s.callbacks, cb)
}

func (s *SimpleService[RequestID]) Do(id RequestID, doFunc func(context.Context) error) bool {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()

	// If the request is already present, don't add it again.
	_, ok := s.requests[id]
	if ok {
		return false
	}

	// Create a request-specific context that allows us to cancel the request.
	baseRequestCtx, cancelRequest := context.WithCancel(context.Background())

	// Record the request
	s.requests[id] = request{
		do: func(ctx context.Context) (err error) {
			defer func() {
				s.callbacksMu.RLock()
				cbs := make([]func(context.Context, RequestID) error, len(s.callbacks))
				copy(cbs, s.callbacks)
				s.callbacksMu.RUnlock()

				callbackErrs := make([]error, 0, len(cbs))
				for _, cb := range cbs {
					if err := cb(ctx, id); err != nil {
						callbackErrs = append(callbackErrs, err)
					}
				}
				err = errors.Join(err, errors.Join(callbackErrs...))
			}()
			return doFunc(ctx)
		},
		baseCtx: baseRequestCtx,
		cancel:  cancelRequest,
	}

	// Add the request to the queue
	s.requestQueue.Add(id)
	return true
}

func (s *SimpleService[RequestID]) Cancel(ids ...RequestID) {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()

	for _, id := range ids {
		req, ok := s.requests[id]
		if ok {
			req.cancel()
			s.requestQueue.Forget(id)
			s.requestQueue.Done(id)
		}
	}
}

func (s *SimpleService[RequestID]) WalkRequests(walkFunc func(RequestID)) {
	s.requestsMu.RLock()
	defer s.requestsMu.RUnlock()

	for id := range s.requests {
		walkFunc(id)
	}
}

type contextKey string

var forceShutdownContextKey = contextKey("forceShutdown")

func WithForceShutdownContext(ctx context.Context, forceShutdownCtx context.Context) context.Context {
	return context.WithValue(ctx, forceShutdownContextKey, forceShutdownCtx)
}

func getForceShutdownContext(ctx context.Context) context.Context {
	forceShutdownCtx := ctx.Value(forceShutdownContextKey)
	if forceShutdownCtx == nil {
		return context.Background()
	}
	return forceShutdownCtx.(context.Context)
}

func (s *SimpleService[RequestID]) Start(ctx context.Context) error {
	forceShutdownCtx := getForceShutdownContext(ctx)

	drained := make(chan struct{})
	go func() {
		<-ctx.Done()
		s.requestQueue.ShutDownWithDrain()
		close(drained)
	}()

	var wg sync.WaitGroup
	wg.Add(s.concurrency)
	for i := 0; i < s.concurrency; i++ {
		go func() {
			defer wg.Done()
			for s.processNextRequest(forceShutdownCtx) {
			}
		}()
	}
	<-ctx.Done()

	select {
	case <-drained:
	case <-forceShutdownCtx.Done():
		s.requestQueue.ShutDown()
		return forceShutdownCtx.Err()
	}
	wg.Wait()

	return nil
}

func (s *SimpleService[RequestID]) processNextRequest(forceShutdownContext context.Context) bool {
	// Pop an item from the queue
	id, shutdown := s.requestQueue.Get()
	if shutdown {
		return false
	}

	// Report that we're done working on the item when we return
	defer s.requestQueue.Done(id)

	// Find the request for the ID.
	req, ok := s.requests[id]
	if !ok {
		panic("request not found: this should never happen")
	}

	// If the force shutdown context is done, cancel the request.
	go func() {
		select {
		case <-forceShutdownContext.Done():
			req.cancel()
		case <-req.baseCtx.Done():
		}
	}()

	// Perform the request.
	// If it fails and the failure is not terminal, add the item back to the queue with rate limiting.
	// Otherwise, remove the item from the queue.
	reqErr := req.do(req.baseCtx)
	if reqErr != nil && !errors.Is(reqErr, &terminalError{}) && !errors.Is(reqErr, context.Canceled) {
		s.requestQueue.AddRateLimited(id)
	} else {
		s.requestsMu.Lock()
		defer s.requestsMu.Unlock()

		req.cancel()
		s.requestQueue.Forget(id)
		delete(s.requests, id)
	}

	return true
}

// TerminalError is an error that will not be retried but still be logged
// and recorded in metrics.
func TerminalError(wrapped error) error {
	return &terminalError{err: wrapped}
}

type terminalError struct {
	err error
}

// This function will return nil if te.err is nil.
func (te *terminalError) Unwrap() error {
	return te.err
}

func (te *terminalError) Error() string {
	if te.err == nil {
		return "nil terminal error"
	}
	return "terminal error: " + te.err.Error()
}

func (te *terminalError) Is(target error) bool {
	tp := &terminalError{}
	return errors.As(target, &tp)
}

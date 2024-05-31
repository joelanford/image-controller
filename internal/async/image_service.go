package async

import (
	"context"
	"errors"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"k8s.io/apimachinery/pkg/types"

	"github.com/joelanford/image-controller/internal/cache"
	"github.com/joelanford/image-controller/internal/image"
)

type ImageRequestID struct {
	Requester types.NamespacedName
	ImageID   string
}

type ImageResult struct {
	Reference string
	Image     *image.Image
	ModTime   *time.Time
	Error     error
}

type ImageService struct {
	svc       Service[ImageRequestID]
	cache     cache.Cache[types.NamespacedName, ImageResult]
	callbacks []func(ImageRequestID, ImageResult)
}

func NewImageService(concurrency int) *ImageService {
	return &ImageService{
		svc:   NewService[ImageRequestID](concurrency),
		cache: cache.NewMemory[types.NamespacedName, ImageResult](),
	}
}

func (s *ImageService) RegisterCallback(cb func(context.Context, ImageRequestID, ImageResult) error) {
	s.svc.RegisterCallback(func(ctx context.Context, reqID ImageRequestID) error {
		result, _, err := s.cache.Get(reqID.Requester)
		if err != nil {
			return err
		}
		return cb(ctx, reqID, result)
	})
}

func (s *ImageService) Start(ctx context.Context) error {
	return s.svc.Start(ctx)
}

func (s *ImageService) GetImage(ctx context.Context, reqID ImageRequestID, imageRef string, refreshInterval *time.Duration, pullOpts ...remote.Option) ImageResult {
	l := logr.FromContextOrDiscard(ctx).WithName("imageservice.get")

	s.cancelStaleRequests(reqID, l)
	result, modTime, err := s.cache.Get(reqID.Requester)
	if err != nil && !errors.Is(err, cache.NotFoundError(nil)) {
		return ImageResult{Error: err}
	}

	needsRefresh := false
	if errors.Is(err, cache.NotFoundError(nil)) {
		needsRefresh = true
		l.V(1).Info("image not found in cache, pulling")
	} else if result.Reference != imageRef {
		needsRefresh = true
		l.V(1).Info("image reference changed, pulling", "old", result.Reference, "new", imageRef)
	} else if refreshInterval != nil && modTime != nil && time.Since(*modTime) > *refreshInterval {
		needsRefresh = true
		l.V(1).Info("image refresh interval expired, pulling", "modTime", modTime, "refreshInterval", refreshInterval)
	}

	if needsRefresh {
		s.svc.Do(reqID, s.pullFunc(reqID.Requester, imageRef, pullOpts...))
		return ImageResult{}
	}

	result.ModTime = modTime
	return result
}

func (s *ImageService) DeleteImage(requestor types.NamespacedName) error {
	return s.cache.Delete(requestor)
}

func (s *ImageService) cancelStaleRequests(keepID ImageRequestID, l logr.Logger) {
	staleRequests := []ImageRequestID{}
	s.svc.WalkRequests(func(reqID ImageRequestID) {
		if reqID == keepID {
			return
		}
		if reqID.Requester == keepID.Requester {
			staleRequests = append(staleRequests, reqID)
		}
	})
	if len(staleRequests) > 0 {
		l.V(1).Info("canceling stale requests", "requests", staleRequests)
		s.svc.Cancel(staleRequests...)
	}
}

func (s *ImageService) pullFunc(requester types.NamespacedName, imageRef string, pullOpts ...remote.Option) func(context.Context) error {
	return func(ctx context.Context) error {
		img, err := image.Pull(ctx, imageRef, pullOpts...)
		result := ImageResult{Reference: imageRef, Image: img, Error: err}
		if err := s.cache.Set(requester, result); err != nil {
			return errors.Join(result.Error, err)
		}
		return result.Error
	}
}

type cacheError struct {
	err error
}

func IsCacheError(err error) bool {
	var ce *cacheError
	return errors.As(err, &ce)
}

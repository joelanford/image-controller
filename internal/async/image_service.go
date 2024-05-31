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

type ImageResult[Artifact any] struct {
	Reference string
	Artifact  Artifact
	ModTime   *time.Time
	Error     error
}

type ImageService[Artifact any] struct {
	svc              Service[ImageRequestID]
	cache            cache.Cache[types.NamespacedName, ImageResult[Artifact]]
	processImageFunc func(context.Context, ImageRequestID, *image.Image) (Artifact, error)
	callbacks        []func(ImageRequestID, ImageResult[Artifact])
}

func NewImageService[Artifact any](concurrency int, processImageFunc func(context.Context, ImageRequestID, *image.Image) (Artifact, error)) *ImageService[Artifact] {
	return &ImageService[Artifact]{
		svc:              NewService[ImageRequestID](concurrency),
		cache:            cache.NewMemory[types.NamespacedName, ImageResult[Artifact]](),
		processImageFunc: processImageFunc,
	}
}

func (s *ImageService[Artifact]) RegisterCallback(cb func(context.Context, ImageRequestID, ImageResult[Artifact]) error) {
	s.svc.RegisterCallback(func(ctx context.Context, reqID ImageRequestID) error {
		result, _, err := s.cache.Get(reqID.Requester)
		if err != nil {
			return err
		}
		return cb(ctx, reqID, result)
	})
}

func (s *ImageService[Artifact]) Start(ctx context.Context) error {
	return s.svc.Start(ctx)
}

func (s *ImageService[Artifact]) GetImage(ctx context.Context, reqID ImageRequestID, imageRef string, refreshInterval *time.Duration, pullOpts ...remote.Option) ImageResult[Artifact] {
	l := logr.FromContextOrDiscard(ctx).WithName("imageservice.get")

	s.cancelStaleRequests(reqID, l)
	result, modTime, err := s.cache.Get(reqID.Requester)
	if err != nil && !errors.Is(err, cache.NotFoundError(nil)) {
		return ImageResult[Artifact]{Error: err}
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
		s.svc.Do(reqID, s.pullFunc(reqID, imageRef, pullOpts...))
		return ImageResult[Artifact]{}
	}

	result.ModTime = modTime
	return result
}

func (s *ImageService[Artifact]) DeleteImage(requestor types.NamespacedName) error {
	return s.cache.Delete(requestor)
}

func (s *ImageService[Artifact]) cancelStaleRequests(keepID ImageRequestID, l logr.Logger) {
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

func (s *ImageService[Artifact]) pullFunc(req ImageRequestID, imageRef string, pullOpts ...remote.Option) func(context.Context) error {
	return func(ctx context.Context) error {
		img, err := image.Pull(ctx, imageRef, pullOpts...)
		result := ImageResult[Artifact]{Reference: imageRef, Error: err}
		if err == nil {
			artifact, err := s.processImageFunc(ctx, req, img)
			result = ImageResult[Artifact]{Reference: imageRef, Artifact: artifact, Error: err}
		}
		if err := s.cache.Set(req.Requester, result); err != nil {
			return errors.Join(result.Error, err)
		}
		return result.Error
	}
}

type cacheError struct {
	err error
}

func (e *cacheError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func IsCacheError(err error) bool {
	var ce *cacheError
	return errors.As(err, &ce)
}

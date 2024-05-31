/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/go-logr/logr"
	containerregistryv1 "github.com/google/go-containerregistry/pkg/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/finalizer"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/operator-framework/operator-registry/alpha/declcfg"

	utilv1alpha1 "github.com/joelanford/image-controller/api/v1alpha1"
	"github.com/joelanford/image-controller/internal/async"
	"github.com/joelanford/image-controller/internal/image"
)

// ImageReconciler reconciles a Image object
type ImageReconciler struct {
	client.Client
	Scheme     *k8sruntime.Scheme
	Finalizers finalizer.Finalizers

	ImageService       *async.ImageService[string]
	imageStoreRootPath string
	contentURLBase     url.URL
}

//+kubebuilder:rbac:groups=util.lanford.io,resources=images,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=util.lanford.io,resources=images/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=util.lanford.io,resources=images/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Image object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *ImageReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	l.Info("start reconcile")
	defer l.Info("finish reconcile")

	// Get the current desired image
	var img utilv1alpha1.Image
	if err := r.Get(ctx, req.NamespacedName, &img); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	res, err := r.Finalizers.Finalize(ctx, &img)
	if err != nil {
		return ctrl.Result{}, err
	}
	var finalizerUpdateErrs []error
	if res.StatusUpdated {
		finalizerUpdateErrs = append(finalizerUpdateErrs, r.Status().Update(ctx, &img))
	}
	if res.Updated {
		finalizerUpdateErrs = append(finalizerUpdateErrs, r.Update(ctx, &img))
	}
	if len(finalizerUpdateErrs) > 0 {
		return ctrl.Result{}, errors.Join(finalizerUpdateErrs...)
	}

	durationFor := func(d *metav1.Duration) *time.Duration {
		if d == nil {
			return nil
		}
		return &d.Duration
	}
	dur := durationFor(img.Spec.RefreshInterval)

	existingArtifacts, err := os.ReadDir(filepath.Join(r.catalogsRootPath(), req.Namespace, req.Name))
	if err != nil {
		l.Info("could not read image store directory to find existing artifacts", "error", err)
	}

	imageResult := r.ImageService.GetImage(ctx, async.ImageRequestID{req.NamespacedName, idOf(img.Spec)}, img.Spec.Reference, dur)
	if imageResult.Error != nil {
		reason := "ImagePullFailed"
		if async.IsCacheError(imageResult.Error) {
			reason = "ImageCacheLookupFailed"
		}
		meta.SetStatusCondition(&img.Status.Conditions, metav1.Condition{
			Type:               utilv1alpha1.ConditionTypeFailing,
			Status:             metav1.ConditionTrue,
			Reason:             reason,
			Message:            imageResult.Error.Error(),
			ObservedGeneration: img.Generation,
		})
		meta.SetStatusCondition(&img.Status.Conditions, metav1.Condition{
			Type:               utilv1alpha1.ConditionTypeProgressing,
			Status:             metav1.ConditionTrue,
			Reason:             "RetryingImagePull",
			Message:            "Retrying image pull",
			ObservedGeneration: img.Generation,
		})

		l.Info(reason, "error", imageResult.Error)
		return ctrl.Result{}, r.Status().Update(ctx, &img)
	}

	if imageResult.Artifact == "" {
		meta.RemoveStatusCondition(&img.Status.Conditions, utilv1alpha1.ConditionTypeFailing)
		meta.SetStatusCondition(&img.Status.Conditions, metav1.Condition{
			Type:               utilv1alpha1.ConditionTypeProgressing,
			Status:             metav1.ConditionTrue,
			Reason:             "PullingImage",
			Message:            "image pull is pending",
			ObservedGeneration: img.Generation,
		})
		l.Info("Pulling")
		return ctrl.Result{}, r.Status().Update(ctx, &img)
	}

	imgDigest := filepath.Base(imageResult.Artifact)

	meta.RemoveStatusCondition(&img.Status.Conditions, utilv1alpha1.ConditionTypeFailing)
	meta.RemoveStatusCondition(&img.Status.Conditions, utilv1alpha1.ConditionTypeProgressing)
	meta.SetStatusCondition(&img.Status.Conditions, metav1.Condition{
		Type:               utilv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             "ImageReady",
		Message:            "image is ready",
		ObservedGeneration: img.Generation,
	})
	img.Status.Digest = imgDigest
	img.Status.ContentURL = r.contentURLBase.String() + path.Join("/", strings.TrimPrefix(imageResult.Artifact, r.catalogsRootPath()))

	l.V(1).Info("image is ready")

	var requeueAfter time.Duration
	if dur != nil {
		expiration := imageResult.ModTime.Add(*dur)
		img.Status.ExpirationTime = &metav1.Time{Time: expiration}

		requeueAfter = expiration.Sub(time.Now())
		l.V(1).Info("requeue for refresh", "at", expiration, "in", requeueAfter)
	}
	defer func() {
		for _, artifact := range existingArtifacts {
			if artifact.Name() != imgDigest {
				os.Remove(filepath.Join(r.catalogsRootPath(), req.Namespace, req.Name, artifact.Name()))
			}
		}
	}()
	return ctrl.Result{RequeueAfter: requeueAfter}, r.Status().Update(ctx, &img)
}

func checkExtractPath(path string) (bool, error) {
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !stat.Mode().IsRegular() {
		return false, fmt.Errorf("extract path %q is not a regular file", path)
	}
	return true, nil
}

func (r *ImageReconciler) extractImage(ctx context.Context, img *image.Image, requestor types.NamespacedName) (string, error) {
	imgDigest, err := img.Digest()
	if err != nil {
		return "", err
	}

	extractPath := filepath.Join(r.catalogsRootPath(), requestor.Namespace, requestor.Name, imgDigest.String())
	tmpDirRoot := filepath.Join(r.imageStoreRootPath, "tmp")

	extractPathExists, err := checkExtractPath(extractPath)
	if err != nil {
		return "", err
	}
	if extractPathExists {
		return extractPath, nil
	}

	var fbcRootDir string
	mountDir, err := img.MountTemp(ctx, tmpDirRoot, image.WithSubPathGetter(func(img containerregistryv1.Image) (string, error) {
		cfgFile, err := img.ConfigFile()
		if err != nil {
			return "", err
		}
		fbcRootDir = cfgFile.Config.Labels["operators.operatorframework.io.index.configs.v1"]
		return fbcRootDir, nil
	}))
	if mountDir != "" {
		defer os.RemoveAll(mountDir)
	}
	if err != nil {
		return "", err
	}

	tmpFile, err := os.CreateTemp(tmpDirRoot, ".catalog-*.json")
	if err != nil {
		return "", err
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	mountedFBCRoot := filepath.Join(mountDir, fbcRootDir)
	fbcFS := os.DirFS(mountedFBCRoot)
	if err := declcfg.WalkMetasFS(ctx, fbcFS, func(path string, meta *declcfg.Meta, err error) error {
		if err != nil {
			return err
		}
		_, err = tmpFile.Write(meta.Blob)
		return err
	}); err != nil {
		return "", err
	}
	if err := tmpFile.Close(); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(extractPath), 0755); err != nil {
		return "", err
	}
	if err := os.Rename(tmpFile.Name(), extractPath); err != nil {
		return "", err
	}
	return extractPath, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ImageReconciler) SetupWithManager(mgr ctrl.Manager, imageStoreRootPath string, contentURLBase url.URL) error {
	r.imageStoreRootPath = imageStoreRootPath
	r.contentURLBase = contentURLBase
	imageSvcEvents := make(chan event.TypedGenericEvent[reconcile.Request])
	imageSvcSource := source.Channel[reconcile.Request](imageSvcEvents, handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, req reconcile.Request) []reconcile.Request {
		return []reconcile.Request{req}
	}))
	r.ImageService = async.NewImageService[string](runtime.NumCPU(), func(ctx context.Context, req async.ImageRequestID, img *image.Image) (string, error) {
		extractPath, err := r.extractImage(ctx, img, req.Requester)
		if err != nil {
			return "", err
		}
		return extractPath, nil
	})
	r.ImageService.RegisterCallback(func(ctx context.Context, req async.ImageRequestID, _ async.ImageResult[string]) error {
		select {
		case imageSvcEvents <- event.TypedGenericEvent[reconcile.Request]{Object: reconcile.Request{NamespacedName: req.Requester}}:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})

	r.Finalizers = finalizer.NewFinalizers()
	if err := r.Finalizers.Register("util.lanford.io/delete-image", finalizerFunc(func(ctx context.Context, obj client.Object) (finalizer.Result, error) {
		l := logr.FromContextOrDiscard(ctx)
		l.Info("deleting image from cache")

		if err := r.ImageService.DeleteImage(client.ObjectKeyFromObject(obj)); err != nil {
			return finalizer.Result{}, err
		}

		if err := recursiveDelete(r.catalogsRootPath(), filepath.Join(obj.GetNamespace(), obj.GetName())); err != nil {
			return finalizer.Result{}, err
		}
		return finalizer.Result{}, nil
	})); err != nil {
		return err
	}

	if err := mgr.Add(r.ImageService); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&utilv1alpha1.Image{}).
		WatchesRawSource(imageSvcSource).
		Complete(r)
}

func (r *ImageReconciler) catalogsRootPath() string {
	return filepath.Join(r.imageStoreRootPath, "content")
}

func idOf(imgSpec utilv1alpha1.ImageSpec) string {
	return fmt.Sprintf("%s", imgSpec.Reference)
}

func recursiveDelete(root, subpath string) error {
	root = filepath.Clean(root)
	deletePath := filepath.Join(root, subpath)
	for {
		if err := os.RemoveAll(deletePath); err != nil {
			return err
		}
		dirPath := filepath.Dir(deletePath)
		if dirPath == root {
			break
		}
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			break
		}
		deletePath = dirPath
	}
	return nil
}

type finalizerFunc func(context.Context, client.Object) (finalizer.Result, error)

func (f finalizerFunc) Finalize(ctx context.Context, obj client.Object) (finalizer.Result, error) {
	return f(ctx, obj)
}

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
	Scheme          *k8sruntime.Scheme
	CacheDir        string
	ExternalURLBase url.URL

	finalizers   finalizer.Finalizers
	imageService *async.ImageService[string]
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
	// Get the current desired image
	var img utilv1alpha1.Image
	if err := r.Get(ctx, req.NamespacedName, &img); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	res, err := r.finalizers.Finalize(ctx, &img)
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
	return r.reconcileImage(ctx, &img)
}

func (r *ImageReconciler) reconcileImage(ctx context.Context, img *utilv1alpha1.Image) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	l.Info("start reconcile")
	defer l.Info("finish reconcile")

	// Look for existing stored artifacts and make sure the existing status reflects the current state.
	// If it does not, something went wrong (maybe our storage was deleted?). In that case, we need to
	// clear the status to reflect the actual state of the backend storage (and the fact that it is no
	// longer available).
	existingArtifacts, err := os.ReadDir(r.catalogFilePath(client.ObjectKeyFromObject(img), ""))
	if err != nil && !os.IsNotExist(err) {
		l.Info("could not find existing artifacts", "error", err)
	}
	if !r.statusMatchesFilesystem(img.Status, existingArtifacts) {
		l.Info("status does not match filesystem, clearing status")
		img.Status.Digest = ""
		img.Status.ContentURL = ""
		img.Status.ExpirationTime = nil
		meta.RemoveStatusCondition(&img.Status.Conditions, utilv1alpha1.ConditionTypeReady)
	}

	// Next we get the image. This will either return a cached image or enqueue a background
	// task to fetch or refresh the image.
	dur := durationFor(img.Spec.RefreshInterval)
	imageResult := r.imageService.GetImage(ctx, requestID(img), img.Spec.Reference, dur)

	// The there was an error fetching the image, we need to update the status to reflect that.
	// Make sure _not_ to touch the Ready condition or other associated fields. We want to keep
	// the original image data around so the clients can still fetch it if they desire.
	//
	// Clients will be able to tell that the Ready condition is stale because its observedGeneration
	// will not match the current generation.
	if imageResult.Error != nil {
		reason := "ImageFetchFailed"
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
			Reason:             "RetryingImageFetch",
			Message:            "Retrying image fetch",
			ObservedGeneration: img.Generation,
		})

		l.Info(reason, "error", imageResult.Error)
		return ctrl.Result{}, r.Status().Update(ctx, img)
	}

	// If there was no error or artifact, that means the image is still being fetched.
	// Again, we don't want to touch the Ready condition or other associated fields
	// while the update is in progress.
	if imageResult.Artifact == "" {
		meta.RemoveStatusCondition(&img.Status.Conditions, utilv1alpha1.ConditionTypeFailing)
		meta.SetStatusCondition(&img.Status.Conditions, metav1.Condition{
			Type:               utilv1alpha1.ConditionTypeProgressing,
			Status:             metav1.ConditionTrue,
			Reason:             "FetchingImage",
			Message:            "image fetch is pending",
			ObservedGeneration: img.Generation,
		})
		l.Info("Pulling")
		return ctrl.Result{}, r.Status().Update(ctx, img)
	}

	// If we got here, we have a valid artifact. We can now update the status to reflect that.
	// We clear the Failing and Progressing conditions and set the Ready condition to True.
	meta.RemoveStatusCondition(&img.Status.Conditions, utilv1alpha1.ConditionTypeFailing)
	meta.RemoveStatusCondition(&img.Status.Conditions, utilv1alpha1.ConditionTypeProgressing)
	meta.SetStatusCondition(&img.Status.Conditions, metav1.Condition{
		Type:               utilv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             "ImageReady",
		Message:            "image is ready",
		ObservedGeneration: img.Generation,
	})
	imgDigest := filepath.Base(imageResult.Artifact)
	img.Status.Digest = imgDigest
	img.Status.ContentURL = r.catalogContentURL(client.ObjectKeyFromObject(img), imgDigest).String()
	l.V(1).Info("image is ready")
	defer func() {
		for _, artifact := range existingArtifacts {
			if artifact.Name() != imgDigest {
				os.Remove(r.catalogFilePath(client.ObjectKeyFromObject(img), artifact.Name()))
			}
		}
	}()

	var requeueAfter time.Duration
	if dur != nil {
		expiration := imageResult.ModTime.Add(*dur)
		img.Status.ExpirationTime = &metav1.Time{Time: expiration}

		requeueAfter = expiration.Sub(time.Now())
		l.V(1).Info("requeue for refresh", "at", expiration, "in", requeueAfter)
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, r.Status().Update(ctx, img)
}

// SetupWithManager sets up the controller with the Manager.
func (r *ImageReconciler) SetupWithManager(mgr ctrl.Manager, CacheDir string, ExternalURLBase url.URL) error {
	r.CacheDir = CacheDir
	r.ExternalURLBase = ExternalURLBase
	imageSvcEvents := make(chan event.TypedGenericEvent[reconcile.Request])
	imageSvcSource := source.Channel[reconcile.Request](imageSvcEvents, handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, req reconcile.Request) []reconcile.Request {
		return []reconcile.Request{req}
	}))
	r.imageService = async.NewImageService[string](runtime.NumCPU(), func(ctx context.Context, req async.ImageRequestID, img *image.Image) (string, error) {
		extractPath, err := r.extractImage(ctx, img, req.Requester)
		if err != nil {
			return "", err
		}
		return extractPath, nil
	})
	r.imageService.RegisterCallback(func(ctx context.Context, req async.ImageRequestID, _ async.ImageResult[string]) error {
		select {
		case imageSvcEvents <- event.TypedGenericEvent[reconcile.Request]{Object: reconcile.Request{NamespacedName: req.Requester}}:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})

	r.finalizers = finalizer.NewFinalizers()
	if err := r.finalizers.Register("util.lanford.io/delete-image", finalizerFunc(func(ctx context.Context, obj client.Object) (finalizer.Result, error) {
		l := logr.FromContextOrDiscard(ctx)
		l.Info("deleting image from cache")

		if err := r.imageService.DeleteImage(client.ObjectKeyFromObject(obj)); err != nil {
			return finalizer.Result{}, err
		}

		if err := recursiveDelete(r.catalogsRootPath(), catalogRelativePath(client.ObjectKeyFromObject(obj), "")); err != nil {
			return finalizer.Result{}, err
		}
		return finalizer.Result{}, nil
	})); err != nil {
		return err
	}

	if err := mgr.Add(r.imageService); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&utilv1alpha1.Image{}).
		WatchesRawSource(imageSvcSource).
		Complete(r)
}

func (r *ImageReconciler) extractImage(ctx context.Context, img *image.Image, requestor types.NamespacedName) (string, error) {
	imgDigest, err := img.Digest()
	if err != nil {
		return "", err
	}

	extractPath := r.catalogFilePath(requestor, imgDigest.String())
	tmpDirRoot := filepath.Join(r.CacheDir, "tmp")

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

func (r *ImageReconciler) statusMatchesFilesystem(status utilv1alpha1.ImageStatus, files []os.DirEntry) bool {
	for _, file := range files {
		if file.Name() == status.Digest {
			return true
		}
	}
	return status.Digest == "" && len(files) == 0
}

func (r *ImageReconciler) catalogsRootPath() string {
	return filepath.Join(r.CacheDir, "content")
}

func catalogRelativePath(key types.NamespacedName, digest string) string {
	return filepath.Join(key.Namespace, key.Name, digest)
}

func (r *ImageReconciler) catalogFilePath(key types.NamespacedName, digest string) string {
	return filepath.Join(r.catalogsRootPath(), catalogRelativePath(key, digest))
}

func (r *ImageReconciler) catalogContentURL(key types.NamespacedName, digest string) *url.URL {
	return r.ExternalURLBase.JoinPath(strings.ReplaceAll(catalogRelativePath(key, digest), string(filepath.Separator), "/"))
}

func requestID(img *utilv1alpha1.Image) async.ImageRequestID {
	return async.ImageRequestID{Requester: client.ObjectKeyFromObject(img), ImageID: fmt.Sprintf("%s", img.Spec.Reference)}
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

func durationFor(d *metav1.Duration) *time.Duration {
	if d == nil {
		return nil
	}
	return &d.Duration
}

type finalizerFunc func(context.Context, client.Object) (finalizer.Result, error)

func (f finalizerFunc) Finalize(ctx context.Context, obj client.Object) (finalizer.Result, error) {
	return f(ctx, obj)
}

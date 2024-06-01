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

package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	utilv1alpha1 "github.com/joelanford/image-controller/api/v1alpha1"
	"github.com/joelanford/image-controller/internal/certutil"
	"github.com/joelanford/image-controller/internal/controller"
	"github.com/joelanford/image-controller/internal/httputil"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(utilv1alpha1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func main() {
	var (
		metricsAddr               string
		probeAddr                 string
		certPath                  string
		keyPath                   string
		cacheDir                  string
		contentListenAddr         string
		rawExternalContentURLBase string
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8443", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.StringVar(&certPath, "cert", "", "Path to the certificate file")
	flag.StringVar(&keyPath, "key", "", "Path to the key file")
	flag.StringVar(&cacheDir, "cache-dir", "", "Path to the cache directory")
	flag.StringVar(&contentListenAddr, "content-bind-address", "localhost:9443", "The address the content server binds to")
	flag.StringVar(&rawExternalContentURLBase, "external-url-base", "https://localhost:9443/catalogs", "The base URL where clients can access image content.")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Standard controller-runtime setup of context and Kubernetes
	// REST configuration.
	ctx := ctrl.SetupSignalHandler()
	cfg, err := ctrl.GetConfig()
	if err != nil {
		setupLog.Error(err, "unable to get rest config")
		os.Exit(1)
	}

	// Setup TLS configuration
	tlsConfig := &tls.Config{}
	certWatcher, err := certutil.ConfigureTLSCertificateRunnable(tlsConfig, certPath, keyPath, setupLog)
	if err != nil {
		setupLog.Error(err, "unable to get or generate cert")
		os.Exit(1)
	}

	// Create the manager
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:    metricsAddr,
			FilterProvider: filters.WithAuthenticationAndAuthorization,
			SecureServing:  true,
			TLSOpts: []func(*tls.Config){
				func(metricsTLSConfig *tls.Config) {
					*metricsTLSConfig = *tlsConfig
				},
			},
		},
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Add the certificate watcher to the manager
	if err := mgr.Add(certWatcher); err != nil {
		setupLog.Error(err, "unable to add certificate watcher")
		os.Exit(1)
	}

	// Get the absolute path of the cache directory.
	if cacheDir == "" {
		setupLog.Error(fmt.Errorf("cache directory not set"), "--cache-dir flag is required")
		os.Exit(1)
	}
	cacheDir, err = filepath.Abs(cacheDir)
	if err != nil {
		setupLog.Error(err, "unable to get absolute path for CACHE_DIR")
		os.Exit(1)
	}

	// Parse the external URL base
	externalURLBase, err := url.Parse(rawExternalContentURLBase)
	if err != nil {
		setupLog.Error(err, "unable to parse external URL base")
		os.Exit(1)
	}

	// Set up the content server and add it to the manager
	contentServer, err := setupContentServer(
		contentListenAddr,
		externalURLBase.Path,
		filepath.Join(cacheDir, "content"),
		tlsConfig,
		cfg,
	)
	if err != nil {
		setupLog.Error(err, "unable to set up content server")
		os.Exit(1)
	}
	if err := mgr.Add(contentServer); err != nil {
		setupLog.Error(err, "unable to add content server")
		os.Exit(1)
	}

	if err = (&controller.ImageReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		CacheDir:        cacheDir,
		ExternalURLBase: *externalURLBase,
	}).SetupWithManager(mgr, cacheDir, *externalURLBase); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Image")
		os.Exit(1)
	}
	//+kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func setupContentServer(listenAddr, basePath, serveRoot string, tlsConfig *tls.Config, cfg *rest.Config) (*manager.Server, error) {
	contentHandler, err := httputil.ContentHandler(serveRoot, basePath, cfg, setupLog)
	if err != nil {
		return nil, fmt.Errorf("unable to create content handler: %w", err)
	}
	contentListener, err := tls.Listen("tcp", listenAddr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create content listener: %w", err)
	}
	contentServer := &manager.Server{
		Name:     "content",
		Listener: contentListener,
		Server: &http.Server{
			Handler:           contentHandler,
			ReadTimeout:       time.Second,
			ReadHeaderTimeout: time.Second,
			WriteTimeout:      time.Minute * 10,
			IdleTimeout:       time.Second,
		},
		OnlyServeWhenLeader: true,
	}
	return contentServer, nil
}

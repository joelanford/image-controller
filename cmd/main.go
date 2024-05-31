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
	"context"
	"crypto/tls"
	"flag"
	"net"
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
	certutil "k8s.io/client-go/util/cert"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	utilv1alpha1 "github.com/joelanford/image-controller/api/v1alpha1"
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
	var metricsAddr string
	var probeAddr string
	var certPath string
	var keyPath string
	var contentListenAddr string
	var rawExternalContentURLBase string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8443", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.StringVar(&certPath, "cert", "", "Path to the certificate file")
	flag.StringVar(&keyPath, "key", "", "Path to the key file")
	flag.StringVar(&contentListenAddr, "content-bind-address", "localhost:9443", "The address the content server binds to")
	flag.StringVar(&rawExternalContentURLBase, "external-url-base", "https://localhost:9443/catalogs", "The base URL where clients can access image content.")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	externalURLBase, err := url.Parse(rawExternalContentURLBase)
	if err != nil {
		setupLog.Error(err, "unable to parse external URL base")
		os.Exit(1)
	}

	var tlsOpts []func(*tls.Config)
	switch {
	case certPath == "" && keyPath == "":
		setupLog.Info("no certificate or key provided, generating self-signed certificate")
		tlsOpts = append(tlsOpts, func(cfg *tls.Config) {
			cert, key, err := certutil.GenerateSelfSignedCertKey("localhost", []net.IP{[]byte{127, 0, 0, 1}}, []string{externalURLBase.Hostname()})
			if err != nil {
				setupLog.Error(err, "unable to generate self-signed certificate")
				os.Exit(1)
			}
			keyPair, err := tls.X509KeyPair(cert, key)
			if err != nil {
				setupLog.Error(err, "unable to create key pair")
				os.Exit(1)
			}
			cfg.Certificates = []tls.Certificate{keyPair}
		})
	case certPath != "" && keyPath != "":
		setupLog.Info("using provided certificate and key")
		tlsOpts = append(tlsOpts, func(cfg *tls.Config) {
			keyPair, err := tls.LoadX509KeyPair(certPath, keyPath)
			if err != nil {
				setupLog.Error(err, "unable to load key pair")
				os.Exit(1)
			}
			cfg.Certificates = []tls.Certificate{keyPair}
		})
	default:
		setupLog.Error(nil, "certificate and key must both be provided together")
		os.Exit(1)
	}

	contentServerTLSConfig := &tls.Config{}
	for _, opt := range tlsOpts {
		opt(contentServerTLSConfig)
	}

	ctx := ctrl.SetupSignalHandler()

	cfg, err := ctrl.GetConfig()
	if err != nil {
		setupLog.Error(err, "unable to get rest config")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:    metricsAddr,
			FilterProvider: filters.WithAuthenticationAndAuthorization,
			SecureServing:  true,
			TLSOpts:        tlsOpts,
		},
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	cacheDir, ok := os.LookupEnv("CACHE_DIR")
	if !ok {
		setupLog.Error(err, "CACHE_DIR not set")
		os.Exit(1)
	}
	cacheDir, err = filepath.Abs(cacheDir)
	if err != nil {
		setupLog.Error(err, "unable to get absolute path for CACHE_DIR")
		os.Exit(1)
	}

	contentHandler, err := httputil.ContentHandler(filepath.Join(cacheDir, "serve"), cfg, setupLog)
	if err != nil {
		setupLog.Error(err, "unable to create content handler")
		os.Exit(1)
	}
	contentListener, err := tls.Listen("tcp", contentListenAddr, contentServerTLSConfig)
	if err != nil {
		setupLog.Error(err, "unable to create content listener")
		os.Exit(1)
	}
	contentServer := &manager.Server{
		Name:     "content",
		Listener: contentListener,
		Server: &http.Server{
			Handler:           contentHandler,
			TLSConfig:         contentServerTLSConfig,
			ReadTimeout:       time.Second,
			ReadHeaderTimeout: time.Second,
			WriteTimeout:      time.Minute * 10,
			IdleTimeout:       time.Second,
			BaseContext: func(listener net.Listener) context.Context {
				return ctx
			},
		},
		OnlyServeWhenLeader: true,
	}
	if err := mgr.Add(contentServer); err != nil {
		setupLog.Error(err, "unable to add content server")
		os.Exit(1)
	}

	if err = (&controller.ImageReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
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

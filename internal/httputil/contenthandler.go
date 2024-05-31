package httputil

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
)

func ContentHandler(contentRoot string, cfg *rest.Config, l logr.Logger) (http.Handler, error) {
	serveHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead:
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		path := r.URL.Path
		target, err := os.Readlink(filepath.Join(contentRoot, path))
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		etag := filepath.Base(target)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}

		w.Header().Set("ETag", etag)
		http.ServeFile(w, r, target)
	})
	httpClient, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create http client: %w", err)
	}

	filter, err := filters.WithAuthenticationAndAuthorization(cfg, httpClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create filter: %w", err)
	}
	authHandler, err := filter(l, serveHandler)
	if err != nil {
		return nil, fmt.Errorf("failed to create auth handler: %w", err)
	}
	return authHandler, nil
}

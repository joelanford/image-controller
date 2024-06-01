package certutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"time"

	"github.com/go-logr/logr"
	certutil "k8s.io/client-go/util/cert"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

func ConfigureTLSCertificateRunnable(tlsConfig *tls.Config, certPath, keyPath string, setupLog logr.Logger) (manager.Runnable, error) {
	switch {
	case certPath == "" && keyPath == "":
		genCert := func(l logr.Logger) (tls.Certificate, error) {
			cert, key, err := certutil.GenerateSelfSignedCertKey("localhost", []net.IP{[]byte{127, 0, 0, 1}}, nil)
			if err != nil {
				return tls.Certificate{}, err
			}
			kp, err := tls.X509KeyPair(cert, key)
			if err != nil {
				return tls.Certificate{}, err
			}
			c, err := x509.ParseCertificate(kp.Certificate[0])
			if err != nil {
				return tls.Certificate{}, err
			}
			l.Info("generated self-signed certificate", "notBefore", c.NotBefore, "notAfter", c.NotAfter)
			return kp, nil
		}

		keyPair, err := genCert(setupLog)
		if err != nil {
			return nil, err
		}
		tlsConfig.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return &keyPair, nil
		}
		return manager.RunnableFunc(func(ctx context.Context) error {
			l := logr.FromContextOrDiscard(ctx).WithName("certwatcher")

			ticker := time.NewTicker(1 * time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
					l.V(1).Info("checking certificate expiration")
					if keyPair.Leaf == nil || keyPair.Leaf.NotAfter.Before(time.Now().Add(24*time.Hour)) {
						keyPair, err = genCert(l)
						if err != nil {
							return err
						}
					}
				}
			}
		}), nil
	case certPath != "" && keyPath != "":
		certWatcher, err := certwatcher.New(certPath, keyPath)
		if err != nil {
			return nil, err
		}
		tlsConfig.GetCertificate = certWatcher.GetCertificate
		return certWatcher, nil
	default:
		return nil, fmt.Errorf("certificate and key must be provided together")
	}
}

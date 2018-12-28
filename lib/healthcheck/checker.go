package healthcheck

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/uber/kraken/utils/httputil"
)

// Checker runs a health check against an address.
type Checker interface {
	Check(ctx context.Context, addr string) error
}

// Default returns a Checker which makes a GET request against /health.
func Default(tls *tls.Config) Checker {
	return defaultChecker{tls}
}

type defaultChecker struct{ tls *tls.Config }

func (c defaultChecker) Check(ctx context.Context, addr string) error {
	_, err := httputil.Get(
		fmt.Sprintf("http://%s/health", addr),
		httputil.SendContext(ctx),
		httputil.SendTLS(c.tls))
	return err
}

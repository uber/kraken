package healthcheck

import (
	"context"
	"fmt"

	"code.uber.internal/infra/kraken/utils/httputil"
)

// Checker runs a health check against an address.
type Checker interface {
	Check(ctx context.Context, addr string) error
}

// Default returns a Checker which makes a GET request against /health.
func Default() Checker {
	return defaultChecker{}
}

type defaultChecker struct{}

func (c defaultChecker) Check(ctx context.Context, addr string) error {
	_, err := httputil.Get(
		fmt.Sprintf("http://%s/health", addr),
		httputil.SendContext(ctx))
	return err
}

package closers

import (
	"io"

	"github.com/uber/kraken/utils/log"
	"go.uber.org/zap"
)

// Close closes the closer. A message will be logged.
// The main reason for the helper existence is to have a utility for defer io.Closer() statements.
func Close(closer io.Closer) {
	if closer != nil {
		err := closer.Close()
		if err != nil {
			log.Desugar().Debug(
				"failed to close a closer",
				zap.Error(err),
				zap.Stack("stack"),
			)
		}
	}
}

package closers

import (
	"io"

	"go.uber.org/zap"

	"github.com/uber/kraken/utils/log"
)

// Close closes the closer. A message will be logged.
// The main reason for the helper existence is to have an utulity for defer io.Closer() statements.
func Close(closer io.Closer) {
	if closer != nil {
		err := closer.Close()
		if err != nil {
			// zap.Stack provides cleaner stack traces than debug.Stack(),
			// it cuts itself from the stack trace.
			log.Error(err, " ", zap.Stack("stack").String)
		}
	}
}

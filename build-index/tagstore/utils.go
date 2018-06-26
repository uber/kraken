package tagstore

import (
	"bytes"
	"os"

	"code.uber.internal/infra/kraken/core"
)

func writeTagToDisk(tag string, d core.Digest, fs FileStore) error {
	buf := bytes.NewBufferString(d.String())
	if err := fs.CreateCacheFile(tag, buf); err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}

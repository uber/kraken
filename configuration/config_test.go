package configuration

import (
	"testing"

	"code.uber.internal/go-common.git/x/log"

	"os"

	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	assert := require.New(t)
	cp := GetConfigFilePath("agent/test.yaml")
	c := NewConfigWithPath(cp)
	dir, _ := os.Getwd()
	log.Infof("%s", dir)
	assert.Equal(c.CacheDir, "/var/tmp/cache/")
	assert.Equal(c.DownloadDir, "/var/tmp/downloads/")
}

package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTorrenMetaConstructor(t *testing.T) {
	m := NewTorrentMeta()
	assert.Equal(t, m.GetSuffix(), "_torrentmeta")
	assert.True(t, m.Movable())
}

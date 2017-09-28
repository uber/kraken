package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStartedAtConstructor(t *testing.T) {
	h := NewStartedAt()
	assert.Equal(t, h.GetSuffix(), "_startedat")
	assert.False(t, h.Movable())
}

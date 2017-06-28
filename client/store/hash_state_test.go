package store

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashStateConstructor(t *testing.T) {
	h := NewHashState("sha256", "500")
	assert.Equal(t, h.GetSuffix(), "_hashstates/sha256/500")

	re := regexp.MustCompile("_hashstates/\\w+/\\w+$")
	assert.True(t, re.MatchString(h.GetSuffix()))

	r := strings.NewReplacer("_", "/")
	assert.Equal(t, r.Replace(h.GetSuffix()), "/hashstates/sha256/500")
}

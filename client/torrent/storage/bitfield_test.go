package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitfieldString(t *testing.T) {
	b := Bitfield{true, false, true, true}
	expect := "1011"
	assert.Equal(t, expect, b.String())
}

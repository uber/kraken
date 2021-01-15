package backend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegisterAndGetFactory(t *testing.T) {
	t.Run("round-trip", func(t *testing.T) {
		name := "dummy"
		factory := &dummyFactory{}

		Register(name, factory)
		roundTrip, err := GetFactory(name)

		assert.NoError(t, err)
		assert.Equal(t, factory, roundTrip)
	})

	t.Run("GetFactory errors out on missing factory", func(t *testing.T) {
		_, err := GetFactory("i_dont_exist")

		assert.Error(t, err)
	})
}

type dummyFactory struct{}

func (f *dummyFactory) Create(config interface{}, authConfig interface{}) (Client, error) {
	return nil, nil
}

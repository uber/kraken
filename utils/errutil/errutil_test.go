package errutil

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiError(t *testing.T) {
	a := errors.New("a")
	b := errors.New("b")
	c := errors.New("c")

	tests := []struct {
		description string
		errs        []error
		result      string
	}{
		{"empty", nil, ""},
		{"one error", []error{a}, "a"},
		{"many errors", []error{a, b, c}, "a, b, c"},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			require.Equal(t, test.result, MultiError(test.errs).Error())
		})
	}
}

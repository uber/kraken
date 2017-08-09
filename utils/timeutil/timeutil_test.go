package timeutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMostRecent(t *testing.T) {
	a := time.Now()
	b := a.Add(time.Second)
	c := b.Add(time.Second)

	for _, test := range []struct {
		description string
		ts          []time.Time
		expected    time.Time
	}{
		{"empty list", []time.Time{}, time.Time{}},
		{"single item", []time.Time{a}, a},
		{"ascending order", []time.Time{a, b, c}, c},
		{"descending order", []time.Time{c, b, a}, c},
	} {
		t.Run(test.description, func(t *testing.T) {
			actual := MostRecent(test.ts...)
			require.Equal(t, test.expected, actual)
		})
	}
}

package tagreplication

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemotesMatch(t *testing.T) {
	require := require.New(t)

	remotes, err := RemotesConfig{
		"a": []string{"foo/.*", "bar/.*"},
		"b": []string{"foo/.*"},
	}.Build()
	require.NoError(err)

	for tag, expected := range map[string][]string{
		"foo/123": {"a", "b"},
		"bar/abc": {"a"},
		"baz/456": nil,
		"xxx":     nil,
	} {
		require.ElementsMatch(expected, remotes.Match(tag), "Tag: %s", tag)
	}
}

func TestRemotesValid(t *testing.T) {
	require := require.New(t)

	remotes, err := RemotesConfig{
		"a": []string{"foo/.*"},
		"b": []string{"foo/.*"},
		"c": []string{"foo/.*"},
		"d": []string{"bar/.*"},
	}.Build()
	require.NoError(err)

	tests := []struct {
		tag      string
		addr     string
		expected bool
	}{
		{"foo/123", "a", true},
		{"foo/123", "b", true},
		{"foo/123", "c", true},
		{"foo/123", "d", false},
		{"bar/123", "d", true},
		{"bar/123", "c", false},
		{"bar/123", "x", false},
	}
	for _, test := range tests {
		require.Equal(
			test.expected, remotes.Valid(test.tag, test.addr),
			"Tag: %s, Addr: %s", test.tag, test.addr)
	}
}

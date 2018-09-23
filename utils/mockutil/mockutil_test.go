package mockutil

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchReader(t *testing.T) {
	tests := []struct {
		expected string
		actual   string
		matches  bool
	}{
		{"abcd", "abcd", true},
		{"abcd", "wxyz", false},
		{"", "", true},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%q==%q", test.expected, test.actual), func(t *testing.T) {
			require := require.New(t)

			f, err := ioutil.TempFile("", "")
			require.NoError(err)
			defer os.Remove(f.Name())

			_, err = f.Write([]byte(test.actual))
			require.NoError(err)

			// Reset file.
			_, err = f.Seek(0, 0)
			require.NoError(err)

			m := MatchReader([]byte(test.expected))

			require.Equal(test.matches, m.Matches(f))
		})
	}
}

func TestMatchWriter(t *testing.T) {
	require := require.New(t)

	f, err := ioutil.TempFile("", "")
	require.NoError(err)
	defer os.Remove(f.Name())

	b := []byte("some text")

	m := MatchWriter(b)

	require.True(m.Matches(f))

	// Reset file.
	_, err = f.Seek(0, 0)
	require.NoError(err)

	// WriterMatcher should write to the file.
	result, err := ioutil.ReadAll(f)
	require.Equal(string(b), string(result))
}

func TestMatchWriterAt(t *testing.T) {
	require := require.New(t)

	f, err := ioutil.TempFile("", "")
	require.NoError(err)
	defer os.Remove(f.Name())

	b := []byte("some text")

	m := MatchWriterAt(b)

	require.True(m.Matches(f))

	// Reset file.
	_, err = f.Seek(0, 0)
	require.NoError(err)

	// WriterAtMatcher should write to the file.
	result, err := ioutil.ReadAll(f)
	require.Equal(string(b), string(result))
}

func TestMatchRegex(t *testing.T) {
	tests := []struct {
		expected string
		actual   string
		matches  bool
	}{
		{"foo/.+", "foo/bar", true},
		{"foo/.+", "foo/", false},
		{"foo", "foo", true},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%q==%q", test.expected, test.actual), func(t *testing.T) {
			m := MatchRegex(test.expected)
			require.Equal(t, test.matches, m.Matches(test.actual))
		})
	}
}

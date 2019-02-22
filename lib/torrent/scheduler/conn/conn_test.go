package conn

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnClose(t *testing.T) {
	require := require.New(t)

	c, cleanup := Fixture()
	defer cleanup()

	require.False(c.IsClosed())

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Close()
		}()
	}
	wg.Wait()

	require.True(c.IsClosed())
}

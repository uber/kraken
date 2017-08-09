package timeutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const d = 50 * time.Millisecond

const delta = 10 * time.Millisecond

func TestTimerFiresAfterStart(t *testing.T) {
	require := require.New(t)
	timer := NewTimer(d)

	require.True(timer.Start())

	select {
	case <-timer.C:
	case <-time.After(d + delta):
		t.Fatal("Timer did not fire within expected duration")
	}
}

func TestTimerSecondStartIsNoop(t *testing.T) {
	require := require.New(t)
	timer := NewTimer(d)

	require.True(timer.Start())
	require.False(timer.Start())

	select {
	case <-timer.C:
	case <-time.After(d + delta):
		t.Fatal("Timer did not fire within duration of initial start")
	}
}

func TestTimerCancelPreventsFiring(t *testing.T) {
	require := require.New(t)
	timer := NewTimer(d)

	require.True(timer.Start())
	require.True(timer.Cancel())

	select {
	case <-timer.C:
		t.Fatal("Timer fired after Cancel was called")
	case <-time.After(d + delta):
	}
}

func TestTimerCanStillStartAfterCancel(t *testing.T) {
	require := require.New(t)
	timer := NewTimer(d)

	require.True(timer.Start())
	require.True(timer.Cancel())
	require.True(timer.Start())

	select {
	case <-timer.C:
	case <-time.After(d + delta):
		t.Fatal("Timer did not fire within expected duration")
	}
}

func TestTimerCancelBeforeStartIsNoop(t *testing.T) {
	require := require.New(t)
	timer := NewTimer(d)

	require.False(timer.Cancel())
	require.True(timer.Start())

	select {
	case <-timer.C:
	case <-time.After(d + delta):
		t.Fatal("Timer did not fire within expected duration")
	}
}

func TestTimerCancelAfterFiringIsNoop(t *testing.T) {
	require := require.New(t)
	timer := NewTimer(d)

	require.True(timer.Start())

	select {
	case <-timer.C:
	case <-time.After(d + delta):
		t.Fatal("Timer did not fire within expected duration")
	}

	require.False(timer.Cancel())
}

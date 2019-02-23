// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

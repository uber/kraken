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

package log

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// obs wires the package global to an in-memory observer and returns a restore func.
func obs(t *testing.T) (*observer.ObservedLogs, func()) {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	SetGlobalLogger(zap.New(core).Sugar())
	return logs, func() { SetGlobalLogger(zap.NewNop().Sugar()) }
}

// --- InfoS / ErrorS ---

func TestInfoS(t *testing.T) {
	logs, restore := obs(t)
	defer restore()

	InfoS("pod updated", "pod", "kubedns", "namespace", "kube-system")

	assert.Equal(t, 1, logs.Len())
	e := logs.All()[0]
	assert.Equal(t, "pod updated", e.Message)
	assert.Equal(t, "kubedns", e.ContextMap()["pod"])
	assert.Equal(t, "kube-system", e.ContextMap()["namespace"])
}

func TestErrorS(t *testing.T) {
	logs, restore := obs(t)
	defer restore()

	ErrorS(errors.New("refused"), "dial failed", "host", "redis:6379")

	e := logs.All()[0]
	assert.Equal(t, zapcore.ErrorLevel, e.Level)
	assert.Equal(t, "dial failed", e.Message)
	assert.Equal(t, "redis:6379", e.ContextMap()["host"])
	_, hasErr := e.ContextMap()["error"]
	assert.True(t, hasErr, "expected 'error' field")
}

// --- WithValues / WithName ---

func TestWithValues(t *testing.T) {
	logs, restore := obs(t)
	defer restore()

	WithValues("traceID", "t1").Info("hit")

	assert.Equal(t, "t1", logs.All()[0].ContextMap()["traceID"])
}

func TestWithName(t *testing.T) {
	logs, restore := obs(t)
	defer restore()

	WithName("scheduler").Info("tick")

	assert.Equal(t, "scheduler", logs.All()[0].LoggerName)
}

// --- V / Verbose ---

func TestV_DisabledByDefault(t *testing.T) {
	logs, restore := obs(t)
	defer restore()

	SetVerbosity(0)
	V(1).Info("should be silent")

	assert.Equal(t, 0, logs.Len())
}

func TestV_Enabled(t *testing.T) {
	logs, restore := obs(t)
	defer restore()

	SetVerbosity(3)
	defer SetVerbosity(0)

	V(2).Info("within threshold")
	V(3).InfoS("at threshold", "k", "v")
	V(4).Info("above threshold – silent")

	assert.Equal(t, 2, logs.Len())
}

func TestV_Enabled_Check(t *testing.T) {
	SetVerbosity(2)
	defer SetVerbosity(0)

	assert.True(t, V(1).Enabled())
	assert.True(t, V(2).Enabled())
	assert.False(t, V(3).Enabled())
}

// --- FromContext / NewContext ---

func TestFromContext_FallsBackToGlobal(t *testing.T) {
	ctx := context.Background()
	assert.NotNil(t, FromContext(ctx), "should return global, not nil")
}

func TestNewContext_FromContext(t *testing.T) {
	logs, restore := obs(t)
	defer restore()

	ctx := NewContext(context.Background(), WithValues("requestID", "req-1"))
	FromContext(ctx).Infow("handled")

	assert.Equal(t, "req-1", logs.All()[0].ContextMap()["requestID"])
}

func TestFromContext_InheritedByChildContext(t *testing.T) {
	logs, restore := obs(t)
	defer restore()

	parent := NewContext(context.Background(), WithValues("traceID", "t1"))
	child, cancel := context.WithCancel(parent)
	defer cancel()

	FromContext(child).Info("child log")

	assert.Equal(t, "t1", logs.All()[0].ContextMap()["traceID"])
}

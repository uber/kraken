// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestConfigureLoggerAppliesOption(t *testing.T) {
	prevDefault := _default
	defer func() { _default = prevDefault }()

	var received *zap.Logger
	override := zap.NewNop()
	opt := func(l *zap.Logger) *zap.Logger {
		received = l
		return override
	}

	logger := ConfigureLogger(zap.NewProductionConfig(), opt)

	assert.NotNil(t, received)
	assert.Equal(t, override.Sugar(), logger)
}

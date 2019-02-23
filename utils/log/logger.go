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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config defines Logger configuration.
type Config struct {
	Level       zapcore.Level       `yaml:"level"`
	Disable     bool                `yaml:"disable"`
	ServiceName string              `yaml:"service_name"`
	Path        string              `yaml:"path"`
	Encoding    string              `yaml:"encoding"`
	EncodeTime  zapcore.TimeEncoder `yaml:"timeEncoder" json:"-"`
}

func (c Config) applyDefaults() Config {
	if c.Path == "" {
		c.Path = "stderr"
	}
	if c.Encoding == "" {
		c.Encoding = "console"
	}
	return c
}

// New creates a logger that is not default.
func New(c Config, fields map[string]interface{}) (*zap.Logger, error) {
	c = c.applyDefaults()
	if c.Disable {
		return zap.NewNop(), nil
	}
	if fields == nil {
		fields = map[string]interface{}{}
	}
	if c.ServiceName != "" {
		fields["service_name"] = c.ServiceName
	}

	return zap.Config{
		Level: zap.NewAtomicLevelAt(c.Level),
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding: c.Encoding,
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "message",
			NameKey:        "logger_name",
			LevelKey:       "level",
			TimeKey:        "ts",
			CallerKey:      "caller",
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     c.EncodeTime,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		DisableStacktrace: true,
		OutputPaths:       []string{c.Path},
		InitialFields:     fields,
	}.Build()
}

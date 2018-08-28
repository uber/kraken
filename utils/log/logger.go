package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config defines Logger configuration.
type Config struct {
	Disable     bool   `yaml:"disable"`
	ServiceName string `yaml:"service_name"`
	Path        string `yaml:"path"`
	Encoding    string `yaml:"encoding"`
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
		Level: zap.NewAtomicLevel(),
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
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		DisableStacktrace: true,
		OutputPaths:       []string{c.Path},
		InitialFields:     fields,
	}.Build()
}

package torrentlog

import (
	"fmt"
	"os"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config defines Logger configuration.
type Config struct {
	Disable     bool   `yaml:"disable"`
	ServiceName string `yaml:"service_name"`
	LogPath     string `yaml:"log_path"`
}

func (c Config) build() (*zap.Logger, error) {
	if c.Disable {
		log.Warn("Torrent log disabled")
		return zap.NewNop(), nil
	}
	return zap.Config{
		Level: zap.NewAtomicLevel(),
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding: "json",
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "message",
			NameKey:        "logger_name",
			LevelKey:       "level",
			TimeKey:        "ts",
			CallerKey:      "caller",
			StacktraceKey:  "stack",
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     zapcore.EpochTimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths: []string{c.LogPath},
		InitialFields: map[string]interface{}{
			"service_name": c.ServiceName,
		},
	}.Build()
}

// Logger wraps structured log entries for important torrent events. These events
// are intended to be consumed at the cluster level via Elk, and are distinct from
// the verbose stdout logs of the agent. In particular, Logger bridges host-agnostic
// metrics to an individual hostnames.
//
// For example, if there is a spike in download times, an engineer can cross-reference
// the spike with the torrent logs in Elk and zero-in on a single host. From there,
// the engineer can inspect the stdout logs of the host for more detailed information
// as to why the download took so long.
type Logger struct {
	zap *zap.Logger
}

// New creates a new Logger.
func New(config Config, pctx core.PeerContext) (*Logger, error) {
	logger, err := config.build()
	if err != nil {
		return nil, fmt.Errorf("config: %s", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("hostname: %s", err)
	}
	return &Logger{logger.With(
		zap.String("hostname", hostname),
		zap.String("zone", pctx.Zone),
		zap.String("peer_id", pctx.PeerID.String()),
	)}, nil
}

// DownloadSuccess logs a successful download.
func (l *Logger) DownloadSuccess(namespace, name string, size int64, downloadTime time.Duration) {
	l.zap.Info(
		"Download success",
		zap.String("namespace", namespace),
		zap.String("name", name),
		zap.Int64("size", size),
		zap.Duration("download_time", downloadTime))
}

// DownloadFailure logs a failed download.
func (l *Logger) DownloadFailure(namespace, name string, size int64, err error) {
	l.zap.Info(
		"Download failure",
		zap.String("namespace", namespace),
		zap.String("name", name),
		zap.Int64("size", size),
		zap.Error(err))
}

// Sync flushes the log.
func (l *Logger) Sync() {
	l.zap.Sync()
}

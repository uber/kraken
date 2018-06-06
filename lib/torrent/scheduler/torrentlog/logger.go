package torrentlog

import (
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	errEmptyReceivedPieces    = errors.New("empty received piece counts")
	errNegativeReceivedPieces = errors.New("negative value in received piece counts")
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
// are intended to be consumed at the cluster level via ELK, and are distinct from
// the verbose stdout logs of the agent. In particular, Logger bridges host-agnostic
// metrics to individual hostnames.
//
// For example, if there is a spike in download times, an engineer can cross-reference
// the spike with the torrent logs in ELK and zero-in on a single host. From there,
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
		zap.String("cluster", pctx.Cluster),
		zap.String("peer_id", pctx.PeerID.String()),
	)}, nil
}

// NewNopLogger returns a Logger containing a no-op zap logger for testing purposes.
func NewNopLogger() *Logger {
	return &Logger{zap.NewNop()}
}

// OutgoingConnectionAccept logs an accepted outgoing connection.
func (l *Logger) OutgoingConnectionAccept(
	name string,
	infoHash core.InfoHash,
	remotePeerID core.PeerID) {

	l.zap.Info(
		"Outgoing connection accept",
		zap.String("name", name),
		zap.String("info_hash", infoHash.String()),
		zap.String("remote_peer_id", remotePeerID.String()))
}

// OutgoingConnectionReject logs a rejected outgoing connection.
func (l *Logger) OutgoingConnectionReject(name string,
	infoHash core.InfoHash,
	remotePeerID core.PeerID,
	err error) {

	l.zap.Info(
		"Outgoing connection reject",
		zap.String("name", name),
		zap.String("info_hash", infoHash.String()),
		zap.String("remote_peer_id", remotePeerID.String()),
		zap.Error(err))
}

// IncomingConnectionAccept logs an accepted incoming connection.
func (l *Logger) IncomingConnectionAccept(
	name string,
	infoHash core.InfoHash,
	remotePeerID core.PeerID) {

	l.zap.Info(
		"Incoming connection accept",
		zap.String("name", name),
		zap.String("info_hash", infoHash.String()),
		zap.String("remote_peer_id", remotePeerID.String()))
}

// IncomingConnectionReject logs a rejected incoming connection.
func (l *Logger) IncomingConnectionReject(
	name string,
	infoHash core.InfoHash,
	remotePeerID core.PeerID,
	err error) {

	l.zap.Info(
		"Incoming connection reject",
		zap.String("name", name),
		zap.String("info_hash", infoHash.String()),
		zap.String("remote_peer_id", remotePeerID.String()),
		zap.Error(err))
}

// SeedTimeout logs a seeding torrent being torn down due to timeout.
func (l *Logger) SeedTimeout(name string, infoHash core.InfoHash) {
	l.zap.Info(
		"Seed timeout",
		zap.String("name", name),
		zap.String("info_hash", infoHash.String()))
}

// LeechTimeout logs a leeching torrent being torn down due to timeout.
func (l *Logger) LeechTimeout(name string, infoHash core.InfoHash) {
	l.zap.Info(
		"Leech timeout",
		zap.String("name", name),
		zap.String("info_hash", infoHash.String()))
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

// ReceivedPiecesSummary logs a summary of pieces received from peers when a torrent completes.
func (l *Logger) ReceivedPiecesSummary(
	name string,
	infoHash core.InfoHash,
	receivedPieces []int) error {

	summary, err := newReceivedPiecesSummary(receivedPieces)
	if err != nil {
		return err
	}

	l.zap.Info(
		"Received pieces summary",
		zap.String("name", name),
		zap.String("info_hash", infoHash.String()),
		zap.Object("pieces_stats", summary))

	return nil
}

// Sync flushes the log.
func (l *Logger) Sync() {
	l.zap.Sync()
}

// receivedPiecesSummary holds summary statistics about pieces from peers.
type receivedPiecesSummary struct {
	numZero int
	min     int
	max     int
	mean    float64
	stdDev  float64 // sample standard deviation
}

// newReceivedPiecesSummary calculates and returns summary statistics about pieces received from peers.
func newReceivedPiecesSummary(receivedPieces []int) (*receivedPiecesSummary, error) {
	if len(receivedPieces) == 0 {
		return nil, errEmptyReceivedPieces
	}

	sum := 0
	summary := receivedPiecesSummary{min: math.MaxInt32}
	for _, count := range receivedPieces {
		if count < 0 {
			return nil, errNegativeReceivedPieces
		}

		sum += count
		if count > summary.max {
			summary.max = count
		}
		if count < summary.min {
			summary.min = count
		}
		if count == 0 {
			summary.numZero++
		}
	}
	summary.mean = float64(sum) / float64(len(receivedPieces))

	sumSqDiff := 0.0
	for _, count := range receivedPieces {
		sumSqDiff += math.Pow(float64(count)-summary.mean, 2)
	}
	summary.stdDev = math.Sqrt(sumSqDiff / float64(len(receivedPieces)-1))

	return &summary, nil
}

func (s *receivedPiecesSummary) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddInt("num_zero_piece_peers", s.numZero)
	enc.AddInt("min_pieces_from_peer", s.min)
	enc.AddInt("max_pieces_from_peer", s.max)
	enc.AddFloat32("mean_pieces_from_peer", float32(s.mean))
	enc.AddFloat32("stddev_pieces_from_peer", float32(s.stdDev))

	return nil
}

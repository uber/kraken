package torrentlog

import (
	"errors"
	"fmt"
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

// SeederSummaries logs a summary of the pieces requested and received from peers for a torrent.
func (l *Logger) SeederSummaries(
	name string,
	infoHash core.InfoHash,
	summaries SeederSummaries) error {

	l.zap.Info(
		"Seeder summaries",
		zap.String("name", name),
		zap.String("info_hash", infoHash.String()),
		zap.Array("seeder_summaries", summaries))
	return nil
}

// LeecherSummaries logs a summary of the pieces requested by and sent to peers for a torrent.
func (l *Logger) LeecherSummaries(
	name string,
	infoHash core.InfoHash,
	summaries LeecherSummaries) error {

	l.zap.Info(
		"Leecher summaries",
		zap.String("name", name),
		zap.String("info_hash", infoHash.String()),
		zap.Array("leecher_summaries", summaries))
	return nil
}

// Sync flushes the log.
func (l *Logger) Sync() {
	l.zap.Sync()
}

// SeederSummary contains information about piece requests to and pieces received from a peer.
type SeederSummary struct {
	PeerID         core.PeerID
	RequestsSent   int
	PiecesReceived int
}

// MarshalLogObject marshals a SeederSummary for logging.
func (s SeederSummary) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("peer_id", s.PeerID.String())
	enc.AddInt("requests_sent", s.RequestsSent)
	enc.AddInt("pieces_received", s.PiecesReceived)
	return nil
}

// SeederSummaries represents a slice of type SeederSummary
// that can be marshalled for logging.
type SeederSummaries []SeederSummary

// MarshalLogArray marshals a SeederSummaries slice for logging.
func (ss SeederSummaries) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, summary := range ss {
		enc.AppendObject(summary)
	}
	return nil
}

// LeecherSummary contains information about piece requests from and pieces sent to a peer.
type LeecherSummary struct {
	PeerID           core.PeerID
	RequestsReceived int
	PiecesSent       int
}

// MarshalLogObject marshals a LeecherSummary for logging.
func (s LeecherSummary) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("peer_id", s.PeerID.String())
	enc.AddInt("requests_received", s.RequestsReceived)
	enc.AddInt("pieces_sent", s.PiecesSent)
	return nil
}

// LeecherSummaries represents a slice of type LeecherSummary
// that can be marshalled for logging.
type LeecherSummaries []LeecherSummary

// MarshalLogArray marshals a LeecherSummaries slice for logging.
func (ls LeecherSummaries) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, summary := range ls {
		enc.AppendObject(summary)
	}
	return nil
}

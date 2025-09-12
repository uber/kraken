package closers

import (
	"bytes"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	mocks_io "github.com/uber/kraken/mocks/io"
	"github.com/uber/kraken/utils/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestClose_NilCloser(t *testing.T) {
	// Should not panic or log anything
	Close(nil)
}

func TestClose_Success(t *testing.T) {
	mockCloser := mocks_io.NewMockCloser(gomock.NewController(t))
	mockCloser.EXPECT().Close().Return(nil)

	Close(mockCloser)
}

func TestClose_Error(t *testing.T) {
	mockCloser := mocks_io.NewMockCloser(gomock.NewController(t))
	mockCloser.EXPECT().Close().Return(errors.New("close error"))

	Close(mockCloser)
}

func TestClose_LogsError(t *testing.T) {
	defaultLogger := log.Default()
	t.Cleanup(func() {
		// Restore the original global logger after the test
		log.SetGlobalLogger(defaultLogger)
	})

	var buf bytes.Buffer
	logger := zap.New(
		zapcore.NewCore(
			zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig()),
			zapcore.AddSync(&buf),
			zapcore.ErrorLevel,
		),
	).Sugar()
	log.SetGlobalLogger(logger)

	mockCloser := mocks_io.NewMockCloser(gomock.NewController(t))
	mockCloser.EXPECT().Close().Return(errors.New("custom error for the test"))

	Close(mockCloser)

	require.Contains(t, buf.String(), "custom error for the test")
}

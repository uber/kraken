package cmd

import (
	"flag"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

func TestParseFlags(t *testing.T) {
	// Save original args and flagset
	oldArgs := os.Args
	oldCommandLine := flag.CommandLine
	defer func() {
		os.Args = oldArgs
		flag.CommandLine = oldCommandLine
	}()

	// Reset flags
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	// Set up test args
	os.Args = []string{
		"cmd",
		"-peer-ip=1.2.3.4",
		"-peer-port=1000",
		"-agent-server-port=2000",
		"-agent-registry-port=3000",
		"-config=config.yaml",
		"-zone=test-zone",
		"-cluster=test-cluster",
		"-secrets=secrets.yaml",
	}

	flags := ParseFlags()

	assert.Equal(t, "1.2.3.4", flags.PeerIP)
	assert.Equal(t, 1000, flags.PeerPort)
	assert.Equal(t, 2000, flags.AgentServerPort)
	assert.Equal(t, 3000, flags.AgentRegistryPort)
	assert.Equal(t, "config.yaml", flags.ConfigFile)
	assert.Equal(t, "test-zone", flags.Zone)
	assert.Equal(t, "test-cluster", flags.KrakenCluster)
	assert.Equal(t, "secrets.yaml", flags.SecretsFile)
}

func TestWithConfigOption(t *testing.T) {
	var o options
	c := Config{RegistryBackup: "test"}
	WithConfig(c)(&o)
	assert.Equal(t, "test", o.config.RegistryBackup)
}

func TestWithMetricsOption(t *testing.T) {
	var o options
	s := tally.NoopScope
	WithMetrics(s)(&o)
	assert.Equal(t, s, o.metrics)
}

func TestWithLoggerOption(t *testing.T) {
	var o options
	l := zap.NewNop()
	WithLogger(l)(&o)
	assert.Equal(t, l, o.logger)
}

func TestWithEffectOption(t *testing.T) {
	var o options
	called := false
	f := func() { called = true }
	WithEffect(f)(&o)
	assert.NotNil(t, o.effect)
	o.effect()
	assert.True(t, called)
}

func TestRunValidation(t *testing.T) {
	tests := []struct {
		desc  string
		flags Flags
		panic string
	}{
		{
			desc:  "missing peer port",
			flags: Flags{AgentServerPort: 1, AgentRegistryPort: 1},
			panic: "must specify non-zero peer port",
		},
		{
			desc:  "missing agent server port",
			flags: Flags{PeerPort: 1, AgentRegistryPort: 1},
			panic: "must specify non-zero agent server port",
		},
		{
			desc:  "missing agent registry port",
			flags: Flags{PeerPort: 1, AgentServerPort: 1},
			panic: "must specify non-zero agent registry port",
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			assert.PanicsWithValue(t, test.panic, func() {
				Run(&test.flags)
			})
		})
	}
}
func TestHeartbeatWithTicker(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	mockClock := clock.NewMock()
	mockTicker := mockClock.Ticker(100 * time.Millisecond)
	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		heartbeatWithTicker(scope, clockTicker{ticker: mockTicker}, done)
	}()

	for i := 0; i < 3; i++ {
		mockClock.Add(100 * time.Millisecond)
	}

	require.Eventually(t, func() bool {
		snapshot := scope.Snapshot()
		for _, counter := range snapshot.Counters() {
			if counter.Name() == "heartbeat" && counter.Value() >= 3 {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)

	close(done)
	wg.Wait()
}

type clockTicker struct {
	ticker *clock.Ticker
}

func (t clockTicker) Chan() <-chan time.Time {
	return t.ticker.C
}

func (t clockTicker) Stop() {
	t.ticker.Stop()
}

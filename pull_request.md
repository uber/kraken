## What?

- Refactor `heartbeat` to use the new helper function `heartbeatWithTicker`, allowing for ticker injection and graceful shutdown
- Introduce `heartbeatWithTicker` to support test-friendly control over ticker and lifecycle
- Add unit tests in `agent/cmd/cmd_test.go` covering flag parsing, options, validation, and new heartbeat logic

## Why?

- `heartbeat` previously ran forever with a real-time ticker, making it untestable
- The new helper enables dependency injection, simplifying unit testing and improving maintainability
- Test coverage increases for the `agent/cmd` package, ensuring core logic is verified

## How?

- `heartbeat` now creates a ticker and delegates to `heartbeatWithTicker`
- Added a `done` channel to `heartbeatWithTicker` to cleanly stop the loop during tests
- Added `cmd_test.go` with tests for `ParseFlags`, option helpers, validation, and `heartbeatWithTicker`

## Test coverage

```
go test -v -coverprofile=coverage.out ./agent/cmd/...
go tool cover -func=coverage.out
```
	github.com/uber/kraken/agent/cmd                coverage: 23.8% of statements
	github.com/uber/kraken/agent/cmd/cmd.go:55:     ParseFlags      100.0%
	github.com/uber/kraken/agent/cmd/cmd.go:89:     WithConfig      100.0%
	github.com/uber/kraken/agent/cmd/cmd.go:94:     WithMetrics     100.0%
	github.com/uber/kraken/agent/cmd/cmd.go:99:     WithLogger      100.0%
	github.com/uber/kraken/agent/cmd/cmd.go:104:    WithEffect      100.0%
	github.com/uber/kraken/agent/cmd/cmd.go:109:    Run             7.2%
	github.com/uber/kraken/agent/cmd/cmd.go:257:    heartbeat       0.0%
```

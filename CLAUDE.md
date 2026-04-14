# Kraken Development Guide for Claude

## Project Overview

Kraken is a P2P-powered Docker registry designed for scalability and availability in hybrid cloud environments. It distributes Docker images using a BitTorrent-inspired protocol with a tracker-coordinated peer network. Built in Go, it has been in production at Uber since 2018, distributing over 1 million blobs per day.

**Key technologies**: Go 1.24+, Docker, Containerd, P2P networking, pluggable storage backends (S3, GCS, ECR, HDFS)

## Quick Start

### Build & Test
```bash
# Install dependencies
make vendor

# Build all binaries (uses Docker for cross-compilation on macOS)
make bins

# Build Docker images
make images

# Run unit tests
make unit-test

# Run integration tests (Python-based)
make integration

# Start local development cluster (requires Docker-for-Mac)
make devcluster
```

### Git Hooks
Install pre-commit hooks that run `golangci-lint` automatically:
```bash
make install-hooks
```

## Architecture Overview

Kraken consists of five main components forming a distributed system:

- **Agent** - Runs on every host, implements Docker registry API, P2P client/server
- **Origin** - Dedicated seeders, stores blobs backed by pluggable storage, forms hash ring
- **Tracker** - Orchestrates peer connections, tracks content availability, forms hash ring
- **Proxy** - Handles uploads, routes to origins via hash ring, publishes tags
- **Build-Index** - Tag→digest mapping, powers cross-cluster replication, forms hash ring

**Key concepts:**
- Components use **self-healing hash rings** for distribution and HA
- **P2P protocol** is custom (BitTorrent-inspired but optimized for stable clusters)
- **Pseudo-random regular graphs** ensure high connectivity and fast distribution
- See docs/ARCHITECTURE.md for details

## Repository Structure

```
/agent           - Agent service (runs on every host)
/build-index     - Build-index service (tag storage and replication)
/origin          - Origin service (blob seeding)
/proxy           - Proxy service (upload handling)
/tracker         - Tracker service (peer coordination)
/lib             - Shared libraries
  /backend       - Storage backend implementations (S3, GCS, etc.)
  /hashring      - Consistent hashing and hash ring implementation
  /torrent       - P2P protocol implementation
  /store         - Local storage abstractions
  /dockerregistry - Docker registry API handling
/tools           - CLI tools (puller, visualization, etc.)
/docker          - Dockerfiles for each component
/examples        - Example deployments (k8s, devcluster)
/docs            - Documentation
```

## Code Conventions

**Follow docs/STYLEGUIDE.md strictly.** Key points:

- **Line length**: Code <100 chars, comments <80 chars
- **Variable naming**: Follow Go naming conventions (short, contextual names)
- **Comments**: Proper grammar, capitalization, punctuation. Avoid obvious/redundant comments.
- **Whitespace**: Be conservative with blank lines, avoid vertical clutter
- **Function signatures**: Break long signatures with one param per line
- **Testing**: Use testify/require for assertions, suffix test files with `_test.go`

### Long Function Example
```go
func Foo(
    bar int,
    baz bool,
    blah []int) (string, error) {

    ...
}
```

## Testing

- **Unit tests**: `make unit-test` - Fast, required before commits
- **Integration tests**: `make integration` - Slower, Python-based
- **Test tags**: Unit tests use `--tags "unit"`
- **Coverage**: Unit tests generate `coverage.txt`

All new features and bug fixes must include tests.

## Important Notes

### Build System
- **macOS**: Uses Docker for cross-compilation (cgo/sqlite3 limitation)
- **Native tools**: puller, reload, visualization can build natively on macOS
- **Linux bins**: agent, origin, tracker, proxy, build-index require Linux build
- Binaries are built inside `golang:1.24.0` container

### Hash Rings
- Multiple components (origin, tracker, build-index) use hash rings for sharding
- Hash rings are **self-healing** - handle member failures gracefully
- Critical for horizontal scaling - understand before modifying

### Storage Backends
- Pluggable architecture in `lib/backend`
- Each backend must implement common interface
- See `lib/backend/shadowbackend` for example proxy backend
- See `lib/backend/sqlbackend` for SQL-backed implementation

### P2P Protocol
- Custom protocol in `lib/torrent`
- Optimized for stable data center environments (not adversarial like BitTorrent)
- Tracker orchestrates connections but doesn't transfer data
- Peers negotiate directly for chunks

### Performance
- Blob size limit: 20G recommended (theoretically unbounded)
- Tag mutation supported but with caveats (Nginx caching, replication delays)
- System designed for immutable content (unique tags)

## Common Development Tasks

### Adding a new feature
1. Read relevant code first - understand patterns before changing
2. Check docs/ARCHITECTURE.md for architectural context
3. Follow existing patterns in the codebase
4. Add tests alongside implementation
5. Run `make unit-test` before committing
6. Lint automatically runs via git hooks if installed

### Debugging
- Use `make devcluster` for local testing
- Logs are your friend - check component logs
- Visualization tool available: `tools/bin/visualization`

### Modifying storage backends
- Look at existing implementations in `lib/backend`
- Maintain interface compatibility
- Consider both read and write paths
- Test with integration tests

## Working with Claude

### Commit Practices
- Run tests before committing: `make unit-test`
- Follow git hooks (golangci-lint will auto-run if installed)
- Write clear commit messages that explain "why" not "what"
- Don't commit until tests pass and code is linted

### Code Changes
- Always read files before modifying them
- Prefer editing existing files over creating new ones
- Don't add unnecessary features or abstractions
- Keep changes focused on the task at hand
- Follow STYLEGUIDE.md for all Go code

### Questions & Alignment
- Check ARCHITECTURE.md before major structural changes
- For architectural decisions, ask before implementing
- When multiple approaches exist, present options

### Don't Do This
- Don't create documentation files unless requested
- Don't add features beyond what's asked
- Don't use destructive git operations without asking
- Don't skip tests ("I'll add them later")
- Don't guess at build commands - use the Makefile

## References

- [Architecture](docs/ARCHITECTURE.md) - System design and component interactions
- [Configuration](docs/CONFIGURATION.md) - How to configure each component
- [Style Guide](docs/STYLEGUIDE.md) - Go code style requirements
- [Contributing](docs/CONTRIBUTING.md) - Fork-and-pull workflow
- [K8s Setup](examples/k8s/README.md) - Kubernetes deployment
- [Devcluster](examples/devcluster/README.md) - Local development setup

## Contact

- GitHub Issues: https://github.com/uber/kraken/issues

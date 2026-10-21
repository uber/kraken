# Kraken review guide

Kraken is a P2P Docker registry written in Go. Apply your normal review; the
rules below add Kraken-specific context and are not exhaustive. Review for
correctness and security first; style is secondary. Skip generated code
(`gen/`, `mocks/`).

## Components

- **agent**: runs on every host; read-only Docker registry that downloads and
  seeds blobs over P2P (`lib/torrent/scheduler`).
- **origin**: seeders storing blobs on disk (`lib/store`), sharded by a hash
  ring (`lib/hashring`); pulls from and writes back to storage backends
  (`lib/backend`).
- **tracker**: hands out peers and origins per torrent; never transfers data.
- **proxy**: read-write Docker registry for pushes; uploads blobs to origins
  and tags to build-index.
- **build-index**: tag-to-digest store; replicates tags within and across
  clusters.

## Correctness and security

- **Error checks**: much of the code predates Go 1.13. It wraps errors with
  `%s` and checks them with `==` (e.g. `backenderrors.ErrBlobNotFound`) or
  type-assertion helpers (e.g. `httputil.IsNotFound`). Don't copy these
  patterns; new code should use `%w`, `errors.Is` and `errors.As`. Flag
  wrapping an error that existing code still checks with `==` or a type
  assertion, unless those checks are updated too.
- **Handler errors**: `handler.Errorf` defaults to 500, so client errors need
  `.Status(...)`. Any error that isn't a `*handler.Error` becomes a 500 with
  its message sent to the client.
- **Intentional 404s**: some paths map backend failures to 404 so pushes
  succeed while storage is down (e.g. `originStat`, `resolveFromBackend`).
  Don't flag these.
- **Resource leaks**: close response bodies, files and store readers on every
  path, including bodies returned by `httputil.Send` and its wrappers.
- **HTTP timeouts**: the `httputil` default timeout covers reading the body,
  so blob transfers need a longer `SendTimeout`.
- **Scheduler event loop**: `state`, `connstate` and `announcequeue` are owned
  by the event loop and may only be touched inside `event.apply`. Never send
  events or do network I/O from `apply`; do the work in a goroutine and report
  back with an event.
- **Time**: logic that tests need to control (TTLs, intervals, timeouts)
  should use the injected `clock.Clock`, not `time.Now` or `time.Sleep`.
- **Goroutines**: every goroutine needs a stop path.
- **Digests**: build them with `core.ParseSHA256Digest` or
  `core.NewSHA256DigestFromHex`. Don't confuse `InfoHash` (torrent and tracker
  state) with `Digest` (stores, rings, backends).
- **Blob writes**: go through the store APIs (upload, sha256 verify, atomic
  rename into cache). Never write cache directories directly or bypass
  verification.
- **Retries**: `persistedretry` executors must be idempotent; tasks run at
  least once and are retried after restarts.
- **Tag replication**: replicate dependencies before the tag.
- **Request input**: `httputil.ParseParam` unescapes path params, so values
  may contain `/` or `..`. Validate them (e.g. `httputil.ParseDigest`,
  `namepath.ParseDockerTag`, `namepath.ValidateDockerRepo`) before using them
  in paths, backend keys or URLs.
- **Peer input**: P2P messages, handshakes and tracker announces are
  untrusted. Bounds-check piece indices, lengths and bitfields, and nil-check
  sub-messages; a panic kills the process.
- **Auth**: the Go servers do no authentication, and the default nginx config
  requires client certs only for non-GET/HEAD requests. State-changing
  endpoints must not be GET or HEAD.
- **Secrets**: never log configs, credentials or DSNs. Keep SQL parameterized.
  Never set `InsecureSkipVerify` or add plain-HTTP fallbacks.
- **Config defaults**: value-receiver `applyDefaults` returns a copy that must
  be assigned (`c = c.applyDefaults()`). Zero values mean "use the default";
  name new booleans so that `false` is the default (e.g. `DisableX`).
- **Wiring**: a new backend needs a blank import in each binary that should
  support it (`origin/main.go`, `build-index/main.go`). A new mock needs an
  `add_mock` line in the `Makefile`, or `make mocks` deletes it.
- **Metrics**: no high-cardinality tags (repo, tag, digest, namespace, peer).
- **Tests**: new features need tests; bug fixes need a regression test.

## Style

- **Table tests**: multiple cases of one behavior belong in a table-driven test
  with `t.Run` subtests.
- **Test helpers**: use testify `require`, existing fixtures and gomock mocks
  from `mocks/`. Prefer a mock clock over `time.Sleep`.
- **Comments**: explain why, not what. Flag comments that restate the code.

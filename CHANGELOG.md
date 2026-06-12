# Changelog

## 0.2.0

Rewritten on `tokio-postgres` with a private current-thread runtime
per connection (gossamer's `std::http_h3` pattern); requires
gossamer 0.13.0.

### Added

- TLS via rustls with native roots, honoring `sslmode`
  (verification always on; deferred server-name resolution so unix
  sockets and `sslmode=disable` keep working).
- True row streaming: `Rows` wraps an owned `RowStream`; results are
  no longer materialized.
- `copy_in` / `copy_out` (PostgreSQL `COPY` both directions).
- `listen` / `unlisten` / `poll_notification` — `NOTIFY` messages
  forwarded from the connection task into a per-connection channel.
- `execute_params` / `query_params` on transactions.
- Rich type decoding: NUMERIC (exact decimal string), DATE / TIME /
  TIMESTAMP (ISO 8601), TIMESTAMPTZ (RFC 3339), UUID, JSON / JSONB
  (compact), int / float / text / bool arrays (JSON array text).
- 18 DATABASE_URL-gated tests (streaming drain, COPY round trip,
  cross-connection notify, in-tx params, type-shape assertions).
- `examples/gossamer/` exercises the full surface — prepared
  statements, parameterized transactions, COPY, NOTIFY, the Select
  builder, migrations, pooling — bit-identically on `gos run`,
  `gos build`, and `gos build --release` against live PostgreSQL.

### Changed

- Dropped the sync `postgres` dependency (and `parking_lot`); the
  client is shared lock-free (`tokio_postgres::Client` methods take
  `&self`).
- `interrupt()` builds a throwaway runtime for the cancel
  connection so it works while the owning thread is parked inside a
  query.

## 0.1.0

First working release, versioned independently of gossamer. Compiles
and runs against gossamer 0.12.1's actual SQL trait surface (the
0.9.0 entry below was written against an API shape that never
shipped) and is verified end-to-end from Gossamer source on every
tier.

### Fixed

- Builds against the real `gossamer_runtime::sql` traits: the
  imagined `Error::driver_kind` / `Kind::Transient` / `with_code` /
  `Stmt::set_timeout` API was replaced with the shipped surface.
  Error classification for Rust callers moved to the public
  `classify(&postgres::Error) -> DriverErrorKind`; the SQLSTATE is
  embedded in the driver error message (`[23505] duplicate key …`)
  and closed connections map to `Error::Closed`.
- `set_busy_timeout` takes `i64` per the trait (still
  `SET lock_timeout`; negative values clamp to 0).

### Added

- Real server-side prepared statements: `prepare` now prepares
  eagerly and `Stmt::execute` / `query` reuse the prepared statement
  instead of re-preparing per call.
- `interrupt()` cancels the in-flight statement via
  `postgres::CancelToken` (was a no-op), making the façade's
  `execute_ctx` / `query_ctx` cancellation effective.
- `close()` marks the connection closed; subsequent calls return
  `Error::Closed` per the trait contract.
- `examples/crud.rs` and `examples/transactions.rs` (Rust, via the
  `gossamer_std` façade) and `examples/gossamer/` (a Gossamer
  project consuming the driver through `[rust-bindings]`), all
  verified against a live PostgreSQL — the Gossamer example
  bit-identically under `gos run`, `gos build`, and
  `gos build --release`.
- Tests for prepared-statement reuse, closed-connection rejection,
  and SQLSTATE-tagged constraint errors.

## 0.9.0

Tracks `gossamer-std` 0.9.0's redesigned SQL trait surface.

### Added

- `begin_with(IsolationLevel)` — maps each isolation level onto the
  corresponding PostgreSQL `BEGIN ISOLATION LEVEL …`.
- `set_busy_timeout(ms)` — emits `SET lock_timeout = <ms>`.
- `set_timeout(ms)` on prepared statements — emits `SET statement_timeout`
  before each call.
- `ping()` — `SELECT 1` round-trip health check.
- Savepoint support via the default trait impls (`SAVEPOINT` /
  `RELEASE SAVEPOINT` / `ROLLBACK TO SAVEPOINT`) — the new trait
  default uses these directly.

### Changed

- Error mapping now produces classified `sql::Error::Driver(DriverError)`
  with a `Kind` (Transient / Constraint / Permission / Syntax / Io /
  Other) derived from the PostgreSQL `SQLSTATE`, plus the raw SQLSTATE
  in `DriverError.code`.  Callers can use `err.is_retriable()` to drive
  retry policy.
- Macro now uses the 0.9.0 ergonomic `name:` form. The hand-written
  `__bindings_force_link` shim survives as a one-liner for runner-template
  back-compat.

## 0.0.0

Initial release.

### Features

- `PostgresDriver` implementing `gossamer_std::database::sql::Driver`
- Full CRUD via `Conn::execute` and `Conn::query` with `$1`-style positional parameters
- Prepared statements (`Conn::prepare` → `Stmt`)
- Transactions (`Conn::begin` → `Tx`) with explicit `commit` / `rollback` and
  implicit rollback on drop
- Gossamer binding: `postgres::register()` registers the driver with
  `std::database::sql` so `sql::open("postgres", url)` works

### Type mapping

| PostgreSQL type | `Value` variant |
|---|---|
| `BOOL` | `Bool` |
| `INT2` / `SMALLINT` | `Int` (widened to `i64`) |
| `INT4` / `INTEGER` | `Int` (widened to `i64`) |
| `INT8` / `BIGINT` | `Int` |
| `FLOAT4` / `REAL` | `Float` (widened to `f64`) |
| `FLOAT8` / `DOUBLE PRECISION` | `Float` |
| `TEXT` / `VARCHAR` / `CHAR` / `NAME` | `Text` |
| `BYTEA` | `Blob` |
| NULL (any type) | `Null` |

### Limitations

- TLS not supported; only `NoTls` connections. Run PostgreSQL with
  `sslmode=disable` or use an SSL-terminating proxy.
- `NUMERIC`, `DATE`, `TIMESTAMP`, `TIMESTAMPTZ`, `UUID`, and other types not
  listed above fall back to `Value::Null`. Cast to `text` in SQL to receive
  them as `Value::Text` (e.g. `SELECT amount::text FROM ledger`).
- `TransactionImpl::execute` does not accept parameters. Use
  `Conn::prepare` → `Stmt::execute` for parameterized DML inside a transaction.
- No connection pooling; each `sql::open` call opens a new TCP connection.

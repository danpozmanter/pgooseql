# pgooseql

PostgreSQL driver for Gossamer's `std::database::sql`.

Implements the `gossamer_runtime::sql::Driver` trait surface over the
[`postgres`](https://crates.io/crates/postgres) crate (NoTls), and
registers as `"postgres"` so Gossamer programs reach it through
`sql::open("postgres", url)`.

## Usage from Gossamer

Declare the binding in `project.toml`:

```toml
[rust-bindings]
pgooseql = { path = "../pgooseql" }
```

Register once at startup, then use the `std::database::sql` surface
(queries, prepared statements, parameterized transactions, COPY,
LISTEN/NOTIFY, pooling, migrations, the Select builder). Works
identically under `gos run`, `gos build`, and `gos build --release`
(gossamer 0.13.0+):

```gossamer
use postgres::register
use std::database::sql

fn main() -> Result<(), sql::Error> {
    register()
    let mut db = sql::open("postgres", "host=localhost user=me dbname=mydb")?
    db.execute("INSERT INTO t VALUES ($1)", &[sql::Value::Int(1)])?
    let mut rows = db.query("SELECT id FROM t", &[])?
    while let Some(row) = rows.next_row()? {
        println!("{}", row.get_i64("id")?)
    }
    Ok(())
}
```

See `examples/gossamer/` for a full CRUD walk-through.

## Usage from Rust

The driver also works directly through the `gossamer_std` façade:

```rust
use std::sync::Arc;
use gossamer_std::database::sql::{self, Value};
use pgooseql::PostgresDriver;

sql::register(Arc::new(PostgresDriver));
let mut db = sql::open("postgres", &url)?;
db.execute("INSERT INTO t VALUES ($1)", &[Value::Int(1)])?;
```

Runnable examples (need a reachable server):

```sh
DATABASE_URL="host=localhost user=me dbname=mydb" cargo run --example crud
DATABASE_URL="host=localhost user=me dbname=mydb" cargo run --example transactions
```

## Connection strings

Anything `postgres::Client::connect` accepts: the keyword form
(`host=localhost port=5432 user=me dbname=mydb`) or the URI form
(`postgresql://me@localhost/mydb`). Unix sockets work via
`host=/var/run/postgresql`.

## Type mapping

| PostgreSQL | `sql::Value` |
|---|---|
| BOOL | `Bool` |
| INT2 / INT4 / INT8 | `Int` |
| FLOAT4 / FLOAT8 | `Float` |
| TEXT / VARCHAR / BPCHAR / NAME | `Text` |
| BYTEA | `Blob` |
| NUMERIC | `Text` (exact decimal string) |
| DATE / TIME / TIMESTAMP | `Text` (ISO 8601) |
| TIMESTAMPTZ | `Text` (RFC 3339) |
| UUID | `Text` (hyphenated) |
| JSON / JSONB | `Text` (compact JSON) |
| INT / FLOAT / TEXT / BOOL arrays | `Text` (JSON array) |
| NULL (any type) | `Null` |

JSON-shaped values (`JSONB`, arrays) parse on the Gossamer side with
the std `json` module. Exotic types (ranges, composites, custom
enums) fall back to text when the wire format allows, else `Null`.

## TLS

Connections always negotiate through rustls with the platform's
native root store, honoring `sslmode` from the connection string:
`disable` stays plaintext, `prefer`/`require` upgrade to TLS with
certificate verification always on (stricter than libpq's bare
`require`). Unix-socket connections skip TLS.

## Behaviour notes

- Result sets stream: `Rows::next_row` pulls one row at a time from
  an owned `RowStream` — constant memory for large results.
- `prepare` creates a real server-side prepared statement; reuse the
  statement to skip re-parsing.
- `copy_in` / `copy_out` map to `COPY … FROM STDIN` / `TO STDOUT`.
- `listen` / `poll_notification` deliver `NOTIFY` messages; the
  connection's socket is pumped during any driver call, and
  `poll_notification` with a positive timeout parks on the socket.
- `execute_params` / `query_params` work inside transactions.
- `interrupt()` cancels the in-flight statement through a PostgreSQL
  cancel connection, so `execute_ctx` / `query_ctx` context
  cancellation works mid-query.
- `set_busy_timeout(ms)` maps to `SET lock_timeout`; `0` disables.
- Dropping an uncommitted transaction rolls it back.

## Tests

`cargo test` runs against `DATABASE_URL` when set and skips silently
otherwise:

```sh
createdb pgooseql_test
DATABASE_URL="host=/var/run/postgresql dbname=pgooseql_test" cargo test
```

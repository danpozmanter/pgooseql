//! PostgreSQL driver for Gossamer's `std::database::sql`.
//!
//! Register the driver once at startup, then open connections with
//! `sql::open("postgres", url)`. The `url` is a libpq connection string
//! (`host=localhost port=5432 user=me dbname=mydb`) or the URI form
//! (`postgresql://me@localhost/mydb`).
//!
//! Built on `tokio-postgres` behind a private current-thread tokio
//! runtime per connection; every driver entry point is synchronous and
//! drives the runtime via `block_on`. Queries stream rows lazily,
//! `COPY` bulk transfer and `LISTEN`/`NOTIFY` are supported, and TLS
//! (rustls) is negotiated according to `sslmode` in the connection
//! string.
//!
//! Gossamer usage:
//! ```text
//! use postgres::register
//! use std::database::sql
//!
//! fn main() -> Result<(), errors::Error> {
//!     register()
//!     let db = sql::open("postgres", "host=localhost user=me dbname=mydb")?
//!     db.execute("INSERT INTO t VALUES ($1)", &[sql::Value::Int(1)])?
//!     Ok(())
//! }
//! ```

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt, TryFutureExt, TryStreamExt, stream};
use gossamer_binding::register_module;
use gossamer_runtime::sql::Notification;
use gossamer_std::database::sql::{
    ConnectionImpl, Driver, DriverErrorKind, Error, IsolationLevel, RowsImpl, StatementImpl,
    TransactionImpl, Value,
};
use rustls::pki_types::InvalidDnsNameError;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::types::{FromSql, IsNull, ToSql, Type};
use tokio_postgres::{AsyncMessage, CancelToken, Client, Row, RowStream, Statement};
use tokio_postgres_rustls::MakeRustlsConnect;

// --- Gossamer binding --------------------------------------------

register_module!(
    name: postgres,
    doc: "PostgreSQL driver — call `register()` once before `sql::open`.",

    /// Registers the PostgreSQL driver with `std::database::sql`. Idempotent.
    fn register() -> () {
        gossamer_std::database::sql::register(
            std::sync::Arc::new(PostgresDriver),
        );
    }
);

/// Linker hook — kept for runner-template back-compat.
pub fn __bindings_force_link() {
    __gos_postgres::force_link();
}

// --- driver ------------------------------------------------------

/// PostgreSQL driver entry. Pass a libpq connection string or URI to
/// `sql::open("postgres", url)`.
#[derive(Debug, Default)]
pub struct PostgresDriver;

impl Driver for PostgresDriver {
    fn name(&self) -> &str {
        "postgres"
    }

    fn open(&self, url: &str) -> Result<Box<dyn ConnectionImpl>, Error> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::driver("postgres", format!("runtime: {e}")))?;
        let runtime = Arc::new(runtime);
        let tls = make_tls()?;
        let (client, connection) = runtime
            .block_on(tokio_postgres::connect(url, tls.clone()))
            .map_err(map_err)?;
        let cancel = client.cancel_token();
        let (forward, notifications) = mpsc::unbounded_channel();
        // The connection future owns the socket; poll_message routes
        // query responses to the client internally and surfaces
        // server-pushed messages here. This task only progresses while
        // the runtime is inside block_on, which every driver entry
        // point goes through.
        runtime.spawn(async move {
            let mut connection = connection;
            let mut messages = stream::poll_fn(move |cx| connection.poll_message(cx));
            while let Some(message) = messages.next().await {
                match message {
                    Ok(AsyncMessage::Notification(n)) => {
                        let forwarded = Notification {
                            channel: n.channel().to_string(),
                            payload: n.payload().to_string(),
                            process_id: i64::from(n.process_id()),
                        };
                        if forward.send(forwarded).is_err() {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });
        Ok(Box::new(PgConn {
            runtime,
            client: Arc::new(client),
            notifications,
            cancel,
            tls,
            closed: false,
        }))
    }
}

/// Builds the rustls connector used for every connection.
///
/// Certificate verification is always on, against the platform's
/// native root store (webpki roots when the native store is empty) —
/// stricter than libpq's bare `sslmode=require`, which skips
/// verification. Whether TLS is used at all still follows `sslmode`
/// in the connection string (`disable` / `prefer` / `require`).
fn make_tls() -> Result<PgTls, Error> {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    roots.add_parsable_certificates(native.certs);
    if roots.is_empty() {
        roots
            .roots
            .extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    // The provider is passed explicitly: other crates in the dep graph
    // enable rustls's `ring` feature, so the implicit process default
    // would be ambiguous.
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::driver("postgres", format!("tls: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(PgTls(MakeRustlsConnect::new(config)))
}

/// TLS connector that defers server-name validation to the moment TLS
/// is actually negotiated. tokio-postgres calls `make_tls_connect`
/// before consulting `sslmode`, so connections that never use TLS
/// (unix sockets, `sslmode=disable`, server-refused TLS under
/// `prefer`) must not fail just because the host is not a valid DNS
/// name.
#[derive(Clone)]
struct PgTls(MakeRustlsConnect);

impl<S> MakeTlsConnect<S> for PgTls
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = <MakeRustlsConnect as MakeTlsConnect<S>>::Stream;
    type TlsConnect = DeferredTlsConnect<<MakeRustlsConnect as MakeTlsConnect<S>>::TlsConnect>;
    type Error = InvalidDnsNameError;

    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error> {
        Ok(DeferredTlsConnect(MakeTlsConnect::<S>::make_tls_connect(
            &mut self.0,
            domain,
        )))
    }
}

/// Carries either a real TLS connector or the server-name error to
/// surface if (and only if) a TLS handshake is actually attempted.
struct DeferredTlsConnect<C>(Result<C, InvalidDnsNameError>);

impl<S, C> TlsConnect<S> for DeferredTlsConnect<C>
where
    C: TlsConnect<S>,
    C::Future: Send + 'static,
    C::Stream: Send + 'static,
    C::Error: 'static,
{
    type Stream = C::Stream;
    type Error = Box<dyn std::error::Error + Sync + Send>;
    type Future = BoxFuture<'static, Result<C::Stream, Self::Error>>;

    fn connect(self, stream: S) -> Self::Future {
        match self.0 {
            Ok(connector) => Box::pin(connector.connect(stream).map_err(Into::into)),
            Err(e) => Box::pin(futures_util::future::ready(Err(Box::new(e) as _))),
        }
    }
}

// --- connection --------------------------------------------------

struct PgConn {
    runtime: Arc<Runtime>,
    client: Arc<Client>,
    notifications: mpsc::UnboundedReceiver<Notification>,
    cancel: CancelToken,
    tls: PgTls,
    closed: bool,
}

impl PgConn {
    fn guard(&self) -> Result<(), Error> {
        if self.closed {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }
}

impl ConnectionImpl for PgConn {
    fn prepare(&mut self, sql: &str) -> Result<Box<dyn StatementImpl>, Error> {
        self.guard()?;
        let stmt = self
            .runtime
            .block_on(self.client.prepare(sql))
            .map_err(map_err)?;
        Ok(Box::new(PgStmt {
            runtime: Arc::clone(&self.runtime),
            client: Arc::clone(&self.client),
            stmt,
        }))
    }

    fn begin(&mut self) -> Result<Box<dyn TransactionImpl>, Error> {
        self.begin_with(IsolationLevel::Default)
    }

    fn begin_with(&mut self, iso: IsolationLevel) -> Result<Box<dyn TransactionImpl>, Error> {
        self.guard()?;
        let stmt = match iso {
            IsolationLevel::Default => "BEGIN",
            IsolationLevel::ReadUncommitted => "BEGIN ISOLATION LEVEL READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "BEGIN ISOLATION LEVEL READ COMMITTED",
            IsolationLevel::RepeatableRead => "BEGIN ISOLATION LEVEL REPEATABLE READ",
            IsolationLevel::Serializable => "BEGIN ISOLATION LEVEL SERIALIZABLE",
        };
        self.runtime
            .block_on(self.client.batch_execute(stmt))
            .map_err(map_err)?;
        Ok(Box::new(PgTx {
            runtime: Arc::clone(&self.runtime),
            client: Arc::clone(&self.client),
            finished: false,
        }))
    }

    /// Maps the busy timeout to `lock_timeout` (PostgreSQL's analogue of
    /// SQLite's busy timeout); `0` disables it.
    fn set_busy_timeout(&mut self, ms: i64) -> Result<(), Error> {
        self.guard()?;
        let ms = ms.max(0);
        let sql = format!("SET lock_timeout = {ms}");
        self.runtime
            .block_on(self.client.batch_execute(&sql))
            .map_err(map_err)
    }

    fn interrupt(&self) {
        // The shared runtime may be parked inside block_on on the
        // thread being interrupted, so cancellation runs on its own
        // short-lived runtime; cancel_query opens a dedicated TCP
        // connection server-side. Best-effort by trait contract.
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return;
        };
        let _ = runtime.block_on(self.cancel.cancel_query(self.tls.clone()));
    }

    fn ping(&mut self) -> Result<(), Error> {
        self.guard()?;
        self.runtime
            .block_on(self.client.batch_execute("SELECT 1"))
            .map_err(map_err)
    }

    fn copy_in(&mut self, sql: &str, data: &[u8]) -> Result<u64, Error> {
        self.guard()?;
        let data = Bytes::copy_from_slice(data);
        let client = Arc::clone(&self.client);
        self.runtime
            .block_on(async move {
                let mut sink = Box::pin(client.copy_in(sql).await?);
                sink.send(data).await?;
                sink.as_mut().finish().await
            })
            .map_err(map_err)
    }

    fn copy_out(&mut self, sql: &str) -> Result<Vec<u8>, Error> {
        self.guard()?;
        let client = Arc::clone(&self.client);
        let collected: Result<Vec<u8>, tokio_postgres::Error> = self.runtime.block_on(async move {
            let mut chunks = Box::pin(client.copy_out(sql).await?);
            let mut out = Vec::new();
            while let Some(chunk) = chunks.try_next().await? {
                out.extend_from_slice(&chunk);
            }
            Ok(out)
        });
        collected.map_err(map_err)
    }

    fn listen(&mut self, channel: &str) -> Result<(), Error> {
        self.guard()?;
        let sql = format!("LISTEN {}", quote_identifier(channel));
        self.runtime
            .block_on(self.client.batch_execute(&sql))
            .map_err(map_err)
    }

    fn unlisten(&mut self, channel: &str) -> Result<(), Error> {
        self.guard()?;
        let sql = format!("UNLISTEN {}", quote_identifier(channel));
        self.runtime
            .block_on(self.client.batch_execute(&sql))
            .map_err(map_err)
    }

    /// Notifications are pumped by the connection task, which only
    /// runs while this runtime is inside `block_on` — i.e. during any
    /// driver call. A positive timeout therefore parks on the runtime
    /// so fresh socket data is read and forwarded; a zero timeout
    /// observes what previous driver activity already pumped.
    fn poll_notification(&mut self, timeout_ms: i64) -> Result<Option<Notification>, Error> {
        self.guard()?;
        if let Ok(n) = self.notifications.try_recv() {
            return Ok(Some(n));
        }
        let wait = Duration::from_millis(timeout_ms.max(0) as u64);
        let runtime = Arc::clone(&self.runtime);
        // The timeout future is built inside block_on: its timer needs
        // the runtime context.
        let received =
            runtime.block_on(async { tokio::time::timeout(wait, self.notifications.recv()).await });
        match received {
            Ok(Some(n)) => Ok(Some(n)),
            // The forwarder task ended: the connection is gone.
            Ok(None) => Err(Error::Closed),
            // Timed out; pick up anything pumped during the wait.
            Err(_) => Ok(self.notifications.try_recv().ok()),
        }
    }

    fn close(&mut self) -> Result<(), Error> {
        self.guard()?;
        self.closed = true;
        Ok(())
    }
}

// --- statement ---------------------------------------------------

struct PgStmt {
    runtime: Arc<Runtime>,
    client: Arc<Client>,
    stmt: Statement,
}

impl StatementImpl for PgStmt {
    fn execute(&mut self, params: &[Value]) -> Result<u64, Error> {
        let owned: Vec<PgParam> = params.iter().cloned().map(PgParam).collect();
        let refs: Vec<&(dyn ToSql + Sync)> =
            owned.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        self.runtime
            .block_on(self.client.execute(&self.stmt, &refs))
            .map_err(map_err)
    }

    fn query(&mut self, params: &[Value]) -> Result<Box<dyn RowsImpl>, Error> {
        let owned: Vec<PgParam> = params.iter().cloned().map(PgParam).collect();
        query_streaming(&self.runtime, &self.client, &self.stmt, &owned)
    }
}

/// Starts a streaming query on `stmt`; rows are pulled lazily by
/// [`PgRows::next_row`], never materialized as a whole result set.
fn query_streaming(
    runtime: &Arc<Runtime>,
    client: &Client,
    stmt: &Statement,
    params: &[PgParam],
) -> Result<Box<dyn RowsImpl>, Error> {
    let columns: Vec<String> = stmt
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    let rows = runtime
        .block_on(client.query_raw(stmt, params.iter().map(|p| p as &(dyn ToSql + Sync))))
        .map_err(map_err)?;
    Ok(Box::new(PgRows {
        runtime: Arc::clone(runtime),
        stream: Box::pin(rows),
        columns,
    }))
}

// --- rows --------------------------------------------------------

struct PgRows {
    runtime: Arc<Runtime>,
    stream: Pin<Box<RowStream>>,
    columns: Vec<String>,
}

impl RowsImpl for PgRows {
    fn next_row(&mut self) -> Result<Option<Vec<Value>>, Error> {
        let row = self
            .runtime
            .block_on(self.stream.try_next())
            .map_err(map_err)?;
        Ok(row.map(|r| row_to_values(&r)))
    }

    fn columns(&self) -> &[String] {
        &self.columns
    }
}

// --- transaction -------------------------------------------------

struct PgTx {
    runtime: Arc<Runtime>,
    client: Arc<Client>,
    finished: bool,
}

impl TransactionImpl for PgTx {
    fn commit(&mut self) -> Result<(), Error> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.runtime
            .block_on(self.client.batch_execute("COMMIT"))
            .map_err(map_err)
    }

    fn rollback(&mut self) -> Result<(), Error> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.runtime
            .block_on(self.client.batch_execute("ROLLBACK"))
            .map_err(map_err)
    }

    fn execute(&mut self, sql: &str) -> Result<u64, Error> {
        self.runtime
            .block_on(self.client.execute(sql, &[]))
            .map_err(map_err)
    }

    fn execute_params(&mut self, sql: &str, params: &[Value]) -> Result<u64, Error> {
        let stmt = self
            .runtime
            .block_on(self.client.prepare(sql))
            .map_err(map_err)?;
        let owned: Vec<PgParam> = params.iter().cloned().map(PgParam).collect();
        let refs: Vec<&(dyn ToSql + Sync)> =
            owned.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        self.runtime
            .block_on(self.client.execute(&stmt, &refs))
            .map_err(map_err)
    }

    fn query_params(&mut self, sql: &str, params: &[Value]) -> Result<Box<dyn RowsImpl>, Error> {
        let stmt = self
            .runtime
            .block_on(self.client.prepare(sql))
            .map_err(map_err)?;
        let owned: Vec<PgParam> = params.iter().cloned().map(PgParam).collect();
        query_streaming(&self.runtime, &self.client, &stmt, &owned)
    }
}

impl Drop for PgTx {
    fn drop(&mut self) {
        if !self.finished {
            // Best-effort implicit rollback; error is intentionally discarded.
            let _ = self.runtime.block_on(self.client.batch_execute("ROLLBACK"));
        }
    }
}

// --- parameter binding -------------------------------------------

struct PgParam(Value);

impl std::fmt::Debug for PgParam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl ToSql for PgParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match &self.0 {
            Value::Null => Ok(IsNull::Yes),
            Value::Bool(b) => b.to_sql(ty, out),
            Value::Int(i) => i.to_sql(ty, out),
            Value::Float(f) => f.to_sql(ty, out),
            Value::Text(s) => s.as_str().to_sql(ty, out),
            Value::Blob(b) => b.as_slice().to_sql(ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    tokio_postgres::types::to_sql_checked!();
}

// --- helpers -----------------------------------------------------

/// Coarsely classifies a PostgreSQL error by SQLSTATE.
///
/// `Error::Driver` carries no structured kind, so this is the
/// Rust-side classification entry; the SQLSTATE is also embedded in
/// the driver error message (`[23505] duplicate key …`) for callers
/// that only see the façade error.
#[must_use]
pub fn classify(e: &tokio_postgres::Error) -> DriverErrorKind {
    let Some(db_err) = e.as_db_error() else {
        return if e.is_closed() {
            DriverErrorKind::Connection
        } else {
            DriverErrorKind::Other
        };
    };
    let sqlstate = db_err.code().code();
    match sqlstate {
        "23505" => DriverErrorKind::UniqueViolation,
        "23503" => DriverErrorKind::ForeignKeyViolation,
        // lock_timeout (55P03) and statement_timeout / cancel (57014).
        "55P03" => DriverErrorKind::Timeout,
        "57014" => DriverErrorKind::Cancelled,
        _ if sqlstate.starts_with("08") => DriverErrorKind::Connection,
        _ => DriverErrorKind::Other,
    }
}

fn map_err(e: tokio_postgres::Error) -> Error {
    if e.is_closed() {
        return Error::Closed;
    }
    let message = match e.as_db_error() {
        Some(db_err) => format!("[{}] {}", db_err.code().code(), db_err.message()),
        None => e.to_string(),
    };
    Error::driver("postgres", message)
}

/// Quotes a channel name as a PostgreSQL identifier (doubles embedded
/// quotes) so `LISTEN` / `UNLISTEN` are injection-safe.
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn row_to_values(row: &Row) -> Vec<Value> {
    (0..row.len()).map(|i| value_from_row(row, i)).collect()
}

fn decoded<'a, T: FromSql<'a>>(row: &'a Row, i: usize) -> Option<T> {
    row.try_get::<_, Option<T>>(i).ok().flatten()
}

/// Renders a decoded array column as a JSON array string; arrays
/// arrive Gossamer-side as JSON text — parse with `std::encoding::json`.
fn json_array<T: serde::Serialize>(items: Option<Vec<T>>) -> Value {
    items
        .and_then(|v| serde_json::to_string(&v).ok())
        .map_or(Value::Null, Value::Text)
}

/// Maps a single PostgreSQL column to a [`Value`].
///
/// Natively handled: BOOL, INT2/4/8, FLOAT4/8, TEXT/VARCHAR/BPCHAR/NAME,
/// BYTEA. Decoded to [`Value::Text`]: NUMERIC (exact decimal string),
/// DATE (`%Y-%m-%d`), TIME (`%H:%M:%S%.f`), TIMESTAMP
/// (`%Y-%m-%dT%H:%M:%S%.f`), TIMESTAMPTZ (RFC 3339), UUID (hyphenated),
/// JSON/JSONB (compact), and INT/FLOAT/TEXT/BOOL arrays (JSON array
/// text). Anything else attempts a text fallback and returns
/// [`Value::Null`] on failure — cast the column to `text` in SQL to
/// receive it as [`Value::Text`].
fn value_from_row(row: &Row, i: usize) -> Value {
    let ty = row.columns()[i].type_();
    if *ty == Type::BOOL {
        decoded::<bool>(row, i).map_or(Value::Null, Value::Bool)
    } else if *ty == Type::INT2 {
        decoded::<i16>(row, i).map_or(Value::Null, |v| Value::Int(i64::from(v)))
    } else if *ty == Type::INT4 {
        decoded::<i32>(row, i).map_or(Value::Null, |v| Value::Int(i64::from(v)))
    } else if *ty == Type::INT8 {
        decoded::<i64>(row, i).map_or(Value::Null, Value::Int)
    } else if *ty == Type::FLOAT4 {
        decoded::<f32>(row, i).map_or(Value::Null, |v| Value::Float(f64::from(v)))
    } else if *ty == Type::FLOAT8 {
        decoded::<f64>(row, i).map_or(Value::Null, Value::Float)
    } else if *ty == Type::BYTEA {
        decoded::<Vec<u8>>(row, i).map_or(Value::Null, Value::Blob)
    } else if *ty == Type::NUMERIC {
        decoded::<rust_decimal::Decimal>(row, i).map_or(Value::Null, |v| Value::Text(v.to_string()))
    } else if *ty == Type::DATE {
        decoded::<chrono::NaiveDate>(row, i).map_or(Value::Null, |v| {
            Value::Text(v.format("%Y-%m-%d").to_string())
        })
    } else if *ty == Type::TIME {
        decoded::<chrono::NaiveTime>(row, i).map_or(Value::Null, |v| {
            Value::Text(v.format("%H:%M:%S%.f").to_string())
        })
    } else if *ty == Type::TIMESTAMP {
        decoded::<chrono::NaiveDateTime>(row, i).map_or(Value::Null, |v| {
            Value::Text(v.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
        })
    } else if *ty == Type::TIMESTAMPTZ {
        decoded::<chrono::DateTime<chrono::Utc>>(row, i)
            .map_or(Value::Null, |v| Value::Text(v.to_rfc3339()))
    } else if *ty == Type::UUID {
        decoded::<uuid::Uuid>(row, i).map_or(Value::Null, |v| Value::Text(v.to_string()))
    } else if *ty == Type::JSON || *ty == Type::JSONB {
        decoded::<serde_json::Value>(row, i).map_or(Value::Null, |v| Value::Text(v.to_string()))
    } else if *ty == Type::INT2_ARRAY {
        json_array(
            decoded::<Vec<i16>>(row, i).map(|v| v.into_iter().map(i64::from).collect::<Vec<_>>()),
        )
    } else if *ty == Type::INT4_ARRAY {
        json_array(
            decoded::<Vec<i32>>(row, i).map(|v| v.into_iter().map(i64::from).collect::<Vec<_>>()),
        )
    } else if *ty == Type::INT8_ARRAY {
        json_array(decoded::<Vec<i64>>(row, i))
    } else if *ty == Type::FLOAT4_ARRAY {
        json_array(
            decoded::<Vec<f32>>(row, i).map(|v| v.into_iter().map(f64::from).collect::<Vec<_>>()),
        )
    } else if *ty == Type::FLOAT8_ARRAY {
        json_array(decoded::<Vec<f64>>(row, i))
    } else if *ty == Type::TEXT_ARRAY || *ty == Type::VARCHAR_ARRAY {
        json_array(decoded::<Vec<String>>(row, i))
    } else if *ty == Type::BOOL_ARRAY {
        json_array(decoded::<Vec<bool>>(row, i))
    } else {
        // TEXT, VARCHAR, BPCHAR, NAME + any type String accepts.
        decoded::<String>(row, i).map_or(Value::Null, Value::Text)
    }
}

// --- tests -------------------------------------------------------

#[cfg(test)]
mod tests {
    use gossamer_std::database::sql::Conn;

    use super::*;

    fn connect() -> Option<Conn> {
        let url = std::env::var("DATABASE_URL").ok()?;
        Some(Conn::new(PostgresDriver.open(&url).expect("connect")))
    }

    fn open_raw() -> Option<Box<dyn ConnectionImpl>> {
        let url = std::env::var("DATABASE_URL").ok()?;
        Some(PostgresDriver.open(&url).expect("connect"))
    }

    fn exec_raw(conn: &mut Box<dyn ConnectionImpl>, sql: &str) {
        conn.prepare(sql).unwrap().execute(&[]).unwrap();
    }

    #[test]
    fn crud_round_trip() {
        let Some(mut db) = connect() else {
            return;
        };
        db.execute("CREATE TEMP TABLE t (id BIGINT, name TEXT)", &[])
            .unwrap();
        db.execute(
            "INSERT INTO t VALUES ($1, $2)",
            &[Value::Int(1), Value::Text("alice".into())],
        )
        .unwrap();
        let mut rows = db
            .query("SELECT id, name FROM t WHERE id = $1", &[Value::Int(1)])
            .unwrap();
        let row = rows.next_row().unwrap().unwrap();
        assert_eq!(row.get("id"), Some(&Value::Int(1)));
        assert_eq!(row.get("name"), Some(&Value::Text("alice".into())));
        assert!(rows.next_row().unwrap().is_none());
    }

    #[test]
    fn null_round_trip() {
        let Some(mut db) = connect() else {
            return;
        };
        db.execute("CREATE TEMP TABLE nulls (n BIGINT)", &[])
            .unwrap();
        db.execute("INSERT INTO nulls VALUES ($1)", &[Value::Null])
            .unwrap();
        let mut rows = db.query("SELECT n FROM nulls", &[]).unwrap();
        let row = rows.next_row().unwrap().unwrap();
        assert_eq!(row.get("n"), Some(&Value::Null));
    }

    #[test]
    fn prepared_statement_reuse() {
        let Some(mut db) = connect() else {
            return;
        };
        db.execute("CREATE TEMP TABLE reuse (v BIGINT)", &[])
            .unwrap();
        let mut stmt = db.prepare("INSERT INTO reuse VALUES ($1)").unwrap();
        for v in 1..=3 {
            stmt.execute(&[Value::Int(v)]).unwrap();
        }
        let mut rows = db.query("SELECT count(*) c FROM reuse", &[]).unwrap();
        let row = rows.next_row().unwrap().unwrap();
        assert_eq!(row.get("c"), Some(&Value::Int(3)));
    }

    #[test]
    fn delete_returns_affected_count() {
        let Some(mut db) = connect() else {
            return;
        };
        db.execute("CREATE TEMP TABLE del_test (v BIGINT)", &[])
            .unwrap();
        db.execute("INSERT INTO del_test VALUES (1), (2), (3)", &[])
            .unwrap();
        let n = db
            .execute("DELETE FROM del_test WHERE v > $1", &[Value::Int(1)])
            .unwrap();
        assert_eq!(n, 2);
    }

    #[test]
    fn transaction_commit() {
        let Some(mut db) = connect() else {
            return;
        };
        db.execute("CREATE TEMP TABLE tx_commit (val BIGINT)", &[])
            .unwrap();
        let mut tx = db.begin().unwrap();
        tx.execute("INSERT INTO tx_commit VALUES (42)").unwrap();
        tx.commit().unwrap();
        let mut rows = db.query("SELECT val FROM tx_commit", &[]).unwrap();
        let row = rows.next_row().unwrap().unwrap();
        assert_eq!(row.get("val"), Some(&Value::Int(42)));
    }

    #[test]
    fn transaction_rollback() {
        let Some(mut db) = connect() else {
            return;
        };
        db.execute("CREATE TEMP TABLE tx_rollback (val BIGINT)", &[])
            .unwrap();
        let mut tx = db.begin().unwrap();
        tx.execute("INSERT INTO tx_rollback VALUES (99)").unwrap();
        tx.rollback().unwrap();
        let mut rows = db.query("SELECT val FROM tx_rollback", &[]).unwrap();
        assert!(rows.next_row().unwrap().is_none());
    }

    #[test]
    fn transaction_drop_rolls_back() {
        let Some(mut db) = connect() else {
            return;
        };
        db.execute("CREATE TEMP TABLE tx_drop (val BIGINT)", &[])
            .unwrap();
        {
            let mut tx = db.begin().unwrap();
            tx.execute("INSERT INTO tx_drop VALUES (7)").unwrap();
            // tx dropped without commit → implicit rollback
        }
        let mut rows = db.query("SELECT val FROM tx_drop", &[]).unwrap();
        assert!(rows.next_row().unwrap().is_none());
    }

    #[test]
    fn query_returns_columns_for_empty_result() {
        let Some(mut db) = connect() else {
            return;
        };
        db.execute("CREATE TEMP TABLE empty_col (a BIGINT, b TEXT)", &[])
            .unwrap();
        let rows = db.query("SELECT a, b FROM empty_col", &[]).unwrap();
        assert_eq!(rows.columns(), &["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn savepoint_round_trip() {
        let Some(mut db) = connect() else {
            return;
        };
        db.execute("CREATE TEMP TABLE sp (v BIGINT)", &[]).unwrap();
        let mut tx = db.begin().unwrap();
        tx.execute("INSERT INTO sp VALUES (1)").unwrap();
        tx.savepoint("sp1").unwrap();
        tx.execute("INSERT INTO sp VALUES (2)").unwrap();
        tx.rollback_to_savepoint("sp1").unwrap();
        tx.release_savepoint("sp1").unwrap();
        tx.commit().unwrap();
        let mut rows = db.query("SELECT count(*) c FROM sp", &[]).unwrap();
        let row = rows.next_row().unwrap().unwrap();
        assert_eq!(row.get("c"), Some(&Value::Int(1)));
    }

    #[test]
    fn isolation_level_serializable() {
        let Some(mut db) = connect() else {
            return;
        };
        db.execute("CREATE TEMP TABLE iso (v BIGINT)", &[]).unwrap();
        let mut tx = db.begin_with(IsolationLevel::Serializable).unwrap();
        tx.execute("INSERT INTO iso VALUES (1)").unwrap();
        tx.commit().unwrap();
    }

    #[test]
    fn ping_succeeds() {
        let Some(mut db) = connect() else {
            return;
        };
        db.ping().unwrap();
    }

    #[test]
    fn closed_connection_rejects_use() {
        let url = match std::env::var("DATABASE_URL") {
            Ok(url) => url,
            Err(_) => return,
        };
        let mut conn = PostgresDriver.open(&url).unwrap();
        conn.close().unwrap();
        assert!(matches!(conn.ping(), Err(Error::Closed)));
        assert!(matches!(conn.prepare("SELECT 1"), Err(Error::Closed)));
    }

    #[test]
    fn constraint_violation_classified() {
        let Some(mut db) = connect() else {
            return;
        };
        db.execute("CREATE TEMP TABLE uniq (id BIGINT PRIMARY KEY)", &[])
            .unwrap();
        db.execute("INSERT INTO uniq VALUES ($1)", &[Value::Int(1)])
            .unwrap();
        let err = db
            .execute("INSERT INTO uniq VALUES ($1)", &[Value::Int(1)])
            .unwrap_err();
        match err {
            Error::Driver { driver, message } => {
                assert_eq!(driver, "postgres");
                assert!(message.starts_with("[23505]"), "message: {message}");
            }
            other => panic!("expected Error::Driver, got {other:?}"),
        }
    }

    #[test]
    fn streaming_drains_large_result() {
        let Some(mut db) = connect() else {
            return;
        };
        let mut rows = db
            .query("SELECT generate_series(1, 5000) AS n", &[])
            .unwrap();
        let mut count = 0_i64;
        while let Some(row) = rows.next_row().unwrap() {
            count += 1;
            assert_eq!(row.get("n"), Some(&Value::Int(count)));
        }
        assert_eq!(count, 5000);
    }

    #[test]
    fn copy_round_trip() {
        let Some(mut conn) = open_raw() else {
            return;
        };
        exec_raw(&mut conn, "CREATE TEMP TABLE copy_t (a BIGINT, b TEXT)");
        let data = b"1\talice\n2\tbob\n";
        let written = conn.copy_in("COPY copy_t (a, b) FROM STDIN", data).unwrap();
        assert_eq!(written, 2);
        let out = conn.copy_out("COPY copy_t TO STDOUT").unwrap();
        assert_eq!(out, data);
    }

    #[test]
    fn listen_notify_round_trip() {
        let Some(mut listener) = open_raw() else {
            return;
        };
        let Some(mut notifier) = open_raw() else {
            return;
        };
        listener.listen("pgooseql_test_chan").unwrap();
        let mut notify = notifier
            .prepare("SELECT pg_notify('pgooseql_test_chan', 'hello')")
            .unwrap();
        let mut rows = notify.query(&[]).unwrap();
        rows.next_row().unwrap();
        let n = listener
            .poll_notification(5000)
            .unwrap()
            .expect("notification within timeout");
        assert_eq!(n.channel, "pgooseql_test_chan");
        assert_eq!(n.payload, "hello");
        assert!(n.process_id > 0);
        listener.unlisten("pgooseql_test_chan").unwrap();
        assert!(listener.poll_notification(0).unwrap().is_none());
    }

    #[test]
    fn transaction_params_round_trip() {
        let Some(mut conn) = open_raw() else {
            return;
        };
        exec_raw(&mut conn, "CREATE TEMP TABLE txp (id BIGINT, name TEXT)");
        let mut tx = conn.begin().unwrap();
        let written = tx
            .execute_params(
                "INSERT INTO txp VALUES ($1, $2)",
                &[Value::Int(1), Value::Text("ada".into())],
            )
            .unwrap();
        assert_eq!(written, 1);
        let mut rows = tx
            .query_params("SELECT name FROM txp WHERE id = $1", &[Value::Int(1)])
            .unwrap();
        let row = rows.next_row().unwrap().unwrap();
        assert_eq!(row, vec![Value::Text("ada".into())]);
        assert!(rows.next_row().unwrap().is_none());
        drop(rows);
        tx.commit().unwrap();
    }

    #[test]
    fn rich_types_decode_to_text() {
        let Some(mut db) = connect() else {
            return;
        };
        let mut rows = db
            .query(
                "SELECT 1.5::numeric AS n, now()::timestamptz AS ts, \
                 gen_random_uuid() AS u, '{\"a\":1}'::jsonb AS j, ARRAY[1,2,3] AS arr",
                &[],
            )
            .unwrap();
        let row = rows.next_row().unwrap().unwrap();
        assert_eq!(row.get("n"), Some(&Value::Text("1.5".into())));
        match row.get("ts") {
            Some(Value::Text(ts)) => assert!(ts.starts_with("20"), "ts: {ts}"),
            other => panic!("expected timestamptz text, got {other:?}"),
        }
        match row.get("u") {
            Some(Value::Text(u)) => assert_eq!(u.len(), 36, "uuid: {u}"),
            other => panic!("expected uuid text, got {other:?}"),
        }
        assert_eq!(row.get("j"), Some(&Value::Text("{\"a\":1}".into())));
        assert_eq!(row.get("arr"), Some(&Value::Text("[1,2,3]".into())));
    }
}

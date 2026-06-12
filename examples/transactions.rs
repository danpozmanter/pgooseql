//! Transactions: commit, rollback, savepoints, isolation levels, and
//! the drop-means-rollback guarantee.
//!
//! Run with a reachable PostgreSQL server:
//! ```sh
//! DATABASE_URL="host=localhost user=me dbname=mydb" cargo run --example transactions
//! ```

use std::sync::Arc;

use gossamer_std::database::sql::{self, Conn, Error, IsolationLevel, Value};
use pgooseql::PostgresDriver;

fn count(db: &mut Conn) -> Result<i64, Error> {
    let mut rows = db.query("SELECT count(*) AS n FROM ledger", &[])?;
    match rows.next_row()? {
        Some(row) => row.get_i64("n"),
        None => Ok(0),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .map_err(|_| "set DATABASE_URL, e.g. host=localhost user=me dbname=mydb")?;

    sql::register(Arc::new(PostgresDriver));
    let mut db = sql::open("postgres", &url)?;
    db.execute("CREATE TEMP TABLE ledger (entry BIGINT)", &[])?;

    // Commit makes the insert visible.
    let mut tx = db.begin()?;
    tx.execute("INSERT INTO ledger VALUES (1)")?;
    tx.commit()?;
    println!("after commit: {}", count(&mut db)?);

    // Rollback discards it.
    let mut tx = db.begin()?;
    tx.execute("INSERT INTO ledger VALUES (2)")?;
    tx.rollback()?;
    println!("after rollback: {}", count(&mut db)?);

    // Dropping an unfinished transaction rolls back implicitly.
    {
        let mut tx = db.begin()?;
        tx.execute("INSERT INTO ledger VALUES (3)")?;
    }
    println!("after drop: {}", count(&mut db)?);

    // Savepoints give partial rollback inside one transaction.
    let mut tx = db.begin()?;
    tx.execute("INSERT INTO ledger VALUES (4)")?;
    tx.savepoint("before_risky")?;
    tx.execute("INSERT INTO ledger VALUES (5)")?;
    tx.rollback_to_savepoint("before_risky")?;
    tx.release_savepoint("before_risky")?;
    tx.commit()?;
    println!("after savepoint dance: {}", count(&mut db)?);

    // Stricter isolation when concurrent writers must serialize.
    let mut tx = db.begin_with(IsolationLevel::Serializable)?;
    tx.execute("INSERT INTO ledger VALUES (6)")?;
    tx.commit()?;
    println!("after serializable commit: {}", count(&mut db)?);

    let mut rows = db.query("SELECT entry FROM ledger ORDER BY entry", &[])?;
    print!("ledger entries:");
    while let Some(row) = rows.next_row()? {
        print!(" {}", row.get_i64("entry")?);
    }
    println!();

    // Parameterized cleanup inside a transaction uses the connection's
    // prepared path; Tx::execute is for raw SQL.
    db.execute("DELETE FROM ledger WHERE entry > $1", &[Value::Int(1)])?;
    println!("after cleanup: {}", count(&mut db)?);

    db.close()?;
    Ok(())
}

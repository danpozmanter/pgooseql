//! Queries, inserts, updates, and deletes through the Gossamer SQL façade.
//!
//! Run with a reachable PostgreSQL server:
//! ```sh
//! DATABASE_URL="host=localhost user=me dbname=mydb" cargo run --example crud
//! ```

use std::sync::Arc;

use gossamer_std::database::sql::{self, Value};
use pgooseql::PostgresDriver;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .map_err(|_| "set DATABASE_URL, e.g. host=localhost user=me dbname=mydb")?;

    sql::register(Arc::new(PostgresDriver));
    let mut db = sql::open("postgres", &url)?;

    // TEMP TABLE: visible only to this connection, dropped on close.
    db.execute(
        "CREATE TEMP TABLE players (id BIGINT PRIMARY KEY, name TEXT NOT NULL, score BIGINT NOT NULL)",
        &[],
    )?;

    // Insert with positional $N parameters; execute returns rows affected.
    let inserted = db.execute(
        "INSERT INTO players VALUES ($1, $2, $3), ($4, $5, $6)",
        &[
            Value::Int(1),
            Value::Text("alice".into()),
            Value::Int(90),
            Value::Int(2),
            Value::Text("bob".into()),
            Value::Int(70),
        ],
    )?;
    println!("inserted {inserted} rows");

    // Prepare once, execute many — the statement is server-side prepared.
    let mut insert = db.prepare("INSERT INTO players VALUES ($1, $2, $3)")?;
    for (id, name, score) in [(3, "carol", 80), (4, "dave", 60)] {
        insert.execute(&[Value::Int(id), Value::Text(name.into()), Value::Int(score)])?;
    }

    // Query with parameters; walk rows with typed getters.
    let mut rows = db.query(
        "SELECT id, name, score FROM players WHERE score >= $1 ORDER BY score DESC",
        &[Value::Int(75)],
    )?;
    println!("score >= 75:");
    while let Some(row) = rows.next_row()? {
        println!(
            "  #{} {} -> {}",
            row.get_i64("id")?,
            row.get_text("name")?,
            row.get_i64("score")?
        );
    }

    // Update with affected count.
    let updated = db.execute(
        "UPDATE players SET score = score + $1 WHERE score < $2",
        &[Value::Int(15), Value::Int(75)],
    )?;
    println!("boosted {updated} low scores");

    // Delete with affected count.
    let deleted = db.execute("DELETE FROM players WHERE score < $1", &[Value::Int(80)])?;
    println!("deleted {deleted} rows");

    let mut rows = db.query("SELECT count(*) AS remaining FROM players", &[])?;
    if let Some(row) = rows.next_row()? {
        println!("remaining: {}", row.get_i64("remaining")?);
    }

    db.close()?;
    Ok(())
}

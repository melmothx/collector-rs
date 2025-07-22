use std::env;
use std::collections::HashMap;
use axum::{
    extract::{State, Query},
    routing::get,
    http::StatusCode,
    Router,
    Json,
};
use tokio_postgres::NoTls;
use bb8::{Pool};
use bb8_postgres::PostgresConnectionManager;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use serde::{Deserialize, Serialize};

type ConnectionPool = Pool<PostgresConnectionManager<NoTls>>;

#[derive(Serialize, Debug)]
struct Entry {
    entry_id: i32,
    rank: f32,
    title: String,
}

async fn search(
    State(pool): State<ConnectionPool>,
    Query(params): Query<HashMap<String, String>>,
) -> (StatusCode, Json<Vec<Entry>>) {
    let conn = pool.get().await.expect("Failed to get a connection from the pool");
    let sql = r#"
SELECT entry_id, title, ts_rank_cd(search_vector, query) AS rank
FROM entry, websearch_to_tsquery($1) query
WHERE search_vector @@ query
ORDER BY rank DESC
LIMIT 10;

"#;
    let query = match params.get("query") {
        Some(value) => value,
        None => "",
    };
    let out = conn.query(sql, &[&query]).await.expect("Query failed")
        .iter().map(|row|
                    Entry {
                        entry_id: row.get(0),
                        title: row.get(1),
                        rank: row.get(2),
                    }).collect();
    dbg!("{:?}", &out);
    (StatusCode::OK, Json(out))
}

#[tokio::main]
async fn main() {
    // Setup database connection pool
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("{}=debug", env!("CARGO_CRATE_NAME")).into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let pg_dsn = env::var("DATABASE_URL").expect("DATABASE_URL env variable should be set");
    let manager = PostgresConnectionManager::new_from_stringlike(&pg_dsn, NoTls)
        .expect("Failed to create connection manager");
    let pool = Pool::builder()
        .max_size(16)
        .build(manager)
        .await
        .expect("Failed to build pool");

    // Create the axum router
    let app = Router::new()
        .route("/search", get(search))
        .with_state(pool);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .expect("cannot bind to 3000");
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

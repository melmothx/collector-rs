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

#[derive(Serialize, Debug)]
struct Facet {
    count: i64,
    term: String,
    id: String,
}

#[derive(Serialize, Debug)]
struct FacetList {
    name: String,
    values: Vec<Facet>,
}

#[derive(Serialize, Debug)]
struct FacetBlock {
    library: FacetList,
    creator: FacetList,
    language: FacetList,
}

#[derive(Serialize, Debug)]
struct SearchResult {
    entries: Vec<Entry>,
    facets: FacetBlock,
}

async fn search(
    State(pool): State<ConnectionPool>,
    Query(params): Query<HashMap<String, String>>,
) -> (StatusCode, Json<SearchResult>) {
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
    let out = conn.query(sql, &[&query]).await.expect("Query should be valid")
        .iter().map(|row|
                    Entry {
                        entry_id: row.get(0),
                        title: row.get(1),
                        rank: row.get(2),
                    }).collect();
    tracing::debug!("{:?}", &out);

    let lang_sql = r#"
SELECT count(*) AS count,
       COALESCE(l.native_name, l.english_name, l.language_code) AS term,
       l.language_code AS id
FROM entry e
JOIN entry_language el ON el.entry_id = e.entry_id
JOIN known_language l ON l.language_code = el.language_code
WHERE websearch_to_tsquery($1) @@ e.search_vector
GROUP BY l.language_code, l.native_name, l.english_name
ORDER BY count(*) DESC
"#;
    let langs = conn.query(lang_sql, &[&query]).await.expect("Query should be valid")
        .iter().map(|row|
                    Facet {
                        count: row.get(0),
                        term: row.get(1),
                        id: row.get(2),
                    }).collect();

    let authors_sql = r#"
SELECT count(*) AS count,
       a.full_name AS term,
       a.agent_id::TEXT AS id
FROM entry e
JOIN entry_agent ea ON ea.entry_id = e.entry_id
JOIN agent a ON a.agent_id = ea.agent_id
WHERE websearch_to_tsquery($1) @@ e.search_vector
GROUP BY a.full_name, a.agent_id
ORDER BY count(*) DESC
"#;
    let authors = conn.query(authors_sql, &[&query]).await.expect("Query should be valid")
        .iter().map(|row|
                    Facet {
                        count: row.get(0),
                        term: row.get(1),
                        id: row.get(2),
                    }).collect();

    let libraries_sql = r#"
SELECT count(*) AS count,
       l.name AS term,
       l.library_id::TEXT AS id
FROM entry e
JOIN datasource ds ON e.entry_id = ds.entry_id
JOIN site s ON s.site_id = ds.site_id
JOIN library l ON s.library_id = l.library_id
WHERE websearch_to_tsquery($1) @@ e.search_vector
GROUP BY l.name, l.library_id
ORDER BY count(*) DESC
"#;

    let libraries = conn.query(libraries_sql, &[&query]).await.expect("Query should be valid")
        .iter().map(|row|
                    Facet {
                        count: row.get(0),
                        term: row.get(1),
                        id: row.get(2),
                    }).collect();



    (StatusCode::OK, Json(SearchResult {
        entries: out,
        facets: FacetBlock {
            language: FacetList {
                name: String::from("language"),
                values: langs,
            },
            creator: FacetList {
                name: String::from("creator"),
                values: authors,
            },
            library: FacetList {
                name: String::from("libraryr"),
                values: libraries,
            },
        }
    }))
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

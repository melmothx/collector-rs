use reqwest::{Error as ReqwestError};
use std::sync::Arc;
use tokio::sync::Mutex;
use futures::future::join_all;
use tokio_postgres::{NoTls, Client};
use tokio;
use std::env;
use crate::oai::pmh;

mod oai;
use oai::pmh::download_all;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pg_dsn = env::var("PG_DSN").unwrap();
    let (client, connection) = tokio_postgres::connect(&pg_dsn, NoTls).await?;
    tokio::spawn(connection);
    let client = Arc::new(Mutex::new(client));
    let sql = r#"
SELECT id, library_id, url FROM collector_site WHERE url <> '' ORDER BY url
"#;
    let rows = client.lock().await.query(sql, &[]).await?;
    let urls: Vec<(i64, i64, String)> = rows.iter().map(|row|
                                                        (row.get(0),
                                                         row.get(1),
                                                         row.get(2))).collect();
    // dbg!("{:#?}", urls);
    let mut tasks = Vec::new();
    for todo in urls {
        let (site_id, library_id, url) = todo;
        let client = Arc::clone(&client);
        let task = tokio::spawn(async move {
            download_all(&url).await;
        });
        tasks.push(task);
    }
    join_all(tasks).await;
    Ok(())
}

async fn download_url(
    url: &str
)-> Result<(u16, String), ReqwestError> {
    let res = reqwest::get(url).await?;
    let status = res.status().as_u16();
    let content = res.text().await?;
    Ok((status, content))
}

async fn insert_dl_result(client: &Arc<Mutex<Client>>, status: u16, site_id: i64, library_id: i64, content: &str)
                          -> Result<i32, Box<dyn std::error::Error>> {
    let client = client.lock().await;
    let sql = r#"
INSERT INTO requests (status, site_id, library_id, content)
VALUES ($1, $2, $3, $4)
RETURNING id
"#;
    let rows = client.query(sql, &[&(status as i32), &site_id, &library_id, &content]).await?;
    match rows.first().map(|row| row.get(0)) {
        Some(id) => Ok(id),
        None => Err(String::from("No id created").into()),
    }
}

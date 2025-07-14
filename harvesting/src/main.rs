use std::sync::Arc;
use tokio::sync::Mutex;
use futures::future::join_all;
use tokio_postgres::{NoTls, Client};
use tokio;
use std::env;
mod oai;
use oai::pmh::HarvestParams;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pg_dsn = env::var("DATABASE_URL").expect("DATABASE_URL env variable should be set");
    let (client, connection) = tokio_postgres::connect(&pg_dsn, NoTls).await?;
    tokio::spawn(connection);
    let client = Arc::new(Mutex::new(client));
    let sql = r#"
SELECT url, oai_metadata_format, oai_set, last_harvested, site_id, library_id
FROM site
WHERE url <> ''
      AND site_type IN ('amusewiki', 'generic', 'koha-marc21', 'koha-unimarc')
ORDER BY url
"#;
    let rows = client.lock().await.query(sql, &[]).await?;
    let urls: Vec<(HarvestParams, i32, i32)> = rows.iter().map(|row|
                                                               (HarvestParams {
                                                                   base_url: row.get(0),
                                                                   metadata_prefix: row.get(1),
                                                                   set: row.get(2),
                                                                   from: row.get(3),
                                                               },
                                                                row.get(4),
                                                                row.get(5)
                                                               )).collect();
    dbg!("{:#?}", &urls);
    let mut tasks = Vec::new();
    for todo in urls {
        let (params, site_id, library_id) = todo;
        let client = Arc::clone(&client);
        let task = tokio::spawn(async move {
            if let Ok(results) = oai::pmh::harvest(params).await {
                for res in &results {
                    println!("{} {} {} {}", res.oai_pmh_identifier(), res.datestamp(), res.title(), res.subtitle());
                    println!("{:?}, {:?}", res.authors(), res.languages());
                }
            }
        });
        tasks.push(task);
    }
    join_all(tasks).await;
    Ok(())
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

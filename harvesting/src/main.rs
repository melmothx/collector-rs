use std::sync::Arc;
use tokio::sync::Mutex;
use futures::future::join_all;
use tokio_postgres::{NoTls, Client};
use tokio;
use std::env;
mod oai;
mod mycorrhiza;
use oai::pmh::{HarvestParams,HarvestedRecord,SiteType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pg_dsn = env::var("DATABASE_URL").expect("DATABASE_URL env variable should be set");
    let (client, connection) = tokio_postgres::connect(&pg_dsn, NoTls).await?;
    tokio::spawn(connection);
    let client = Arc::new(Mutex::new(client));
    let sql = r#"
SELECT url, site_type, last_harvested, site_id, library_id
FROM site
WHERE url <> '' AND site_type IN ('amusewiki', 'koha-marc21', 'koha-unimarc')
ORDER BY url
"#;
    let rows = client.lock().await.query(sql, &[]).await?;
    let urls: Vec<HarvestParams> = rows.iter().map(|row| HarvestParams {
        base_url: row.get(0),
        site_type: match row.get(1) {
            "amusewiki" => SiteType::Amusewiki,
            "koha-marc21" => SiteType::KohaMarc21,
            "koha-unimarc" => SiteType::KohaUnimarc,
            _ => panic!("Invalid site_type"),
        },
        from: row.get(2),
        site_id: row.get(3),
        library_id: row.get(4),
    }).collect();
    dbg!("{:#?}", &urls);
    let mut tasks = Vec::new();
    for todo in urls {
        let client = Arc::clone(&client);
        let task = tokio::spawn(async move {
            let results = oai::pmh::harvest(&todo).await;
            for res in results {
                match mycorrhiza::insert_harvested_record(&client, &todo, &res).await {
                    Ok(return_id) => (),
                    Err(e) => eprintln!("Error inserting record for {:?}: {:?}", res, e),
                }
            }
        });
        tasks.push(task);
    }
    join_all(tasks).await;
    Ok(())
}


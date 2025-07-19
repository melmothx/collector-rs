use std::sync::Arc;
use tokio::sync::Mutex;
use futures::future::join_all;
use tokio_postgres::{NoTls, Client};
use tokio;
use std::env;
mod oai;
use oai::pmh::{HarvestParams,HarvestedRecord};

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
    let urls: Vec<HarvestParams> = rows.iter().map(|row| HarvestParams {
        base_url: row.get(0),
        metadata_prefix: row.get(1),
        set: row.get(2),
        from: row.get(3),
        site_id: row.get(4),
        library_id: row.get(5),
    }).collect();
    dbg!("{:#?}", &urls);
    let mut tasks = Vec::new();
    for todo in urls {
        let client = Arc::clone(&client);
        let task = tokio::spawn(async move {
            let results = oai::pmh::harvest(&todo).await;
            for res in results {
                match insert_harvested_record(&client, &todo, &res).await {
                    Ok(return_id) => println!("Inserted/Updated row with URL ID: {}", return_id),
                    Err(e) => eprintln!("Error inserting status code for {:?}: {:?}", res, e),
                }
            }
        });
        tasks.push(task);
    }
    join_all(tasks).await;
    Ok(())
}

async fn insert_harvested_record(client: &Arc<Mutex<Client>>,
                                 params: &HarvestParams,
                                 res: &HarvestedRecord)
                                 -> Result<i32, Box<dyn std::error::Error>> {
    // println!("{:?}", res.uri());
    println!("{:?} {} {} {} {} {}",
             params,
             res.identifier(),
             res.oai_pmh_identifier(), res.datestamp(), res.title(), res.subtitle());
    println!("{:?}, {:?}", res.authors(), res.languages());
    println!("{} | {:?} | {:?} | {} | {} | {} | {} | {} | {} | {}",
             res.description(),
             res.edition_years(),
             res.uri(),
             res.publisher(),
             res.isbn(),
             res.material_description(),
             res.shelf_location_code(),
             res.edition_statement(),
             res.place_date_of_publication_distribution(),
             res.is_aggregation(),
    );
    let client = client.lock().await;
    let sql = r#"
INSERT INTO entry (title, checksum)
VALUES ($1, $2)
RETURNING entry_id
"#;
    let rows = client.query(sql, &[&res.title(), &"test"]).await?;
    match rows.first().map(|row| row.get(0)) {
        Some(id) => Ok(id),
        None => Err(String::from("No id created").into()),
    }
}

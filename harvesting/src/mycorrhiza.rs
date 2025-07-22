use std::sync::Arc;
use tokio::sync::Mutex;
use crate::oai::pmh::{HarvestParams,HarvestedRecord,SiteType};
use tokio_postgres::{Client};
use unicode_normalization::UnicodeNormalization;
use unicode_categories::UnicodeCategories;
use chrono::{DateTime, Utc};
use tokio_postgres;


fn strip_diacritics(s: &str) -> String {
    s.nfkd().filter(|c| !c.is_mark_nonspacing()).collect()
}

pub async fn insert_harvested_record(client: &Arc<Mutex<Client>>,
                                     params: &HarvestParams,
                                     res: &HarvestedRecord)
                                     -> Result<i32, Box<dyn std::error::Error>> {
    // println!("{:?}", res.uri());
//    println!("{:?} {} {} {} {} {}",
//             params,
//             res.identifier(),
//             res.oai_pmh_identifier(), res.datestamp(), res.title(), res.subtitle());
//    println!("{:?}, {:?}, {}", res.authors(), res.languages(), res.checksum());
//    println!("{} | {:?} | {:?} | {} | {} | {} | {} | {} | {} | {:?}",
//             res.description(),
//             res.edition_years(),
//             res.uri(),
//             res.publisher(),
//             res.isbn(),
//             res.material_description(),
//             res.shelf_location_code(),
//             res.edition_statement(),
//             res.place_date_of_publication_distribution(),
//             res.aggregations(),
//    );
    let sql = r#"
INSERT INTO entry (title, subtitle, checksum, search_text)
VALUES ($1, $2, $3, $4)
ON CONFLICT (checksum)
DO UPDATE SET last_indexed = NOW()
RETURNING entry_id
"#;
    let title = res.title();
    let subtitle = res.subtitle();
    let search_text = [&title, &subtitle].iter().map(|s| strip_diacritics(s)).collect::<Vec<String>>().join(" ");
    let rows = client.lock().await.query(sql,
                       &[&title,
                         &subtitle,
                         &res.checksum(),
                         &search_text,
                       ]).await?;
    match rows.first().map(|row| row.get(0)) {
        Some(entry_id) => {
            match insert_agents(client, params, res, entry_id).await {
                Ok(()) => (),
                Err(e) => println!("Got {e:?} while inserting agents")
            };
            match insert_languages(client, params, res, entry_id).await {
                Ok(()) => (),
                Err(e) => println!("Got {e:?} while inserting languages")
            };
            match insert_datasource(client, params, res, entry_id).await {
                Ok(()) => (),
                Err(e) => println!("Got {e:?} while inserting datasource")
            };
            Ok(entry_id)
        },
        None => Err(String::from("No id created").into()),
    }
}

async fn insert_agents(client: &Arc<Mutex<Client>>,
                       params: &HarvestParams,
                       res: &HarvestedRecord,
                       entry_id: i32)
                       -> Result<(), Box<dyn std::error::Error>> {
    let c = client.lock().await;
    let sql_agent = r#"
INSERT INTO agent (full_name, search_text)
VALUES ($1, $2)
ON CONFLICT (full_name)
DO UPDATE SET last_modified = NOW() -- needed so we return the id
RETURNING agent_id
"#;
    let sql_bridge = r#"
INSERT INTO entry_agent (entry_id, agent_id)
VALUES ($1, $2)
ON CONFLICT DO NOTHING
"#;
    for full_name in res.authors() {
        let row = c.query(sql_agent, &[&full_name, &strip_diacritics(&full_name)]).await?;
        match row.first().map(|row| row.get::<_, i32>(0)) {
            Some(agent_id) => {
                c.query(sql_bridge, &[&entry_id, &agent_id]).await?;
            },
            None => println!("No agent id returned"),
        };
    }
    Ok(())
}

async fn insert_languages(client: &Arc<Mutex<Client>>,
                          params: &HarvestParams,
                          res: &HarvestedRecord,
                          entry_id: i32)
                          -> Result<(), Box<dyn std::error::Error>> {
    let c = client.lock().await;
    let sql_lang = r#"
INSERT INTO known_language (language_code)
VALUES ($1)
ON CONFLICT DO NOTHING
"#;
    let sql_bridge = r#"
INSERT INTO entry_language (entry_id, language_code)
VALUES ($1, $2)
ON CONFLICT DO NOTHING
"#;

    for lang in res.languages() {
        c.query(sql_lang, &[&lang]).await?;
        c.query(sql_bridge, &[&entry_id, &lang]).await?;
    }
    Ok(())
}
async fn insert_datasource(client: &Arc<Mutex<Client>>,
                           params: &HarvestParams,
                           res: &HarvestedRecord,
                           entry_id: i32)
                           -> Result<(), Box<dyn std::error::Error>> {
    let sql_datasource = r#"
INSERT INTO datasource (
  site_id,
  oai_pmh_identifier,
  entry_id,
  datestamp,
  description,
  year_edition,
  year_first_edition,
  publisher,
  isbn,
  uri,
  uri_label,
  content_type,
  material_description,
  shelf_location_code,
  edition_statement,
  place_date_of_publication_distribution,
  search_text
)
VALUES (
  $1, $2, $3, $4, $5,
  $6, $7, $8, $9, $10,
  $11, $12, $13, $14, $15,
  $16, $17
)
ON CONFLICT (site_id, oai_pmh_identifier)
DO UPDATE SET
entry_id = EXCLUDED.entry_id,
datestamp = EXCLUDED.datestamp,
description = EXCLUDED.description,
year_edition = EXCLUDED.year_edition,
year_first_edition = EXCLUDED.year_first_edition,
publisher = EXCLUDED.publisher,
isbn = EXCLUDED.isbn,
uri = EXCLUDED.uri,
uri_label = EXCLUDED.uri_label,
content_type = EXCLUDED.content_type,
material_description = EXCLUDED.material_description,
shelf_location_code = EXCLUDED.shelf_location_code,
edition_statement = EXCLUDED.edition_statement,
place_date_of_publication_distribution = EXCLUDED.place_date_of_publication_distribution,
search_text = EXCLUDED.search_text,
last_modified = NOW()
RETURNING datasource_id
"#;
    let mut full_text = String::from("");
    if let Ok(body) = res.full_text().await {
        full_text = strip_diacritics(&body);
    };
    let mut year_edition = None;
    let mut year_first_edition = None;
    let years = res.edition_years();
    if years.len() == 1 {
        year_edition = years.first();
    }
    else if years.len() > 1 {
        year_first_edition = years.first();
        year_edition = years.last();
    }
    let mut uri = None;
    let mut uri_label = None;
    let mut content_type = None;
    if let Some(uri_struct) = res.uri() {
        uri = Some(uri_struct.uri);
        uri_label = Some(uri_struct.uri_label);
        content_type = Some(uri_struct.content_type);
    }
    let datestamp_string = res.datestamp();
    let datestamp = match datestamp_string.parse::<DateTime<Utc>>() {
        Ok(t) => {
            println!("{datestamp_string} => {t}");
            t
        }
        Err(e) => {
            eprintln!("Error parsing timestamp: {e}");
            Utc::now()
        }
    };
    let rows = client.lock().await.query(sql_datasource, &[
        &params.site_id,
        &res.oai_pmh_identifier(),
        &entry_id,
        &datestamp,
        &res.description(),
        &year_edition,
        &year_first_edition,
        &res.publisher(),
        &res.isbn(),
        &uri,
        &uri_label,
        &content_type,
        &res.material_description(),
        &res.shelf_location_code(),
        &res.edition_statement(),
        &res.place_date_of_publication_distribution(),
        &full_text,
    ]).await?;
    match rows.first().map(|row| row.get::<_, i32>(0)) {
        Some(datasource_id) => Ok(()),
        None => Err(String::from("No id created").into()),
    }
}

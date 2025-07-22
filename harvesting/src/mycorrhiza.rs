use std::sync::Arc;
use tokio::sync::Mutex;
use crate::oai::pmh::{HarvestParams,HarvestedRecord,SiteType};
use tokio_postgres::{Client};
use unicode_normalization::UnicodeNormalization;
use unicode_categories::UnicodeCategories;

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
    if let Ok(full_text) = res.full_text().await {
        
    };
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
    
    Ok(())
}

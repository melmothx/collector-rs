use reqwest;
use serde::Deserialize;
use quick_xml::de::from_str;
use chrono::{DateTime, Utc};
use url::Url;
use std::time::SystemTime;

#[derive(Debug, Deserialize)]
struct ResponseError {
    #[serde(rename = "@code")]
    code: String,
    #[serde(rename = "$text")]
    message: String,
}


#[derive(Debug, Deserialize)]
pub struct OaiPmhRecordHeader {
    identifier: String,
    datestamp: String,
    #[serde(rename = "@status")]
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct OaiPmhRecordMetadata {
    #[serde(rename = "record")]
    record: MarcRecord,
}

#[derive(Debug, Deserialize)]
pub struct MarcDataField {
    #[serde(rename = "@tag")]
    tag: String,
    #[serde(rename = "@ind1")]
    ind1: String,
    #[serde(rename = "@ind2")]
    ind2: String,
    #[serde(rename = "subfield")]
    subfield: Vec<MarcSubField>,
}

#[derive(Debug, Deserialize)]
pub struct MarcSubField {
    #[serde(rename = "@code")]
    code: String,
    #[serde(rename = "$text")]
    text: String,
}

#[derive(Debug, Deserialize)]
pub struct MarcRecord {
    #[serde(rename = "@xmlns")]
    namespace: String,
    leader: Option<String>,
    datafield: Vec<MarcDataField>,
}

#[derive(Debug, Deserialize)]
pub struct OaiPmhRecord {
    header: OaiPmhRecordHeader,
    metadata: OaiPmhRecordMetadata,
}

impl OaiPmhRecord {
    pub fn identifier(&self) -> &str {
        self.header.identifier.as_str()
    }
    pub fn datestamp(&self) -> &str {
        self.header.datestamp.as_str()
    }
}


#[derive(Debug, Deserialize)]
struct ListRecords {
    #[serde(rename = "resumptionToken")]
    resumption_token: Option<String>,
    #[serde(rename = "record")]
    records: Vec<OaiPmhRecord>,
}

#[derive(Debug, Deserialize)]
struct OaiPmhResponse {
    #[serde(rename = "responseDate")]
    response_date: String,
    request: String,
    error: Option<ResponseError>,
    #[serde(rename = "ListRecords")]
    list_records: Option<ListRecords>,
}

fn parse_response (xml: &str) -> OaiPmhResponse {
    match from_str(xml) {
        Ok(res) => res,
        Err(e) => OaiPmhResponse {
            response_date: String::from("NOW"),
            request: String::from("Invalid"),
            error: Some(ResponseError {
                code: String::from("Invalid XML"),
                message: e.to_string(),
            }),
            list_records: None,
        },
    }
}

async fn download_url(
    url: Url
)-> Result<OaiPmhResponse, Box<dyn std::error::Error>> {
    println!("Downloading {url}");
    let res = reqwest::get(url).await?;
    let status = res.status().as_u16();
    let content = res.text().await?;
    if status == 200 {
        Ok(parse_response(&content))
    }
    else {
        Err(format!("Status is {status}").into())
    }
}

#[derive(Debug)]
pub struct HarvestParams {
    pub base_url: String,
    pub metadata_prefix: String,
    pub set: Option<String>,
    pub from: Option<SystemTime>,
}

impl HarvestParams {
    pub fn harvest_url (&self, token: Option<&str>) -> Url {
        let mut url = Url::parse(&self.base_url).expect("base_url needs to be valid");
        url.query_pairs_mut().append_pair("verb", "ListRecords").append_pair("metadataPrefix", &self.metadata_prefix);
        match token {
            Some(token) => {
                url.query_pairs_mut().append_pair("resumptionToken", token);
            },
            None => {
                if let Some(oai_set) = &self.set {
                    url.query_pairs_mut().append_pair("set", &oai_set);
                }
                if let Some(from_date) = self.from {
                    let epoch = from_date.duration_since(SystemTime::UNIX_EPOCH).unwrap();
                    let dt: DateTime<Utc> = DateTime::from_timestamp(epoch.as_secs() as i64,
                                                                     epoch.subsec_nanos()).unwrap();
                    let zulu = dt.format("%Y-%m-%dT%H:%M:%SZ").to_string();
                    println!("Zulu for url {} is {}", &self.base_url, &zulu);
                    url.query_pairs_mut().append_pair("from", &zulu);
                }
            }
        };
        url
    }
}

pub async fn harvest(params: HarvestParams) -> Result<Vec<OaiPmhRecord>, Box<dyn std::error::Error>> {
    let mut interaction = 1;
    let mut all_records: Vec<OaiPmhRecord> = Vec::new();
    let mut url = params.harvest_url(None);
    loop {
        match download_url(url.clone()).await {
            Ok(res) => {
                match res.list_records {
                    Some(records) => {
                        println!("{} {:?}", url, records);
                        for rec in records.records {
                            all_records.push(rec)
                        }
                        if let Some(token) = records.resumption_token {
                            if token.len() > 1 {
                                interaction += 1;
                                println!("{url} download n.{interaction}");
                                url = params.harvest_url(Some(&token));
                                continue
                            }
                        } else {
                            println!("{url} download completed");
                        }
                    },
                    None => {
                        println!("{url} {res:#?}returned no record");
                    }
                }
            },
            Err(e) => println!("Error {url}: {e}"),
        };
        break
    };
    Ok(all_records)
}


use reqwest::{Error as ReqwestError};
use serde::Deserialize;
use quick_xml::de::from_str;
use url::Url;

#[derive(Debug, Deserialize)]
struct ResponseError {
    #[serde(rename = "@code")]
    code: String,
    #[serde(rename = "$text")]
    message: String,
}


#[derive(Debug, Deserialize)]
struct OaiPmhRecordHeader {
    identifier: String,
    datestamp: String,
}

#[derive(Debug, Deserialize)]
struct OaiPmhRecordMetadata {
    #[serde(rename = "record")]
    record: MarcRecord,
}

#[derive(Debug, Deserialize)]
struct MarcDataField {
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
struct MarcSubField {
    #[serde(rename = "@code")]
    code: String,
    #[serde(rename = "$text")]
    text: String,
}

#[derive(Debug, Deserialize)]
struct MarcRecord {
    #[serde(rename = "@xmlns")]
    namespace: String,
    leader: String,
    datafield: Vec<MarcDataField>,
}

#[derive(Debug, Deserialize)]
struct OaiPmhRecord {
    header: OaiPmhRecordHeader,
    metadata: OaiPmhRecordMetadata,
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
    let res = reqwest::get(url).await?;
    let status = res.status().as_u16();
    let content = res.text().await?;
    Ok(parse_response(&content))
}

pub async fn download_all(base_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut url = Url::parse(base_url)?;
    url.query_pairs_mut()
        .append_pair("verb", "ListRecords")
        .append_pair("metadataPrefix", "marc21");
    println!("Url is {url}");
    let records: Vec<OaiPmhRecord> = Vec::new();
    loop {
        match download_url(url).await {
            Ok(res) => {
                println!("{:?}", res);
                break
            },
            Err(e) => {
                break
            }
        }
    };
    Ok(())
}


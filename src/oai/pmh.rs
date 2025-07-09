use reqwest::{Error as ReqwestError};
use serde::Deserialize;
use quick_xml::de::from_str;
use url::Url;

#[derive(Debug, Deserialize)]
pub struct ResponseError {
    #[serde(rename = "@code")]
    code: String,
    #[serde(rename = "$text")]
    message: String,
}


#[derive(Debug, Deserialize)]
pub struct ListRecords {
    #[serde(rename = "resumptionToken")]
    resumption_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct OaiPmhResponse {
    #[serde(rename = "responseDate")]
    response_date: String,
    request: String,
    error: Option<ResponseError>,
    #[serde(rename = "ListRecords")]
    list_records: Option<ListRecords>,
}

pub fn parse_response (xml: &str) -> OaiPmhResponse {
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

pub async fn download_all(base_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut url = Url::parse(base_url)?;
    url.query_pairs_mut()
        .append_pair("verb", "ListRecords")
        .append_pair("metadataPrefix", "marc21");
    println!("Url is {url}");
    let res = download_url(url).await;
    println!("{:?}", res);
    Ok(())
}

async fn download_url(
    url: Url
)-> Result<OaiPmhResponse, Box<dyn std::error::Error>> {
    let res = reqwest::get(url).await?;
    let status = res.status().as_u16();
    let content = res.text().await?;
    Ok(parse_response(&content))
}

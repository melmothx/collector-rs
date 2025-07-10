use reqwest;
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
pub struct OaiPmhRecordHeader {
    identifier: String,
    datestamp: String,
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
    leader: String,
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
    // println!("Downloading {url}");
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

pub async fn harvest(base_url: &str) -> Result<Vec<OaiPmhRecord>, Box<dyn std::error::Error>> {
    let mut url = Url::parse(base_url)?;
    url.query_pairs_mut()
        .append_pair("verb", "ListRecords")
        .append_pair("metadataPrefix", "marc21");
    println!("Url is {url}");
    let mut interaction = 1;
    let mut all_records: Vec<OaiPmhRecord> = Vec::new();
    loop {
        match download_url(url.clone()).await {
            Ok(res) => {
                if let Some(records) = res.list_records {
                    for rec in records.records {
                        all_records.push(rec)
                    }
                    if let Some(token) = records.resumption_token {
                        interaction += 1;
                        println!("{base_url} download n.{interaction}");
                        url.query_pairs_mut()
                            .clear()
                            .append_pair("verb", "ListRecords")
                            .append_pair("metadataPrefix", "marc21")
                            .append_pair("resumptionToken", &token);
                        continue
                    } else {
                        println!("{base_url} download completed");
                    }
                }
            },
            Err(e) => println!("Error {base_url}: {e}"),
        };
        break
    };
    Ok(all_records)
}


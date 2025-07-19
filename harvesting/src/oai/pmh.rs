use reqwest;
use serde::Deserialize;
use quick_xml::de::from_str;
use chrono::{DateTime, Utc};
use url::Url;
use std::time::SystemTime;
use regex::Regex;
use std::collections::HashSet;

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
    #[serde(rename = "@status")]
    status: Option<String>,
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
    subfields: Vec<MarcSubField>,
}

#[derive(Debug, Deserialize)]
struct MarcSubField {
    #[serde(rename = "@code")]
    code: String,
    #[serde(rename = "$text")]
    text: String,
}

#[derive(Debug, Deserialize)]
pub struct MarcRecord {
    #[serde(rename = "@xmlns")]
    namespace: String,
    // ignore this
    // leader: Option<String>,
    #[serde(rename = "datafield")]
    datafields: Vec<MarcDataField>,
}

#[derive(Debug, Deserialize)]
pub struct OaiPmhRecord {
    header: OaiPmhRecordHeader,
    metadata: OaiPmhRecordMetadata,
}

#[derive(Debug)]
pub enum MetadataType {
    Marc21,
    UniMarc,
}

#[derive(Debug)]
pub struct RecordUri {
    uri: String,
    content_type: String,
    uri_label: String,
}

#[derive(Debug)]
pub struct HarvestedRecord {
    raw: OaiPmhRecord,
    record_type: MetadataType,
    host: String,
}

impl HarvestedRecord {
    fn new(record: OaiPmhRecord, params: &HarvestParams) -> Self {
        let base_uri = Url::parse(&params.base_url).expect("url must be valid at this point");
        let host = match base_uri.host_str() {
            Some(h) => String::from(h),
            None => String::from(""),
        };
        if params.metadata_prefix == "unimarc" {
            HarvestedRecord {
                raw: record,
                record_type: MetadataType::UniMarc,
                host,
            }
        } else {
            HarvestedRecord {
                raw: record,
                record_type: MetadataType::Marc21,
                host,
            }
        }
    }
    fn get_fields(&self, field: &str) -> Vec<&MarcDataField> {
        let rec = &self.raw.metadata.record;
        let mut out = Vec::new();
        for df in &rec.datafields {
            if df.tag == field {
                out.push(df)
            }
        }
        out
    }
    fn extract_fields(&self, field: &str, codes: Vec<&str>) -> Vec<&str> {
        let mut out = Vec::new();
        for df in self.get_fields(field) {
            for sf in &df.subfields {
                for code in &codes {
                    if &sf.code == code {
                        out.push(sf.text.as_str());
                    }
                }
            }
        }
        out
    }
    // we map these for the db
    pub fn oai_pmh_identifier(&self) -> &str {
        self.raw.header.identifier.as_str()
    }
    pub fn datestamp(&self) -> &str {
        self.raw.header.datestamp.as_str()
    }
    pub fn identifier(&self) -> String {
        match &self.record_type {
            MetadataType::Marc21 => {
                self.extract_fields("024", vec!["a"]).join(" ")
            },
            MetadataType::UniMarc => {
                self.extract_fields("090", vec!["a"]).join(" ")
            },
        }
    }
    pub fn title(&self) -> String {
        match &self.record_type {
            MetadataType::Marc21 => self.extract_fields("245", vec!["a", "b", "c"]).join(" "),
            MetadataType::UniMarc => self.extract_fields("200", vec!["a", "e"]).join(" "),
        }
    }
    pub fn subtitle(&self) -> String {
        match &self.record_type {
            MetadataType::Marc21 => self.extract_fields("246", vec!["a", "b"]).join(" "),
            MetadataType::UniMarc => String::from(""),
        }
    }
    // multiple
    pub fn authors(&self) -> Vec<&str> {
        match &self.record_type {
            MetadataType::Marc21 => self.extract_fields("100", vec!["a"]),
            MetadataType::UniMarc => self.extract_fields("200", vec!["f"]),
        }
    }
    // multiple
    pub fn languages(&self) -> Vec<&str> {
        match &self.record_type {
            MetadataType::Marc21 => {
                let mut langs = self.extract_fields("041", vec!["a"]);
                langs.extend(self.extract_fields("546", vec!["a"]));
                langs
            },
            MetadataType::UniMarc => {
                self.extract_fields("101", vec!["a"])
            },
        }
    }
    pub fn description(&self) -> String {
        match &self.record_type {
            MetadataType::Marc21 => {
                self.extract_fields("520", vec!["a"]).join(" ")
            },
            MetadataType::UniMarc => {
                let mut descs = self.extract_fields("300", vec!["a"]);
                descs.extend(self.extract_fields("330", vec!["a"]));
                descs.join(" ")
            },
        }
    }
    fn dates(&self) -> Vec<&str> {
        match &self.record_type {
            MetadataType::Marc21 => {
                let mut dates = self.extract_fields("264", vec!["c"]);
                dates.extend(self.extract_fields("363", vec!["i"]));
                dates.extend(self.extract_fields("362", vec!["a"]));
                dates
            },
            MetadataType::UniMarc => {
                self.extract_fields("210", vec!["d"])
            },
        }
    }
    pub fn edition_years(&self) -> Vec<i32> {
        let re = Regex::new(r"\b\d{4}\b").unwrap();
        let unique: HashSet<i32> = re.captures_iter(self.dates().join(" ").as_str())
            .filter_map(|c| c.get(0).map(|year| year.as_str().parse::<i32>().ok()).flatten())
            .collect();
        let mut years: Vec<i32> = unique.into_iter().collect();
        years.sort_unstable();
        years
    }
    pub fn publisher(&self) -> String {
        match &self.record_type {
            MetadataType::Marc21 => {
                String::from("")
            },
            MetadataType::UniMarc => {
                self.extract_fields("210", vec!["c"]).join(" ")
            },
        }
    }
    pub fn isbn(&self) -> String {
        match &self.record_type {
            MetadataType::Marc21 => {
                self.extract_fields("020", vec!["a"]).join(" ")
            },
            MetadataType::UniMarc => {
                self.extract_fields("010", vec!["a"]).join(" ")
            },
        }
    }
    pub fn uri(&self) -> Option<RecordUri> {
        match &self.record_type {
            MetadataType::Marc21 => {
                let re = Regex::new(r"https?://").unwrap();
                let mut found_uri = None;
                for uri in self.get_fields("856") {
                    let mut same_host = false;
                    let mut found = false;
                    let mut uri_str = "";
                    let mut content_type = "";
                    let mut label = "";
                    for sf in &uri.subfields {
                        if &sf.code == "u" {
                            if re.is_match(&sf.text) {
                                // println!("{} found!", sf.text);
                                same_host = sf.text.contains(&self.host);
                                uri_str = &sf.text;
                                found = true;
                            }
                        } else if &sf.code == "q" {
                            content_type = &sf.text;
                        } else if &sf.code == "y" {
                            label = &sf.text;
                        }
                    }
                    if found {
                        let uri_struct = RecordUri {
                            uri: String::from(uri_str),
                            content_type: String::from(content_type),
                            uri_label: String::from(label),
                        };
                        found_uri = Some(uri_struct);
                        // if we have an uri matching the origin, stop here
                        if same_host {
                            break
                        }
                    }
                }
                // try the koha uri if nothing was found
                if let None = found_uri {
                    if let Some(koha_uri) = self.extract_fields("952", vec!["u"]).first() {
                        found_uri = Some(RecordUri {
                            uri: koha_uri.to_string(),
                            content_type: String::from(""),
                            uri_label: String::from(""),
                        });
                    }
                }
                found_uri
            },
            MetadataType::UniMarc => None
        }
    }
    pub fn material_description(&self) -> String {
        match &self.record_type {
            MetadataType::Marc21 => {
                String::from("")
            },
            MetadataType::UniMarc => {
                self.extract_fields("215", vec!["a", "c", "d", "e"]).join(" ")
            },
        }
    }
    pub fn shelf_location_code(&self) -> String {
        match &self.record_type {
            MetadataType::Marc21 => {
                String::from("")
            },
            MetadataType::UniMarc => {
                let mut locs = self.extract_fields("950", vec!["a"]);
                locs.extend(self.extract_fields("676", vec!["a"]));
                locs.join(" ")
            },
        }
    }
    pub fn edition_statement(&self) -> String {
        match &self.record_type {
            MetadataType::Marc21 => {
                String::from("")
            },
            MetadataType::UniMarc => {
                self.extract_fields("255", vec!["a", "v"]).join(" ")
            },
        }
    }
    pub fn place_date_of_publication_distribution(&self) -> String {
        match &self.record_type {
            MetadataType::Marc21 => {
                String::from("")
            },
            MetadataType::UniMarc => {
                self.extract_fields("210", vec!["a", "d"]).join(" ")
            },
        }
    }
    pub fn is_aggregation(&self) -> bool {
        match &self.record_type {
            MetadataType::Marc21 => {
                false
            },
            MetadataType::UniMarc => {
                false
            },
        }
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
    pub library_id: i32,
    pub site_id: i32,
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

pub async fn harvest(params: &HarvestParams) -> Vec<HarvestedRecord> {
    let mut interaction = 1;
    let mut all_records: Vec<HarvestedRecord> = Vec::new();
    let mut url = params.harvest_url(None);
    loop {
        match download_url(url.clone()).await {
            Ok(res) => {
                match res.list_records {
                    Some(records) => {
                        // println!("{} {:?}", url, records);
                        for rec in records.records {
                            all_records.push(HarvestedRecord::new(rec, &params));
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
    all_records
}


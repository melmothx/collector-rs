use reqwest;
use serde::Deserialize;
use quick_xml::de::from_str;
use chrono::{DateTime, Utc};
use url::Url;
use std::time::SystemTime;
use regex::Regex;
use std::collections::HashSet;
use sha2::{Sha256, Digest};

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

#[derive(Clone, Debug)]
pub enum SiteType {
    Amusewiki,
    KohaMarc21,
    KohaUnimarc,
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
    site_type: SiteType,
}

pub fn language_iso_code(lang: &str) -> String {
    let re = Regex::new(r"[^a-z]").unwrap();
    let clean = re.replace_all(&lang.to_lowercase(), "").to_string();
    if clean.len() == 2 {
        return clean;
    }
    let mapped = match clean.as_str() {
        "alb" =>  "sq",
        "arm" =>  "hy",
        "baq" =>  "eu",
        "bur" =>  "my",
        "chi" =>  "zh",
        "cze" =>  "cs",
        "dut" =>  "nl",
        "fre" =>  "fr",
        "geo" =>  "ka",
        "ger" =>  "de",
        "gre" =>  "el",
        "ice" =>  "is",
        "mac" =>  "mk",
        "mao" =>  "mi",
        "may" =>  "ms",
        "per" =>  "fa",
        "rum" =>  "ro",
        "slo" =>  "sk",
        "tib" =>  "bo",
        "wel" =>  "cy",
        "abk" =>  "ab",
        "aar" =>  "aa",
        "afr" =>  "af",
        "aka" =>  "ak",
        "sqi" =>  "sq",
        "amh" =>  "am",
        "ara" =>  "ar",
        "arg" =>  "an",
        "hye" =>  "hy",
        "asm" =>  "as",
        "ava" =>  "av",
        "ave" =>  "ae",
        "aym" =>  "ay",
        "aze" =>  "az",
        "bam" =>  "bm",
        "bak" =>  "ba",
        "eus" =>  "eu",
        "bel" =>  "be",
        "ben" =>  "bn",
        "bis" =>  "bi",
        "bos" =>  "bs",
        "bre" =>  "br",
        "bul" =>  "bg",
        "mya" =>  "my",
        "cat" =>  "ca",
        "cha" =>  "ch",
        "che" =>  "ce",
        "nya" =>  "ny",
        "zho" =>  "zh",
        "chu" =>  "cu",
        "chv" =>  "cv",
        "cor" =>  "kw",
        "cos" =>  "co",
        "cre" =>  "cr",
        "hrv" =>  "hr",
        "ces" =>  "cs",
        "dan" =>  "da",
        "div" =>  "dv",
        "nld" =>  "nl",
        "dzo" =>  "dz",
        "eng" =>  "en",
        "epo" =>  "eo",
        "est" =>  "et",
        "ewe" =>  "ee",
        "fao" =>  "fo",
        "fij" =>  "fj",
        "fin" =>  "fi",
        "fra" =>  "fr",
        "fry" =>  "fy",
        "ful" =>  "ff",
        "gla" =>  "gd",
        "glg" =>  "gl",
        "lug" =>  "lg",
        "kat" =>  "ka",
        "deu" =>  "de",
        "ell" =>  "el",
        "kal" =>  "kl",
        "grn" =>  "gn",
        "guj" =>  "gu",
        "hat" =>  "ht",
        "hau" =>  "ha",
        "heb" =>  "he",
        "her" =>  "hz",
        "hin" =>  "hi",
        "hmo" =>  "ho",
        "hun" =>  "hu",
        "isl" =>  "is",
        "ido" =>  "io",
        "ibo" =>  "ig",
        "ind" =>  "id",
        "ina" =>  "ia",
        "ile" =>  "ie",
        "iku" =>  "iu",
        "ipk" =>  "ik",
        "gle" =>  "ga",
        "ita" =>  "it",
        "jpn" =>  "ja",
        "jav" =>  "jv",
        "kan" =>  "kn",
        "kau" =>  "kr",
        "kas" =>  "ks",
        "kaz" =>  "kk",
        "khm" =>  "km",
        "kik" =>  "ki",
        "kin" =>  "rw",
        "kir" =>  "ky",
        "kom" =>  "kv",
        "kon" =>  "kg",
        "kor" =>  "ko",
        "kua" =>  "kj",
        "kur" =>  "ku",
        "lao" =>  "lo",
        "lat" =>  "la",
        "lav" =>  "lv",
        "lim" =>  "li",
        "lin" =>  "ln",
        "lit" =>  "lt",
        "lub" =>  "lu",
        "ltz" =>  "lb",
        "mkd" =>  "mk",
        "mlg" =>  "mg",
        "msa" =>  "ms",
        "mal" =>  "ml",
        "mlt" =>  "mt",
        "glv" =>  "gv",
        "mri" =>  "mi",
        "mar" =>  "mr",
        "mah" =>  "mh",
        "mon" =>  "mn",
        "nau" =>  "na",
        "nav" =>  "nv",
        "nde" =>  "nd",
        "nbl" =>  "nr",
        "ndo" =>  "ng",
        "nep" =>  "ne",
        "nor" =>  "no",
        "nob" =>  "nb",
        "nno" =>  "nn",
        "iii" =>  "ii",
        "oci" =>  "oc",
        "oji" =>  "oj",
        "ori" =>  "or",
        "orm" =>  "om",
        "oss" =>  "os",
        "pli" =>  "pi",
        "pus" =>  "ps",
        "fas" =>  "fa",
        "pol" =>  "pl",
        "por" =>  "pt",
        "pan" =>  "pa",
        "que" =>  "qu",
        "ron" =>  "ro",
        "roh" =>  "rm",
        "run" =>  "rn",
        "rus" =>  "ru",
        "sme" =>  "se",
        "smo" =>  "sm",
        "sag" =>  "sg",
        "san" =>  "sa",
        "srd" =>  "sc",
        "srp" =>  "sr",
        "sna" =>  "sn",
        "snd" =>  "sd",
        "sin" =>  "si",
        "slk" =>  "sk",
        "slv" =>  "sl",
        "som" =>  "so",
        "sot" =>  "st",
        "spa" =>  "es",
        "sun" =>  "su",
        "swa" =>  "sw",
        "ssw" =>  "ss",
        "swe" =>  "sv",
        "tgl" =>  "tl",
        "tah" =>  "ty",
        "tgk" =>  "tg",
        "tam" =>  "ta",
        "tat" =>  "tt",
        "tel" =>  "te",
        "tha" =>  "th",
        "bod" =>  "bo",
        "tir" =>  "ti",
        "ton" =>  "to",
        "tso" =>  "ts",
        "tsn" =>  "tn",
        "tur" =>  "tr",
        "tuk" =>  "tk",
        "twi" =>  "tw",
        "uig" =>  "ug",
        "ukr" =>  "uk",
        "urd" =>  "ur",
        "uzb" =>  "uz",
        "ven" =>  "ve",
        "vie" =>  "vi",
        "vol" =>  "vo",
        "wln" =>  "wa",
        "cym" =>  "cy",
        "wol" =>  "wo",
        "xho" =>  "xh",
        "yid" =>  "yi",
        "yor" =>  "yo",
        "zha" =>  "za",
        "zul" =>  "zu",

        // "custom"
        "esp" => "es",
        "france" => "fr",
        "francese" => "fr",
        "inglese" => "en",
        "italiano" => "it",
        "spagnolo" => "es",
        "tedesco" => "de",
        _ => "unknown",
    };
    return String::from(mapped);
}

impl HarvestedRecord {
    fn new(record: OaiPmhRecord, params: &HarvestParams) -> Self {
        let base_uri = Url::parse(&params.base_url).expect("url must be valid at this point");
        HarvestedRecord {
            raw: record,
            record_type: match params.site_type {
                SiteType::KohaUnimarc => MetadataType::UniMarc,
                SiteType::KohaMarc21 => MetadataType::Marc21,
                SiteType::Amusewiki => MetadataType::Marc21,
            },
            host: match base_uri.host_str() {
                Some(h) => String::from(h),
                None => String::from(""),
            },
            site_type: params.site_type.clone(),
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
    pub fn languages(&self) -> Vec<String> {
        let mut langs = Vec::new();
        match &self.record_type {
            MetadataType::Marc21 => {
                langs.extend(self.extract_fields("041", vec!["a"]));
                langs.extend(self.extract_fields("546", vec!["a"]));
            },
            MetadataType::UniMarc => {
                langs.extend(self.extract_fields("101", vec!["a"]));
            },
        };
        langs.iter().map(|lang| language_iso_code(lang)).collect()
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
                let mut publishers = self.extract_fields("260",  vec!["b"]);
                publishers.extend(self.extract_fields("264",  vec!["b"]));
                publishers.join(" ")
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
                self.extract_fields("300", vec!["a", "b", "c", "e"]).join(" ")
            },
            MetadataType::UniMarc => {
                self.extract_fields("215", vec!["a", "c", "d", "e"]).join(" ")
            },
        }
    }
    pub fn shelf_location_code(&self) -> String {
        match &self.record_type {
            MetadataType::Marc21 => {
                let mut locs = self.extract_fields("952", vec!["o"]);
                locs.extend(self.extract_fields("852", vec!["c"]));
                locs.join(" ")
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
                self.extract_fields("250", vec!["a"]).join(" ")
            },
            MetadataType::UniMarc => {
                self.extract_fields("255", vec!["a", "v"]).join(" ")
            },
        }
    }
    pub fn place_date_of_publication_distribution(&self) -> String {
        match &self.record_type {
            MetadataType::Marc21 => {
                let mut places = self.extract_fields("260", vec!["a", "c"]);
                places.extend(self.extract_fields("264", vec!["a", "c"]));
                places.join(" ")
            },
            MetadataType::UniMarc => {
                self.extract_fields("210", vec!["a", "d"]).join(" ")
            },
        }
    }
    pub fn aggregations(&self) -> Vec<RecordAggregation> {
        let mut out = Vec::<RecordAggregation>::new();
        match &self.record_type {
            MetadataType::Marc21 => {
                for aggregation_field in self.get_fields("773") {
                    let mut agg = RecordAggregation {
                        name: None,
                        issue: None,
                        isbn: None,
                        order: None,
                        place_date_publisher: None,
                        item_identifier: None,
                        linkage: None,
                        host: self.host.clone(),
                    };
                    for sf in &aggregation_field.subfields {
                        let text = String::from(&sf.text);
                        match sf.code.as_str() {
                            "t" => { agg.name = Some(text) }
                            "g" => { agg.issue = Some(text) },
                            "z" => { agg.isbn = Some(text) },
                            "q" => {
                                match text.parse::<i32>() {
                                    Ok(i) => { agg.order = Some(i) },
                                    Err(_) => (),
                                }
                            },
                            "d" => { agg.place_date_publisher = Some(text) },
                            "o" => { agg.item_identifier = Some(text) },
                            "6" => { agg.linkage = Some(text) },
                            _ => (),
                        };
                    }
                    if let Some(_) = agg.name {
                        // println!("Aggregation: {}", agg.identifier());
                        out.push(agg);
                    }
                }
            },
            MetadataType::UniMarc => (),
        };
        out
    }
    pub fn checksum(&self) -> String {
        let mut hasher = Sha256::new();
        // order is for backcompat. We don't care as long as it's stable
        for agg in self.aggregations() {
            hasher.update(agg.full_aggregation_name());
        }
        for author in self.authors() {
            hasher.update(author);
        }
        for lang in self.languages() {
            hasher.update(lang);
        }
        hasher.update(self.subtitle());
        hasher.update(self.title());
        format!("{:x}", hasher.finalize())
    }
    pub async fn full_text(&self) -> Result<String, Box<dyn std::error::Error>> {
        match self.site_type {
            SiteType::Amusewiki => {
                match self.uri() {
                    Some(uri) => {
                        let bare_html = format!("{}.bare.html", uri.uri);
                        let body = reqwest::get(&bare_html).await?.text().await?;
                        // println!("Downloaded {bare_html}");
                        Ok(body)
                    },
                    None => {
                        Err(format!("No uri found").into())
                    },
                }
            }
            _ => {
                Err(format!("Not a site type with full text").into())
            },
        }
    }
}

#[derive(Debug)]
pub struct RecordAggregation {
    name: Option<String>,
    issue: Option<String>,
    isbn: Option<String>,
    order: Option<i32>,
    place_date_publisher: Option<String>,
    item_identifier: Option<String>,
    linkage: Option<String>,
    host: String,
}

impl RecordAggregation {
    pub fn name(&self) -> &str {
        match &self.name {
            Some(aggregation_name) => &aggregation_name,
            None => panic!("The name method cannot be called without the name set")
        }
    }
    pub fn identifier(&self) -> String {
        let mut identifier = vec!["aggregation", &self.host];
        match &self.item_identifier {
            Some(item_identifier) => {
                identifier.push(&item_identifier)
            },
            None => {
                identifier.push(self.name());
                if let Some(issue_number) = &self.issue {
                    identifier.push(&issue_number)
                }
            }
        }
        identifier.join(":")
    }
    pub fn full_aggregation_name(&self) -> String {
        let mut full_name = Vec::new();
        full_name.push(self.name());
        if let Some(issue_number) = &self.issue {
            full_name.push(&issue_number)
        }
        if let Some(place_date_publisher) = &self.place_date_publisher {
            full_name.push(&place_date_publisher)
        }
        full_name.join(" ")
    }
    pub fn checksum(&self) -> String {
        format!("{:x}", Sha256::digest(self.full_aggregation_name()))
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
    pub from: Option<SystemTime>,
    pub library_id: i32,
    pub site_id: i32,
    pub site_type: SiteType,
}

impl HarvestParams {
    pub fn harvest_url (&self, token: Option<&str>) -> Url {
        let mut url = Url::parse(&self.base_url).expect("base_url needs to be valid");
        let metadata_prefix = match self.site_type {
            // I think this is just a misconfiguration, but that's what we get in our cases
            SiteType::KohaUnimarc => "marc21",
            SiteType::KohaMarc21 => "marc21",
            SiteType::Amusewiki => "marc21",
        };
        url.query_pairs_mut().append_pair("verb", "ListRecords")
            .append_pair("metadataPrefix", metadata_prefix);
        match token {
            Some(token) => {
                url.query_pairs_mut().append_pair("resumptionToken", token);
            },
            None => {
                if let SiteType::Amusewiki = self.site_type {
                    url.query_pairs_mut().append_pair("set", "web");
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn id1_ok() {
        let rec =  RecordAggregation {
            name: Some(String::from("test")),
            issue: Some(String::from("n.1")),
            isbn: None,
            order: None,
            place_date_publisher: None,
            item_identifier: None,
            linkage: None,
            host: String::from("test-host"),
        };
        assert_eq!(rec.identifier(), "aggregation:test-host:test:n.1");
        assert_eq!(rec.full_aggregation_name(), "test n.1");
    }

    #[test]
    fn id2_ok() {
        let rec =  RecordAggregation {
            name: Some(String::from("test")),
            issue: Some(String::from("n.1")),
            isbn: None,
            order: None,
            place_date_publisher: (Some(String::from("Some place"))),
            item_identifier: Some(String::from("xxx")),
            linkage: None,
            host: String::from("test-host"),
        };
        assert_eq!(rec.identifier(), "aggregation:test-host:xxx");
        assert_eq!(rec.full_aggregation_name(), "test n.1 Some place");
    }

    #[test]
    fn id3_ok() {
        let rec =  RecordAggregation {
            name: Some(String::from("test")),
            issue: None,
            isbn: None,
            order: None,
            place_date_publisher: None,
            item_identifier: None,
            linkage: None,
            host: String::from("test-host"),
        };
        for _ in [1, 2] {
            assert_eq!(rec.identifier(), "aggregation:test-host:test");
            assert_eq!(rec.full_aggregation_name(), "test");
        }
    }
    #[test]
    #[should_panic]
    fn bad_id_ok() {
        let rec =  RecordAggregation {
            name: None,
            issue: None,
            isbn: None,
            order: None,
            place_date_publisher: None,
            item_identifier: None,
            linkage: None,
            host: String::from("test-host"),
        };
        rec.name();
    }

}

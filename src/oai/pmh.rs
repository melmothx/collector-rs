use serde::Deserialize;
use quick_xml::de::from_str;

#[derive(Debug, Deserialize)]
pub struct ResponseError {
    #[serde(rename = "@code")]
    code: String,
    #[serde(rename = "$text")]
    message: String,
}

#[derive(Debug, Deserialize)]
pub struct OaiPmhResponse {
    #[serde(rename = "responseDate")]
    response_date: String,
    request: String,
    error: ResponseError,
}

pub fn parse_response (xml: &str) -> OaiPmhResponse {
    match from_str(xml) {
        Ok(res) => res,
        Err(e) => OaiPmhResponse {
            response_date: String::from("NOW"),
            request: String::from("Invalid"),
            error: ResponseError {
                code: String::from("Invalid XML"),
                message: e.to_string(),
            }
        },
    }
}

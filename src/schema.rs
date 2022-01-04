use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::default;
use std::fmt;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    pub fields: HashMap<String, Field>,
    pub default_order_by: String,
    pub table: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Field {
    name: String,
    pub converter: Option<ConverterSchema>,
    #[serde(default)]
    pub query: FieldQuery,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct ConverterSchema {
    pub from: ConvertFrom,
    pub to: ConvertTo,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum FieldQuery {
    Range {
        min: String,
        max: String,
        #[serde(default)]
        aliases: HashMap<String, i64>,
    },
    Fulltext {
        lang: String,
        #[serde(default)]
        syntax: FulltextSyntax,
        target: Option<String>,
    },
    AmbiguousTag,
    NumericTag {
        #[serde(default)]
        aliases: HashMap<String, i64>,
    },
    StringTag,
    Nested,
    Min,
    Max,
    Bool,
}

impl default::Default for FieldQuery {
    fn default() -> Self {
        FieldQuery::AmbiguousTag
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum FulltextSyntax {
    TsQuery,
    Plain,
    Phrase,
    WebSearch,
}

impl default::Default for FulltextSyntax {
    fn default() -> Self {
        FulltextSyntax::WebSearch
    }
}

impl fmt::Display for FulltextSyntax {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FulltextSyntax::TsQuery => write!(f, "to_tsquery"),
            FulltextSyntax::Plain => write!(f, "plainto_tsquery"),
            FulltextSyntax::Phrase => write!(f, "phraseto_tsquery"),
            FulltextSyntax::WebSearch => write!(f, "websearch_to_tsquery"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum ConvertFrom {
    CommaSeparatedString,
    SemicolonSeparatedString,
    DateTimeString,
    DateString,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum ConvertTo {
    Timestamp,
    TimestampMillis,
    TagArray,
}

#[cfg(feature = "rocket_support")]
use rocket::{
    request::{self, FromRequest, Request},
    State,
};
#[cfg(feature = "rocket_support")]
#[rocket::async_trait]
impl<'r> FromRequest<'r> for Schema {
    type Error = ();

    async fn from_request(request: &'r Request<'_>) -> request::Outcome<Self, ()> {
        request
            .guard::<&State<Schema>>()
            .await
            .map(|s| s.inner().clone()) // clone bad, i know
    }
}

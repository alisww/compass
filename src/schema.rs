use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::default;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    pub fields: HashMap<String, Field>,
    pub default_order_by: String,
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
    Range { min: String, max: String },
    Fulltext { lang: String },
    AmbiguousTag,
    NumericTag,
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
pub enum ConvertFrom {
    CommaSeparatedString,
    SemicolonSeparatedString,
    DateTimeString,
    DateString,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum ConvertTo {
    Timestamp,
    TagArray,
}

#[cfg(feature = "rocket_support")]
use rocket::{
    async_trait,
    outcome::IntoOutcome,
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

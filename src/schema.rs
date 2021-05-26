use std::default;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    pub fields: HashMap<String,Field>,
    pub default_order_by: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Field {
    name: String,
    pub converter: Option<ConverterSchema>,
    #[serde(default)]
    pub query: FieldQuery
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct ConverterSchema {
    pub from: ConvertFrom,
    pub to: ConvertTo
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum FieldQuery {
    Range {  min: String,  max: String },
    Fulltext,
    Tag,
    Nested,
    Min,
    Max
}

impl default::Default for FieldQuery {
    fn default() -> Self { FieldQuery::Tag }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum ConvertFrom {
    CommaSeparatedString,
    SemicolonSeparatedString,
    DateTimeString,
    DateString
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum ConvertTo {
    Timestamp,
    TagArray
}

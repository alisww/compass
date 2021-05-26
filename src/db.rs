use super::*;
use uuid::Uuid;

use deadpool_postgres::Client;

use serde_json::{json,Value};
use serde_json::map::Map as SerdeMap;

use tokio_postgres::types::ToSql;
use tokio_postgres::types::Type as PostgresType;
use tokio_postgres::{Statement,Row};

use futures::TryStreamExt;
use std::collections::HashMap;

use chrono::{DateTime, NaiveDateTime, Utc};

type JSONMap = SerdeMap<String,Value>;

pub async fn add_document(client: &Client, doc: String) -> Result<(),CompassError> {
    let v : Value = serde_json::from_str(&doc)?;
    client.execute("INSERT INTO documents (doc_id, object) VALUES ($1,$2);",&[&Uuid::new_v4(),&v]).await?;
    Ok(())
}

fn parse_query_list<F>(q: &str, filter_gen: F) -> (String,JSONMap) where F: Fn(&str, i32) -> (String,(String,Value)) {
    let mut filters: Vec<String> = Vec::new();
    let mut bindings: JSONMap = JSONMap::new();
    let mut iter = q.split("_");

    let mut i: i32 = 0;

    while let Some(val) = iter.next() {
        let (filter, binding) = filter_gen(val,i);
        filters.push(filter);
        bindings.insert(binding.0,binding.1);

        i += 1;

        if let Some(joiner) = iter.next() {
            if joiner == "and" {
                filters.push("&&".to_string());
            } else if joiner == "or" {
                filters.push("||".to_string());
            }
        } else {
            break;
        }
    }

    (filters.join(" "), bindings)
}

pub async fn json_search(client: &Client, schema: &Schema, params: &Value) -> Result<Vec<Value>,CompassError> {
    let fields = params.as_object().ok_or(CompassError::FieldNotFound)?;

    let mut jsonb_filters = Vec::<String>::new();
    let mut other_filters = Vec::<String>::new();

    let mut jsonb_bindings = JSONMap::new();
    let mut other_bindings = Vec::<String>::new();

    let mut converters: HashMap<String,ConverterSchema> = HashMap::new();

    for (k,field) in schema.fields.iter() { // make a table of field -> converter, to see if we need to do any conversions on the results
        if let Some(converter) = field.converter {
            converters.insert(k.to_string(),converter);
        }
    }

    for (k,v) in fields {
        let field_maybe = match schema.fields.get(k) { // find field from URL query in schema
            Some(field) => {
                Some((k,field.query.clone())) // oh, we found it by name. cool, return that
            },
            None => {
                schema.fields.iter().find_map(|f| {
                    match f.1.query { // oops we couldn't find it; let's see if it's a field that can have multiple names like range or metadata
                        FieldQuery::Range { ref min, ref max } => {
                            if k == min {
                                Some((f.0,FieldQuery::Min))
                            } else if k == max {
                                Some((f.0,FieldQuery::Max))
                            } else {
                                None
                            }
                        },
                        FieldQuery::Nested => {
                            if k.split('.').next().unwrap() == f.0 {
                                Some((k,FieldQuery::Nested))
                            } else {
                                None
                            }
                        }
                        _ => None
                    }
                })
            }
        };

        if let None = field_maybe {
            continue // yeah no i hate this. this is for fields like limit or offset, which don't have entries in the schema
        }

        let field = field_maybe.unwrap();

        match field.1 { // time to generate the query!
            FieldQuery::Min => {
                let (filters,mut bindings) = parse_query_list(v.as_str().unwrap(),|x,i| {
                    (format!("(@.{} > ${}_{})",field.0,k,i),
                    (
                        format!("{}_{}",k,i),
                        json!(x.parse::<i32>().unwrap() // don't unwrap here. please. change it to a better thing.
                    )))
                });
                jsonb_bindings.append(&mut bindings);
                jsonb_filters.push(filters);
            },
            FieldQuery::Max => {
                let (filters,mut bindings) = parse_query_list(v.as_str().unwrap(),|x,i| {
                    (format!("(@.{} < ${}_{})",field.0,k,i),
                    (
                        format!("{}_{}",k,i),
                        json!(x.parse::<i32>().unwrap()
                    )))
                });
                jsonb_bindings.append(&mut bindings);
                jsonb_filters.push(filters);
            },
            FieldQuery::Tag => {
                let (filters,mut bindings) = parse_query_list(v.as_str().unwrap(),|x,i| {
                    (format!("(@.{} == ${}_{})",field.0,k,i),
                    (
                        format!("{}_{}",k,i),
                        json!(x)
                    ))
                });
                jsonb_bindings.append(&mut bindings);
                jsonb_filters.push(filters);
            },
            FieldQuery::Nested => {
                let (filters,mut bindings) = parse_query_list(v.as_str().unwrap(),|x,i| {
                    (format!("(@.{} == ${}_{})",field.0,k.replace(".","_"),i),
                    (
                        format!("{}_{}",k.replace(".","_"),i), // if it looks like an int, make it an int! because we can't specificy all the metadata fields in the schema. yeah i don't like this either
                        if let Ok(n) = x.parse::<i64>() { json!(n) } else { json!(x) }
                    ))
                });
                jsonb_bindings.append(&mut bindings);
                jsonb_filters.push(filters);
            },
            FieldQuery::Fulltext => {
                other_filters.push(format!("to_tsvector(object->'{}') @@ phraseto_tsquery(${})",field.0, other_filters.len() + 6));
                other_bindings.push(v.to_string());
            },
            _ => {}
        }
    }

    let json_query = format!("$ ? ({})",jsonb_filters.join(" && "));
    let json_val = json!(jsonb_bindings);

    // build out full query
    let mut query = if jsonb_filters.len() > 0 && other_filters.len() == 0 {
        "SELECT jsonb_path_query(object, CAST($1 AS JSONPATH), $2) FROM documents".to_string()
    } else if jsonb_filters.len() > 0 && other_filters.len() > 0 {
        ("SELECT jsonb_path_query(object, CAST($1 AS JSONPATH), $2) FROM documents".to_string() + &format!(" INTERSECT SELECT object FROM documents WHERE {}",other_filters.join(" AND "))).to_string()
    } else {
        format!("SELECT object FROM documents WHERE {}",other_filters.join(" AND "))
    };

    query += &format!("ORDER BY object->>$3 LIMIT $4 OFFSET $5");

    let statement: Statement = client.prepare_typed(query.as_str(), &[PostgresType::TEXT, PostgresType::JSONB]).await.map_err(CompassError::PGError)?;

    // some defaults. todo, don't unwrap, please;

    let limit = match fields.get("limit") {
        Some(l) => {
            l.as_str().unwrap().parse::<i64>().unwrap()
        },
        None => {
            50
        }
    };

    let offset = match fields.get("offset") {
        Some(l) => {
            l.as_str().unwrap().parse::<i64>().unwrap()
        },
        None => {
            0
        }
    };

    let params: Vec<&dyn ToSql> = vec![
        &json_query,
        &json_val,
        &schema.default_order_by,
        &limit,
        &offset
    ];

    let rows: Vec<Row> = client.query_raw(&statement,
        params.iter()
        .copied()
        .chain(other_bindings
            .iter()
            .map(|x|&*x as &dyn ToSql))
        .collect::<Vec<&dyn ToSql>>())
    .await.map_err(CompassError::PGError)?
    .try_collect()
    .await.map_err(CompassError::PGError)?;

    Ok(rows.into_iter().map(|x| {
        let mut val = x.get::<usize,Value>(0);
        for (key,conv) in converters.iter() {
            if let Some(field) = val.get_mut(key) {
                match (conv.from, conv.to) {
                    (ConvertFrom::DateTimeString, ConvertTo::Timestamp) => { // convert timestamps back into date-strings
                        let timest = field.as_i64().unwrap();
                        let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timest, 0), Utc);
                        *field = json!(dt.to_rfc3339());

                    },
                    _ => {}
                }
            }
        }
        val
    }).collect())
}

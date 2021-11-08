use super::*;

use postgres::Client;

use serde_json::{json, Value};

use postgres::fallible_iterator::FallibleIterator;
use postgres::types::ToSql;
use postgres::types::Type as PostgresType;
use postgres::{Row, Statement};

use std::collections::HashMap;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

use uuid::Uuid;

fn parse_query_list<F>(q: &str, filter_gen: F) -> Result<String, CompassError>
where
    F: Fn(&str) -> Result<String, CompassError>,
{
    let mut filters: Vec<String> = Vec::new();
    let mut iter = q.split("_");

    while let Some(val) = iter.next() {
        let filter = filter_gen(val)?;
        filters.push(filter);

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

    Ok(format!("({})", filters.join(" ")))
}

pub fn json_search(
    client: &mut Client,
    schema: &Schema,
    fields: &HashMap<String, String>,
) -> Result<Vec<Value>, CompassError> {
    let mut jsonb_filters = Vec::<String>::new();
    let mut other_filters = Vec::<String>::new();

    let mut other_bindings = Vec::<String>::new();

    let converters: HashMap<String, ConverterSchema> = schema
        .fields
        .iter()
        .filter_map(|(k, v)| {
            if let Some(converter) = v.converter {
                Some((k.to_owned(), converter))
            } else {
                None
            }
        })
        .collect();

    for (k, v) in fields {
        let field_maybe = match schema.fields.get(k) {
            // find field from URL query in schema
            Some(field) => {
                Some((k, field.query.clone())) // oh, we found it by name. cool, return that
            }
            None => {
                schema.fields.iter().find_map(|f| {
                    match f.1.query {
                        // oops we couldn't find it; let's see if it's a field that can have multiple names like range or metadata
                        FieldQuery::Range {
                            ref min, ref max, ..
                        } => {
                            if k == min {
                                Some((f.0, FieldQuery::Min))
                            } else if k == max {
                                Some((f.0, FieldQuery::Max))
                            } else {
                                None
                            }
                        }
                        FieldQuery::Nested => {
                            if k.split('.').next().unwrap() == f.0 {
                                Some((k, FieldQuery::Nested))
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                })
            }
        };

        if let None = field_maybe {
            continue; // yeah no i hate this. this is for fields like limit or offset, which don't have entries in the schema
        }

        let field = field_maybe.unwrap();

        match field.1 {
            // time to generate the query!
            FieldQuery::Range {
                min: _,
                max: _,
                ref aliases,
            } => {
                // if something gets directly found as a 'Range' query, it means someone used season=18 instead of like, season_min=16. so it actually, counter-intuitively, is like a numeric tag!
                let filters = parse_query_list(v, |x| {
                    if x == "exists" {
                        Ok(format!("(exists($.{}))", field.0))
                    } else if x == "notexists" {
                        Ok(format!("(!exists($.{}))", field.0))
                    } else {
                        if let Some(n) = aliases.get(&x.to_uppercase()) {
                            Ok(format!("($.{} == {})", field.0, n))
                        } else {
                            Ok(format!(
                                "($.{} == {})",
                                field.0,
                                x.parse::<i64>().map_err(CompassError::InvalidNumberError)?
                            ))
                        }
                    }
                })?;
                jsonb_filters.push(filters);
            }
            FieldQuery::Min => {
                let filters = parse_query_list(v, |x| {
                    Ok(format!(
                        "($.{} > {})",
                        field.0,
                        x.parse::<i64>().map_err(CompassError::InvalidNumberError)?
                    ))
                })?;
                jsonb_filters.push(filters);
            }
            FieldQuery::Max => {
                let filters = parse_query_list(v, |x| {
                    Ok(format!(
                        "($.{} < {})",
                        field.0,
                        x.parse::<i64>().map_err(CompassError::InvalidNumberError)?
                    ))
                })?;
                jsonb_filters.push(filters);
            }
            FieldQuery::Bool => {
                let filters = parse_query_list(v, |x| {
                    if x == "exists" {
                        Ok(format!("(exists($.{}))", field.0))
                    } else if x == "notexists" {
                        Ok(format!("(!exists($.{}))", field.0))
                    } else {
                        Ok(format!(
                            "($.{} == {})",
                            field.0,
                            x.parse::<bool>().map_err(CompassError::InvalidBoolError)?
                        ))
                    }
                })?;
                jsonb_filters.push(filters);
            }
            FieldQuery::AmbiguousTag => {
                let filters = parse_query_list(v, |x| {
                    let mut filter: Vec<String> = Vec::new();

                    if let Ok(n) = x.parse::<i64>() {
                        filter.push(format!("($.{} == {})", field.0, n)); // if it looks like an int, make it an int! because we can't specificy all the metadata fields in the schema. yeah i don't like this either
                    } else if let Ok(n) = x.parse::<bool>() {
                        filter.push(format!("($.{} == {})", field.0, n));
                    } else if x == "exists" {
                        filter.push(format!("(exists($.{}))", field.0))
                    } else if x == "notexists" {
                        filter.push(format!("(!exists($.{}))", field.0))
                    }

                    filter.push(format!("($.{} == \"{}\")", field.0, x));

                    Ok(format!("({})", filter.join(" || ")))
                })?;
                jsonb_filters.push(filters);
            }
            FieldQuery::NumericTag { ref aliases } => {
                let filters = parse_query_list(v, |x| {
                    if x == "exists" {
                        Ok(format!("(exists($.{}))", field.0))
                    } else if x == "notexists" {
                        Ok(format!("(!exists($.{}))", field.0))
                    } else {
                        if let Some(n) = aliases.get(&x.to_uppercase()) {
                            Ok(format!(
                                "(($.{field} == {value}) || ($.{field} == \"{value}\"))",
                                field = field.0,
                                value = n
                            ))
                        } else {
                            Ok(format!(
                                "(($.{field} == {value}) || ($.{field} == \"{value}\"))",
                                field = field.0,
                                value =
                                    x.parse::<i64>().map_err(CompassError::InvalidNumberError)?
                            ))
                        }
                    }
                })?;
                jsonb_filters.push(filters);
            }
            FieldQuery::StringTag => {
                let filters = parse_query_list(v, |x| Ok(format!("($.{} == \"{}\")", field.0, x)))?;
                jsonb_filters.push(filters);
            }
            FieldQuery::Nested => {
                let filters = parse_query_list(v, |x| {
                    let mut filter: Vec<String> = Vec::new();

                    if let Ok(n) = x.parse::<i64>() {
                        filter.push(format!("($.{} == {})", field.0, n)); // if it looks like an int, make it an int! because we can't specificy all the metadata fields in the schema. yeah i don't like this either
                    } else if let Ok(n) = x.parse::<bool>() {
                        filter.push(format!("($.{} == {})", field.0, n));
                    } else if x == "exists" {
                        filter.push(format!("(exists($.{}))", field.0))
                    } else if x == "notexists" {
                        filter.push(format!("(!exists($.{}))", field.0))
                    }

                    filter.push(format!("($.{} == \"{}\")", field.0, x));

                    Ok(format!("({})", filter.join(" || ")))
                })?;
                jsonb_filters.push(filters);
            }
            FieldQuery::Fulltext {
                ref lang,
                ref syntax,
                ref target,
            } => {
                other_filters.push(format!(
                    "to_tsvector('{lang}',object->>'{key}') @@ {function}('{lang}',${parameter})",
                    lang = lang,
                    key = target.as_ref().unwrap_or(field.0),
                    function = syntax,
                    parameter = other_filters.len() + 5
                ));
                other_bindings.push(v.to_string());
            }
        }
    }

    let json_query = format!("({})", jsonb_filters.join(" && "));

    // build out full query
    let mut query = if jsonb_filters.len() > 0 && other_filters.len() == 0 {
        format!(
            "SELECT object FROM {} WHERE object @@ CAST($1 AS JSONPATH)",
            schema.table
        )
    } else if jsonb_filters.len() > 0 && other_filters.len() > 0 {
        (format!(
            "SELECT object FROM {} WHERE object @@ CAST($1 AS JSONPATH)",
            schema.table
        ) + &format!(" AND {}", other_filters.join(" AND ")))
            .to_string()
    } else if other_filters.len() > 0 {
        format!(
            "SELECT object FROM {} WHERE {}",
            schema.table,
            other_filters.join(" AND ")
        )
    } else {
        format!("SELECT object FROM {}", schema.table)
    };

    let order = match fields.get("sortorder") {
        Some(l) => {
            let ord = l.as_str().to_uppercase();
            if ord == "ASC" || ord == "DESC" {
                ord
            } else {
                "ASC".to_owned()
            }
        }
        None => "DESC".to_owned(),
    };

    query += &format!(
        " ORDER BY (object #> ($2)::text[]) {}, doc_id NULLS LAST LIMIT $3 OFFSET $4",
        order
    );

    let statement: Statement = client
        .prepare_typed(query.as_str(), &[PostgresType::TEXT, PostgresType::TEXT])
        .map_err(CompassError::PGError)?;

    let sort_by = match fields.get("sortby") {
        Some(l) => l.as_str(),
        None => &schema.default_order_by.as_str(),
    };

    let limit = match fields.get("limit") {
        Some(l) => l.parse::<i64>().map_err(CompassError::InvalidNumberError)?,
        None => 100,
    };

    let offset = match fields.get("offset") {
        Some(l) => l.parse::<i64>().map_err(CompassError::InvalidNumberError)?,
        None => 0,
    };

    let params: Vec<&dyn ToSql> = vec![&json_query, &sort_by, &limit, &offset];

    let rows: Vec<Row> = client
        .query_raw(
            &statement,
            params
                .iter()
                .copied()
                .chain(other_bindings.iter().map(|x| &*x as &dyn ToSql))
                .collect::<Vec<&dyn ToSql>>(),
        )
        .map_err(CompassError::PGError)?
        .collect()
        .map_err(CompassError::PGError)?;

    Ok(rows
        .into_iter()
        .map(|x| {
            let mut val = x.get::<usize, Value>(0);
            for (key, conv) in converters.iter() {
                if let Some(field) = val.get_mut(key) {
                    match (conv.from, conv.to) {
                        (ConvertFrom::DateTimeString, ConvertTo::Timestamp) => {
                            // convert timestamps back into date-strings
                            let timest = field.as_i64().unwrap();
                            let dt = DateTime::<Utc>::from_utc(
                                NaiveDateTime::from_timestamp(timest, 0),
                                Utc,
                            );
                            *field = json!(dt.to_rfc3339_opts(chrono::SecondsFormat::Millis,true));
                        }
                        (ConvertFrom::DateTimeString, ConvertTo::TimestampMillis) => {
                            let dt = Utc.timestamp_millis(field.as_i64().unwrap());
                            *field = json!(dt.to_rfc3339_opts(chrono::SecondsFormat::Millis,true));
                        }
                        _ => {}
                    }
                }
            }
            val
        })
        .collect())
}

pub fn get_by_ids(
    client: &mut Client,
    schema: &Schema,
    ids: &Vec<Uuid>,
) -> Result<Vec<Value>, CompassError> {
    // make a table of field -> converter, to see if we need to do any conversions on the results
    let converters: HashMap<String, ConverterSchema> = schema
        .fields
        .iter()
        .filter_map(|(k, v)| {
            if let Some(converter) = v.converter {
                Some((k.to_owned(), converter))
            } else {
                None
            }
        })
        .collect();

    Ok(client
        .query(
            format!("SELECT object FROM {} WHERE doc_id = ANY($1)", schema.table).as_str(),
            &[ids],
        )?
        .into_iter()
        .map(|x| {
            let mut val = x.get::<usize, Value>(0);
            for (key, conv) in converters.iter() {
                if let Some(field) = val.get_mut(key) {
                    match (conv.from, conv.to) {
                        (ConvertFrom::DateTimeString, ConvertTo::Timestamp) => {
                            // convert timestamps back into date-strings
                            let timest = field.as_i64().unwrap();
                            let dt = DateTime::<Utc>::from_utc(
                                NaiveDateTime::from_timestamp(timest, 0),
                                Utc,
                            );
                            *field = json!(dt.to_rfc3339_opts(chrono::SecondsFormat::Millis,true));
                        }
                        (ConvertFrom::DateTimeString, ConvertTo::TimestampMillis) => {
                            let dt = Utc.timestamp_millis(field.as_i64().unwrap());
                            *field = json!(dt.to_rfc3339_opts(chrono::SecondsFormat::Millis,true));
                        }
                        _ => {}
                    }
                }
            }
            val
        })
        .collect())
}

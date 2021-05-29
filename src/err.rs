use tokio_postgres::error::Error as PGError;
use serde_json::error::Error as SerdeError;
use deadpool_postgres::PoolError;
use actix_web::{HttpResponse, ResponseError};
use std::fmt;
use std::num::ParseIntError;

#[derive(Debug)]
pub enum CompassError {
    FieldNotFound,
    PGError(PGError),
    PoolError(PoolError),
    JSONError(SerdeError),
    InvalidNumberError(ParseIntError)
}

impl std::error::Error for CompassError {}

impl From<PGError> for CompassError {
    fn from(err: PGError) -> CompassError {
        CompassError::PGError(err)
    }
}

impl From<PoolError> for CompassError {
    fn from(err: PoolError) -> CompassError {
        CompassError::PoolError(err)
    }
}

impl From<SerdeError> for CompassError {
    fn from(err: SerdeError) -> CompassError {
        CompassError::JSONError(err)
    }
}

impl From<ParseIntError> for CompassError {
    fn from(err: ParseIntError) -> CompassError {
        CompassError::InvalidNumberError(err)
    }
}

impl ResponseError for CompassError {
    fn error_response(&self) -> HttpResponse {
        match *self {
            CompassError::PoolError(ref err) => { HttpResponse::InternalServerError().body(err.to_string()) },
            CompassError::PGError(ref err) => { HttpResponse::InternalServerError().body(err.to_string()) },
            CompassError::InvalidNumberError(ref err) => { HttpResponse::InternalServerError().body(err.to_string()) },
            _ => HttpResponse::InternalServerError().finish()
        }
    }
}

impl fmt::Display for CompassError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,"{:?}",self)
    }
}

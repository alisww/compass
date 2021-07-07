use postgres::error::Error as PGError;
use serde_json::error::Error as SerdeError;
use std::fmt;
use std::num::ParseIntError;
use std::str::ParseBoolError;

#[derive(Debug)]
pub enum CompassError {
    FieldNotFound,
    PGError(PGError),
    JSONError(SerdeError),
    InvalidNumberError(ParseIntError),
    InvalidBoolError(ParseBoolError),
}

impl std::error::Error for CompassError {}

impl From<PGError> for CompassError {
    fn from(err: PGError) -> CompassError {
        CompassError::PGError(err)
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

impl From<ParseBoolError> for CompassError {
    fn from(err: ParseBoolError) -> CompassError {
        CompassError::InvalidBoolError(err)
    }
}

impl fmt::Display for CompassError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(feature = "rocket_support")]
use rocket::{
    http::Status,
    response::{self, Responder, Response},
    Request,
};
#[cfg(feature = "rocket_support")]
use std::io::Cursor;
#[cfg(feature = "rocket_support")]
use CompassError::*;
#[cfg(feature = "rocket_support")]
impl<'r> Responder<'r, 'static> for CompassError {
    fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
        match self {
            FieldNotFound => {
                let r_text = "field not found in schema";
                Response::build()
                    .status(Status::BadRequest)
                    .sized_body(r_text.len(), Cursor::new(r_text))
                    .ok()
            }
            InvalidNumberError(_) => {
                let r_text = "couldn't parse number parameter";
                Response::build()
                    .status(Status::BadRequest)
                    .sized_body(r_text.len(), Cursor::new(r_text))
                    .ok()
            }
            InvalidBoolError(_) => {
                let r_text = "couldn't parse boolean parameter";
                Response::build()
                    .status(Status::BadRequest)
                    .sized_body(r_text.len(), Cursor::new(r_text))
                    .ok()
            }
            PGError(ref err) => {
                let r_text = err.to_string();
                Response::build()
                    .status(Status::InternalServerError)
                    .sized_body(r_text.len(), Cursor::new(r_text))
                    .ok()
            }
            JSONError(ref err) => {
                let r_text = err.to_string();
                Response::build()
                    .status(Status::InternalServerError)
                    .sized_body(r_text.len(), Cursor::new(r_text))
                    .ok()
            }
        }
    }
}

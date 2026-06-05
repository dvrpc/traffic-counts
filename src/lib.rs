//! This library contains data structures related to DVRPC's traffic counts
//! and enables performing various kinds of operations on them, like
//! [extracting][non_perm::extract_from_file] data from files,
//! [CRUD db operations][db::crud],
//! [aggregating volume data by hour][non_perm::HourlyVehicle::from_db], and
//! [averaging speed data by hour][non_perm::HourlyAvgSpeed::create].
//!
//! There are currently two binary programs that extract data from files, transform it, and insert
//! it into our database. The [non_perm_import](../non_perm_import/index.html) handles non-permanent
//! traffic counts, while the [perm_bikeped_import](../perm_bikeped_import/index.html) program
//! handles permanent bicycle/pedestrian counts. See each program's documentation for further
//! details, including filename specifications, the specific types of counts they handle/create,
//! and how they are run.
//!
//! See <https://www.dvrpc.org/traffic/> for additional information about traffic counting.

use std::io;
use std::num::{ParseFloatError, ParseIntError};
use std::path::PathBuf;

use chrono::NaiveDate;
use thiserror::Error;

pub mod db;
pub mod non_perm;
pub mod perm_bikeped;

/// A trait for getting a [`NaiveDate`](https://docs.rs/chrono/latest/chrono/struct.NaiveDate.html)
/// from a type.
pub trait GetDate {
    fn get_date(&self) -> NaiveDate;
}

/// Various errors that can occur.
#[derive(Debug, Error)]
pub enum CountError {
    #[error("unknown count type '{0}'")]
    UnknownCountType(String),
    #[error("problem with file or directory path")]
    BadPath(PathBuf),
    #[error("unable to open file '{0}'")]
    CannotOpenFile(#[from] io::Error),
    #[error("the filename at {path:?} is not to specification: {problem:?}")]
    InvalidFileName {
        problem: FileNameProblem,
        path: PathBuf,
    },
    #[error("no matching count type for directory '{0}'")]
    BadLocation(String),
    #[error("no matching count type for header in '{0}'")]
    BadHeader(PathBuf),
    #[error("no such direction '{0}'")]
    BadDirection(String),
    #[error("missing directions")]
    MissingDirection,
    #[error("mismatch in count types between file location ('{0}') and header of that file")]
    LocationHeaderMisMatch(PathBuf),
    #[error("mismatch in number of directions between database and data in that file")]
    DirectionLenMisMatch,
    #[error("data does not exist in expected column")]
    MissingDataColumn,
    #[error("cannot parse value as number")]
    ParseIntError(#[from] ParseIntError),
    #[error("cannot parse value as number")]
    ParseFloatError(#[from] ParseFloatError),
    #[error("cannot parse date/time '{0:?}'")]
    ChronoParseError(chrono::format::ParseErrorKind),
    #[error("no such vehicle class '{0}'")]
    BadVehicleClass(u8),
    #[error("unable to determine interval from count")]
    BadIntervalCount,
    #[error("error converting header row to string")]
    HeadertoStringRecordError(#[from] csv::Error),
    #[error("invalid MCD ({0})")]
    InvalidMcd(String),
    #[error("too many fields in permanent bikeped data")]
    TooManyPermBikePedFields,
    #[error("too few fields in permanent bikeped data")]
    TooFewPermBikePedFields,
    #[error("unexpected number of fields in permanent bikeped data (expected 3 or 5)")]
    UnexpectedNumberOfPermBikePedFields,
    #[error("inconsistent data in database")]
    InconsistentData,
    // Errors from database specifically handled/custom error messages.
    #[error("{0}")]
    DbError(String),
    // Errors from database passed through transparently without specific handling.
    #[error("database error '{0}'")]
    OracleError(#[from] oracle::Error),
    #[error("{0}")]
    DataCheckError(String),
}

/// Identifying the problem when there's an error with a filename.
#[derive(Debug)]
pub enum FileNameProblem {
    InvalidRecordNum,
    InvalidDirections,
}

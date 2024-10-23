//! Various implementations for use with an Oracle database.
use std::str::FromStr;

use chrono::NaiveDateTime;
use log::Level;
use oracle::{sql_type::FromSql, Error as OracleError, RowValue, SqlValue};

use crate::{
    db::ImportLogEntry, CountKind, LaneDirection, OptionCountKind, OptionLaneDirection,
    OptionRoadDirection, RoadDirection,
};

impl FromSql for CountKind {
    fn from_sql(val: &SqlValue<'_>) -> oracle::Result<Self> {
        match CountKind::from_str(&val.to_string()) {
            Ok(v) => Ok(v),
            Err(e) => Err(OracleError::ParseError(Box::new(e))),
        }
    }
}

impl FromSql for OptionCountKind {
    fn from_sql(val: &SqlValue<'_>) -> oracle::Result<Self> {
        match val.is_null() {
            Ok(true) => Ok(OptionCountKind(None)),
            Ok(false) => match CountKind::from_str(&val.to_string()) {
                Ok(v) => Ok(OptionCountKind(Some(v))),
                Err(e) => Err(OracleError::ParseError(Box::new(e))),
            },
            Err(e) => Err(OracleError::ParseError(Box::new(e))),
        }
    }
}

impl RowValue for ImportLogEntry {
    fn get(row: &oracle::Row) -> oracle::Result<Self> {
        let recordnum = row.get("recordnum")?;
        let msg: String = row.get("message")?;
        let level: String = row.get("log_level")?;
        let level = Level::from_str(level.as_str()).unwrap();
        let datetime: NaiveDateTime = row.get("datetime")?;
        let mut log_record = ImportLogEntry::new(recordnum, msg, level);
        log_record.datetime = Some(datetime);
        Ok(log_record)
    }
}

impl FromSql for LaneDirection {
    fn from_sql(val: &SqlValue<'_>) -> oracle::Result<Self> {
        match LaneDirection::from_str(&val.to_string()) {
            Ok(v) => Ok(v),
            Err(e) => Err(OracleError::ParseError(Box::new(e))),
        }
    }
}

impl FromSql for OptionLaneDirection {
    fn from_sql(val: &SqlValue<'_>) -> oracle::Result<Self> {
        match val.is_null() {
            Ok(true) => Ok(OptionLaneDirection(None)),
            Ok(false) => match LaneDirection::from_str(&val.to_string()) {
                Ok(v) => Ok(OptionLaneDirection(Some(v))),
                Err(e) => Err(OracleError::ParseError(Box::new(e))),
            },
            Err(e) => Err(OracleError::ParseError(Box::new(e))),
        }
    }
}

impl FromSql for RoadDirection {
    fn from_sql(val: &SqlValue<'_>) -> oracle::Result<Self> {
        match RoadDirection::from_str(&val.to_string()) {
            Ok(v) => Ok(v),
            Err(e) => Err(OracleError::ParseError(Box::new(e))),
        }
    }
}

impl FromSql for OptionRoadDirection {
    fn from_sql(val: &SqlValue<'_>) -> oracle::Result<Self> {
        match val.is_null() {
            Ok(true) => Ok(OptionRoadDirection(None)),
            Ok(false) => match RoadDirection::from_str(&val.to_string()) {
                Ok(v) => Ok(OptionRoadDirection(Some(v))),
                Err(e) => Err(OracleError::ParseError(Box::new(e))),
            },
            Err(e) => Err(OracleError::ParseError(Box::new(e))),
        }
    }
}

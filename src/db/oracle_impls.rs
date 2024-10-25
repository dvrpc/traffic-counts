//! Various implementations for use with an Oracle database.
use std::str::FromStr;

use chrono::NaiveDateTime;
use log::Level;
use oracle::{
    sql_type::{FromSql, OracleType, ToSql, ToSqlNull},
    Connection, Error as OracleError, RowValue, SqlValue,
};

use crate::{db::ImportLogEntry, CountError, CountKind, LaneDirection, RoadDirection};

impl FromSql for CountKind {
    fn from_sql(val: &SqlValue<'_>) -> oracle::Result<Self> {
        match CountKind::from_str(&val.to_string()) {
            Ok(v) => Ok(v),
            Err(CountError::UnknownCountType(_)) => Err(OracleError::NullValue),
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
            Err(CountError::BadDirection(_)) => Err(OracleError::NullValue),
            Err(e) => Err(OracleError::ParseError(Box::new(e))),
        }
    }
}

impl ToSql for LaneDirection {
    fn oratype(&self, _conn: &Connection) -> oracle::Result<OracleType> {
        Ok(OracleType::NVarchar2(format!("{self}").len() as u32))
    }
    fn to_sql(&self, val: &mut SqlValue<'_>) -> oracle::Result<()> {
        format!("{self}").to_sql(val)
    }
}

impl ToSqlNull for LaneDirection {
    fn oratype_for_null(_conn: &Connection) -> oracle::Result<OracleType> {
        Ok(OracleType::NVarchar2(0))
    }
}

impl FromSql for RoadDirection {
    fn from_sql(val: &SqlValue<'_>) -> oracle::Result<Self> {
        match RoadDirection::from_str(&val.to_string()) {
            Ok(v) => Ok(v),
            Err(CountError::BadDirection(_)) => Err(OracleError::NullValue),
            Err(e) => Err(OracleError::ParseError(Box::new(e))),
        }
    }
}

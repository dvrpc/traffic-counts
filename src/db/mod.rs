//! Shared database functionality.

pub mod count_insert;

use std::env;
use std::fmt::Display;

use chrono::NaiveDateTime;
use log::Level;
use oracle::{
    pool::{Pool, PoolBuilder},
    Connection, Error as OracleError,
};
use serde::Serialize;

use crate::{CountError, Metadata};

pub const RECORD_CREATION_LIMIT: u32 = 50;

pub fn get_creds() -> (String, String) {
    dotenvy::dotenv().expect("Unable to load .env file.");

    (
        env::var("DB_USERNAME").unwrap(),
        env::var("DB_PASSWORD").unwrap(),
    )
}

/// Create a connection pool.
pub fn create_pool(username: String, password: String) -> Result<Pool, OracleError> {
    PoolBuilder::new(username, password, "dvrpcprod_tp_tls")
        .max_connections(5)
        .build()
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct LogRecord {
    pub datetime: Option<NaiveDateTime>,
    pub record_num: u32,
    pub msg: String,
    pub level: String,
}

impl LogRecord {
    pub fn new(record_num: u32, msg: String, level: Level) -> Self {
        Self {
            datetime: None,
            record_num,
            msg,
            level: level.to_string(),
        }
    }
}

impl Display for LogRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{:?} {}{}{}",
            self.datetime.unwrap().date(),
            self.datetime.unwrap().time(),
            self.record_num,
            self.msg,
            self.level
        )
    }
}

pub fn update_db_import_log(conn: &Connection, log_record: LogRecord) -> Result<(), oracle::Error> {
    conn.execute(
        "insert into import_log (recordnum, message, log_level) values (:1, :2, :3)",
        &[&log_record.record_num, &log_record.msg, &log_record.level],
    )?;
    conn.commit()
}

pub fn get_import_log(
    conn: &Connection,
    record_num: Option<u32>,
) -> Result<Vec<LogRecord>, oracle::Error> {
    let results = match record_num {
        Some(v) => conn.query_as::<LogRecord>(
            "select * from import_log WHERE recordnum = :1 order by datetime desc",
            &[&v],
        ),
        None => conn.query_as::<LogRecord>("select * from import_log order by datetime desc", &[]),
    }?;

    let mut log_records = vec![];
    for row in results {
        let log_record = row?;
        log_records.push(log_record);
    }

    Ok(log_records)
}

/// Get total number of records in metadata (tc_header) table.
pub fn get_metadata_total_recs(conn: &Connection) -> Result<u32, CountError> {
    Ok(conn.query_row_as::<u32>("select count(*) from tc_header", &[])?)
}

/// Get paginated metadata (tc_header) records.
pub fn get_metadata(conn: &Connection, record_num: u32) -> Result<Metadata, CountError> {
    Ok(conn.query_row_as::<Metadata>(
        "select * from tc_header where recordnum = :1",
        &[&record_num],
    )?)
}

/// Get paginated metadata (tc_header) records.
pub fn get_metadata_paginated(
    conn: &Connection,
    offset: Option<u32>,
    limit: Option<u32>,
) -> Result<Vec<Metadata>, CountError> {
    let mut records = vec![];

    let offset = offset.unwrap_or(0);
    let limit = limit.unwrap_or(100);
    let results = conn.query_as::<Metadata>(
        "select * from tc_header 
            order by recordnum DESC
            offset :1 rows
            fetch first :2 rows only",
        &[&offset, &limit],
    )?;

    for row in results {
        let row = row?;
        records.push(row)
    }
    Ok(records)
}

/// Insert one or more empty metadata (tc_header) records (with recordnum and created date only).
pub fn insert_empty_metadata(conn: &Connection, number: u32) -> Result<Vec<u32>, CountError> {
    if number == 0 {
        return Err(CountError::DbError("Cannot create 0 records".to_string()));
    }
    if number > RECORD_CREATION_LIMIT {
        return Err(CountError::DbError(format!(
            "Too many new records requested: cannot created more than {}",
            RECORD_CREATION_LIMIT
        )));
    }

    let mut recordnums = vec![];
    for _ in 0..number {
        let stmt = conn.execute(
            "insert into tc_header (createheaderdate) values (CURRENT_DATE) RETURNING recordnum INTO :record_num",
            &[&None::<u32>],
        )?;
        let record_num: u32 = stmt.returned_values("record_num")?[0];
        recordnums.push(record_num);
    }
    conn.commit()?;
    Ok(recordnums)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[ignore]
    #[test]
    fn create_pool_succeeds() {
        let (username, password) = get_creds();
        assert!(create_pool(username, password).is_ok())
    }

    #[ignore]
    #[test]
    fn select_type_correct() {
        let (username, password) = get_creds();
        let pool = create_pool(username, password).unwrap();
        let conn = pool.get().unwrap();

        let count_type = conn
            .query_row("select type from tc_header where recordnum = '151454'", &[])
            .unwrap();

        assert_eq!(
            count_type.get_as::<String>().unwrap(),
            "15 min Volume".to_string()
        )
    }
}

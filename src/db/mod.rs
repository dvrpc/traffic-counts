//! Database functionality.

//! ## A Note about Data Entry/Completeness
//!
//! Data for counts are inserted into the database without checking for complete periods. For
//! example, if the count starts at 10:55am, any records for vehicles counted between 10:55 and
//! 11am will be added to the database, even though it is not a full 15-minute period. Similarly,
//! when data is aggregated by hour and inserted into the TC_VOLCOUNT table, the first and last
//! hours may not be a full hour of count data.

pub mod crud;
pub mod oracle_impls;

use std::env;
use std::fmt::Display;

use chrono::NaiveDateTime;
use log::Level;
use oracle::{
    pool::{Pool, PoolBuilder},
    Connection, Error as OracleError,
};
use serde::Serialize;

use crate::{CountError, CountKind, Metadata};

/// The maximum number of empty metadata records allowed to be created.
pub const RECORD_CREATION_LIMIT: u32 = 50;

/// Get database credentials from environment variable.
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

/// AADV calculation requires an intermediate table to be updated first.
pub fn update_intermediate_aadv(recordnum: u32, conn: &Connection) -> Result<(), CountError> {
    let sql = "begin update_tc_countdate(:1); end;";
    let mut stmt = conn.statement(sql).build()?;
    Ok(stmt.execute(&[&recordnum])?)
}

/// Call database function to calculate and insert AADV.
pub fn calc_aadv(recordnum: u32, conn: &Connection) -> Result<i32, CountError> {
    match conn.query_row_as::<i32>(&format!("select calc_aadv({}) from dual", recordnum), &[]) {
        Ok(v) => Ok(v),
        Err(_) => Err(CountError::DbError(format!(
            "Unable to calculate AADV for {recordnum}"
        ))),
    }
}

/// A log entry from data imports.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ImportLogEntry {
    pub datetime: Option<NaiveDateTime>,
    pub recordnum: u32,
    pub msg: String,
    pub level: String,
}

impl ImportLogEntry {
    pub fn new(recordnum: u32, msg: String, level: Level) -> Self {
        Self {
            datetime: None,
            recordnum,
            msg,
            level: level.to_string(),
        }
    }
}

impl Display for ImportLogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{:?} {}{}{}",
            self.datetime.unwrap().date(),
            self.datetime.unwrap().time(),
            self.recordnum,
            self.msg,
            self.level
        )
    }
}

/// Insert an [`ImportLogEntry`].
pub fn insert_import_log_entry(
    conn: &Connection,
    log_record: ImportLogEntry,
) -> Result<(), oracle::Error> {
    conn.execute(
        "insert into import_log (recordnum, message, log_level) values (:1, :2, :3)",
        &[&log_record.recordnum, &log_record.msg, &log_record.level],
    )?;
    conn.commit()
}

/// Get all [Import Log Entries](ImportLogEntry).
pub fn get_import_log(
    conn: &Connection,
    recordnum: Option<u32>,
) -> Result<Vec<ImportLogEntry>, oracle::Error> {
    let results = match recordnum {
        Some(v) => conn.query_as::<ImportLogEntry>(
            "select * from import_log WHERE recordnum = :1 order by datetime desc",
            &[&v],
        ),
        None => {
            conn.query_as::<ImportLogEntry>("select * from import_log order by datetime desc", &[])
        }
    }?;

    let mut log_records = vec![];
    for row in results {
        let log_record = row?;
        log_records.push(log_record);
    }

    Ok(log_records)
}

/// Get total number of records in [`Metadata`] table.
pub fn get_metadata_total_recs(conn: &Connection) -> Result<u32, CountError> {
    Ok(conn.query_row_as::<u32>("select count(*) from tc_header", &[])?)
}

/// Get a [`Metadata`] record.
pub fn get_metadata(conn: &Connection, recordnum: u32) -> Result<Metadata, CountError> {
    Ok(conn.query_row_as::<Metadata>(
        "select * from tc_header where recordnum = :1",
        &[&recordnum],
    )?)
}

/// Get paginated [`Metadata`] records.
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

/// Insert one or more empty [`Metadata`] records (with recordnum and created date only).
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
            "insert into tc_header (createheaderdate) values (CURRENT_DATE) RETURNING recordnum INTO :recordnum",
            &[&None::<u32>],
        )?;
        let recordnum: u32 = stmt.returned_values("recordnum")?[0];
        recordnums.push(recordnum);
    }
    conn.commit()?;
    Ok(recordnums)
}

/// Insert one or more [`Metadata`] records using fields from existing one.
pub fn insert_metadata_from_existing(
    conn: &Connection,
    number: u32,
    metadata: Metadata,
) -> Result<Vec<u32>, CountError> {
    if number == 0 {
        return Err(CountError::DbError("Cannot create 0 records".to_string()));
    }
    if number > RECORD_CREATION_LIMIT {
        return Err(CountError::DbError(format!(
            "Too many new records requested: cannot created more than {}",
            RECORD_CREATION_LIMIT
        )));
    }
    let sql = "insert into tc_header (
        amending, ampeak, bikepeddesc, bikepedfacility, bikepedgroup, \
        cntdir, comments, type, counterid, createheaderdate, datelastcounted, \
        description, fc, fromlmt, importdatadate, indir, isurban, latitude, \
        longitude, mcd, mp, offset, outdir, pmending, pmpeak, prj, program, rdprefix, \
        rdsuffix, road, route, seg, sidewalk, speedlimit, source, sr, sri, stationid, \
        takenby, tolmt, trafdir, x, y)
        VALUES \
        (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, 
        :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, 
        :35, :36, :37, :38, :39, :40, :41, :42, :43)
        RETURNING recordnum INTO :recordnum";
    let mut stmt = conn.statement(sql).build()?;
    let mut recordnums = vec![];
    for _ in 0..number {
        stmt.execute(&[
            &metadata.amending,
            &metadata.ampeak,
            &metadata.bikepeddesc,
            &metadata.bikepedfacility,
            &metadata.bikepedgroup,
            &metadata.cntdir,
            &metadata.comments,
            &metadata.count_kind,
            &metadata.counter_id,
            &metadata.createheaderdate,
            &metadata.datelastcounted,
            &metadata.description,
            &metadata.fc,
            &metadata.fromlmt,
            &metadata.importdatadate,
            &metadata.indir,
            &metadata.isurban,
            &metadata.latitude,
            &metadata.longitude,
            &metadata.mcd,
            &metadata.mp,
            &metadata.offset,
            &metadata.outdir,
            &metadata.pmending,
            &metadata.pmpeak,
            &metadata.prj,
            &metadata.program,
            &metadata.rdprefix,
            &metadata.rdsuffix,
            &metadata.road,
            &metadata.route,
            &metadata.seg,
            &metadata.sidewalk,
            &metadata.speedlimit,
            &metadata.source,
            &metadata.sr,
            &metadata.sri,
            &metadata.stationid,
            &metadata.tolmt,
            &metadata.trafdir,
            &metadata.x,
            &metadata.y,
            &None::<u32>,
        ])?;
        let recordnum: u32 = stmt.returned_values("recordnum")?[0];
        recordnums.push(recordnum);
    }
    conn.commit()?;
    Ok(recordnums)
}

/// Get the type of count for a given record number.
pub fn get_count_kind(conn: &Connection, recordnum: u32) -> Result<Option<CountKind>, CountError> {
    match conn.query_row_as::<Option<CountKind>>(
        "select type from tc_header where recordnum = :1",
        &[&recordnum],
    ) {
        Ok(v) => Ok(v),
        Err(e) => Err(CountError::DbError(format!("{e}"))),
    }
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

//! Shared database functionality.

use std::env;
use std::fmt::Display;
use std::str::FromStr;

use log::Level;
use oracle::{
    pool::{Pool, PoolBuilder},
    sql_type::Timestamp,
    Connection, Error as OracleError, RowValue,
};
use serde::Serialize;
use time::{
    format_description::BorrowedFormatItem, macros::format_description, Date, PrimitiveDateTime,
    Time,
};

use crate::{CountError, Direction, Metadata};

pub const RECORD_CREATION_LIMIT: u32 = 50;
pub const YYYY_MM_DD_FMT: &[BorrowedFormatItem<'_>] =
    format_description!("[year]-[month padding:none]-[day padding:none]");

// TODO: the dates here get converted to Options if errors, but they should be handled properly.
impl RowValue for Metadata {
    fn get(row: &oracle::Row) -> oracle::Result<Self> {
        let amending = row.get("amending")?;
        let ampeak = row.get("ampeak")?;
        let bikepeddesc = row.get("bikepeddesc")?;
        let bikepedfacility = row.get("bikepedfacility")?;
        let bikepedgroup = row.get("bikepedgroup")?;
        let cntdir = Direction::from_option_string(row.get("cntdir")?).ok();
        let comments = row.get("comments")?;
        let count_type = row.get("type")?;
        let counterid = row.get("counterid")?;
        let createheaderdate = row.get("createheaderdate")?;
        let datelastcounted = row.get("datelastcounted")?;
        let description = row.get("description")?;
        let fc = row.get("fc")?;
        let fromlmt = row.get("fromlmt")?;
        let importdatadate = row.get("importdatadate")?;
        let indir = Direction::from_option_string(row.get("indir")?).ok();
        let isurban = {
            let isurban: String = row.get("isurban")?;
            if isurban == *"Y" {
                Some(true)
            } else {
                Some(false)
            }
        };
        let latitude = row.get("latitude")?;
        let longitude = row.get("longitude")?;
        let mcd = row.get("mcd")?;
        let mp = row.get("mp")?;
        let offset = row.get("offset")?;
        let outdir = Direction::from_option_string(row.get("outdir")?).ok();
        let pmending = row.get("pmending")?;
        let pmpeak = row.get("pmpeak")?;
        let prj = row.get("prj")?;
        let program = row.get("program")?;
        let record_num = row.get("recordnum")?;
        let rdprefix = row.get("rdprefix")?;
        let rdsuffix = row.get("rdsuffix")?;
        let road = row.get("road")?;
        let route = row.get("route")?;
        let seg = row.get("seg")?;
        let sidewalk = row.get("sidewalk")?;
        let speed_limit = row.get("speedlimit")?;
        let source = row.get("source")?;
        let sr = row.get("sr")?;
        let sri = row.get("sri")?;
        let stationid = row.get("stationid")?;
        let technician = row.get("takenby")?;
        let tolmt = row.get("tolmt")?;
        let trafdir = Direction::from_option_string(row.get("trafdir")?).ok();
        let x = row.get("x")?;
        let y = row.get("y")?;

        let record = Metadata {
            amending,
            ampeak,
            bikepeddesc,
            bikepedgroup,
            bikepedfacility,
            cntdir,
            comments,
            count_type,
            counterid,
            createheaderdate,
            datelastcounted,
            description,
            fc,
            fromlmt,
            importdatadate,
            indir,
            isurban,
            latitude,
            longitude,
            mcd,
            mp,
            offset,
            outdir,
            pmending,
            pmpeak,
            prj,
            program,
            record_num,
            rdprefix,
            rdsuffix,
            road,
            route,
            seg,
            sidewalk,
            speed_limit,
            source,
            sr,
            sri,
            stationid,
            technician,
            tolmt,
            trafdir,
            x,
            y,
        };
        Ok(record)
    }
}

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
    pub datetime: Option<PrimitiveDateTime>,
    pub record_num: u32,
    pub msg: String,
    pub level: String,
}

impl RowValue for LogRecord {
    fn get(row: &oracle::Row) -> oracle::Result<Self> {
        let record_num = row.get("recordnum")?;
        let msg: String = row.get("message")?;
        let level: String = row.get("log_level")?;
        let level = Level::from_str(level.as_str()).unwrap();

        let datetime: Timestamp = row.get("datetime")?;
        let date_format = format_description!("[year]-[month padding:none]-[day padding:none]");
        let time_format = format_description!("[hour padding:none]:[minute padding:none]");
        let datetime = PrimitiveDateTime::new(
            Date::parse(
                &format!(
                    "{}-{}-{}",
                    datetime.year(),
                    datetime.month(),
                    datetime.day()
                ),
                date_format,
            )
            .unwrap(),
            Time::parse(
                &format!("{}:{}", datetime.hour(), datetime.minute()),
                &time_format,
            )
            .unwrap(),
        );
        let mut log_record = LogRecord::new(record_num, msg, level);
        log_record.datetime = Some(datetime);
        Ok(log_record)
    }
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

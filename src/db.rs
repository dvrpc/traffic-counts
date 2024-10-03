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
use time::{
    format_description::BorrowedFormatItem, macros::format_description, Date, PrimitiveDateTime,
    Time,
};

pub const YYYY_MM_DD_FMT: &[BorrowedFormatItem<'_>] =
    format_description!("[year]-[month padding:none]-[day padding:none]");

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

#[derive(Debug)]
pub struct LogRecord {
    pub datetime: Option<PrimitiveDateTime>,
    pub record_num: u32,
    pub msg: String,
    pub level: Level,
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
            level,
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
        &[
            &log_record.record_num,
            &log_record.msg,
            &log_record.level.as_str(),
        ],
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

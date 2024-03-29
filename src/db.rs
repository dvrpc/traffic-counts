//! Interact with database.
use oracle::{
    pool::{Pool, PoolBuilder},
    Connection, Error as OracleError,
};

use crate::*;

/// A trait for database operations on output count types.
pub trait CountTable {
    /// The name of the table in the database that this count type corresponds to.
    const TABLE_NAME: &'static str; // associated constant

    /// Delete all records in the table.
    fn delete(conn: &Connection, recordnum: i32) -> Result<(), oracle::Error> {
        let sql = "delete from :1 where recordnum = :2";
        conn.execute(sql, &[&Self::TABLE_NAME, &recordnum])?;
        conn.commit()
    }
    // TODO
    fn insert(&self) {}
}

impl CountTable for FifteenMinuteVehicleClassCount {
    const TABLE_NAME: &'static str = "tc_clacount";
}

impl CountTable for FifteenMinuteSpeedRangeCount {
    const TABLE_NAME: &'static str = "tc_specount";
}
impl CountTable for NonNormalAvgSpeedCount {
    const TABLE_NAME: &'static str = "tc_spesum";
}
impl CountTable for NonNormalVolCount {
    const TABLE_NAME: &'static str = "tc_volcount";
}
impl CountTable for FifteenMinuteVehicle {
    const TABLE_NAME: &'static str = "tc_15minvolcount";
}

/// Create a connection pool.
pub fn create_pool(username: String, password: String) -> Result<Pool, OracleError> {
    PoolBuilder::new(username, password, "dvrpcprod_tp_tls")
        .max_connections(5)
        .build()
}

#[cfg(test)]

mod tests {
    use super::*;
    use std::env;

    fn get_creds() -> (String, String) {
        dotenvy::dotenv().expect("Unable to load .env file.");

        (
            env::var("DB_USERNAME").unwrap(),
            env::var("DB_PASSWORD").unwrap(),
        )
    }

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

//! Shared database functionality.

use std::env;

use oracle::{
    pool::{Pool, PoolBuilder},
    Error as OracleError,
};
use time::{format_description::BorrowedFormatItem, macros::format_description};

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

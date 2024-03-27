//! Interact with database.
use oracle::{
    pool::{Pool, PoolBuilder},
    Error as OracleError,
};

use crate::{
    CountError, FifteenMinuteSpeedRangeCount, FifteenMinuteVehicleClassCount,
    NonNormalAvgSpeedCount, NonNormalVolCount,
};

pub trait Insert {
    type Item;
    fn insert(item: Self::Item) -> Result<(), CountError<'static>>;
}

impl Insert for FifteenMinuteVehicleClassCount {
    type Item = FifteenMinuteVehicleClassCount;
    fn insert(_: FifteenMinuteVehicleClassCount) -> Result<(), CountError<'static>> {
        Ok(())
    }
}

impl Insert for FifteenMinuteSpeedRangeCount {
    type Item = FifteenMinuteSpeedRangeCount;
    fn insert(_: FifteenMinuteSpeedRangeCount) -> Result<(), CountError<'static>> {
        Ok(())
    }
}

impl Insert for NonNormalVolCount {
    type Item = NonNormalVolCount;
    fn insert(_: NonNormalVolCount) -> Result<(), CountError<'static>> {
        Ok(())
    }
}

impl Insert for NonNormalAvgSpeedCount {
    type Item = NonNormalVolCount;
    fn insert(_: NonNormalVolCount) -> Result<(), CountError<'static>> {
        Ok(())
    }
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

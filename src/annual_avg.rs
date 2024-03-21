//! Average annual calculations and helpers.
use std::collections::HashSet;

use oracle::sql_type::Timestamp;
use oracle::{Connection, Error as OracleError, Statement};
use time::{Date, Weekday};

use crate::CountType;

/// A trait for getting a [`Date`](https://docs.rs/time/latest/time/struct.Date.html) from a type.
pub trait GetDate {
    fn get_date(&self) -> Date;
}

/// Determine the date to use for the annual average calculation.
pub fn determine_date<T>(counts: Vec<T>) -> Option<Date>
where
    T: GetDate,
{
    if counts.is_empty() {
        return None;
    }

    // Get all unique dates of the counts.
    let mut dates = counts
        .iter()
        .map(|count| count.get_date())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    // Drop the first day, as it won't be full day of data, after sorting to ensure order.
    dates.sort();
    dates.remove(0);

    let week_days = [
        Weekday::Monday,
        Weekday::Tuesday,
        Weekday::Wednesday,
        Weekday::Thursday,
        Weekday::Friday,
    ];

    // Return the first date that is a non-weekend weekday.
    dates
        .iter()
        .find(|date| week_days.contains(&date.weekday()))
        .copied()
}

/// Set SET_DATE (date for AADT/AADB/AADP) in TC_HEADER (& redundant SET_FLAG in TC_VOLCOUNT)
// TODO: move this to db.rs once that module is ready
pub fn set_date_for_annual_avg_calc(
    date: Date,
    recordnum: u32,
    count_type: CountType,
    conn: &Connection,
) -> Result<Statement, OracleError> {
    let oracle_date = Timestamp::new(
        date.year(),
        date.month() as u32,
        date.day().into(),
        0,
        0,
        0,
        0,
    );

    // If this is motor vehicle count, we also need to set SET_FLAG to -1 in TC_VOLCOUNT
    if count_type == CountType::FifteenMinuteVehicle {
        conn.execute(
            "UPDATE tc_volcount SET set_flag = -1 WHERE recordnum = :1 AND COUNTDATE = :2",
            &[&recordnum, &oracle_date],
        )?;
    }

    conn.execute(
        "INSERT into tc_header (set_date) VALUES (:1) WHERE recordnum = :2",
        &[&oracle_date, &recordnum],
    )
}

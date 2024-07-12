use oracle::sql_type::Timestamp;
use time::{macros::format_description, Date, PrimitiveDateTime, Time};

use crate::denormalize::NonNormalVolCount;
use crate::{Connection, CountError, Direction, TimeBinnedVehicleClassCount};

pub trait Verify {
    /// Verify data meets expectations.
    fn verify_data(record_num: u32, conn: &Connection) -> Result<Vec<Warning>, CountError>;
}

#[derive(Debug, Clone)]
pub struct ClassCountVerification {
    datetime: PrimitiveDateTime,
    lane: u8,
    dir: Direction,
    c2: u32,
    c15: u32,
    total: u32,
}

#[derive(Debug, Clone)]
pub struct Warning {
    pub message: String,
    pub recordnum: u32,
}

impl Warning {
    fn new(message: String, recordnum: u32) -> Self {
        Self { message, recordnum }
    }
}

impl Verify for ClassCountVerification {
    fn verify_data(recordnum: u32, conn: &Connection) -> Result<Vec<Warning>, CountError> {
        let results = conn.query_as::<(Timestamp, Timestamp, u8, String, u32, u32, u32)>(
            "select countdate, counttime, countlane, ctdir, total, cars_and_tlrs, unclassified from tc_clacount where recordnum = :1",
            &[&recordnum],
        )?;

        let mut counts = vec![];
        for result in results {
            let (count_date, count_time, lane, direction, total, c2, c15) = result?;
            let date_format = format_description!("[year]-[month padding:none]-[day padding:none]");
            let time_format = format_description!("[hour padding:none]:[minute padding:none]");
            let datetime = PrimitiveDateTime::new(
                Date::parse(
                    &format!(
                        "{}-{}-{}",
                        count_date.year(),
                        count_date.month(),
                        count_date.day()
                    ),
                    date_format,
                )
                .unwrap(),
                Time::parse(
                    &format!("{}:{}", count_time.hour(), count_time.minute()),
                    &time_format,
                )
                .unwrap(),
            );
            counts.push(ClassCountVerification {
                datetime,
                lane,
                dir: Direction::from_string(direction).unwrap(),
                c2,
                c15,
                total,
            })
        }

        let c2_sum = counts.iter().map(|count| count.c2).sum::<u32>();
        let c15_sum = counts.iter().map(|count| count.c15).sum::<u32>();
        let total_sum = counts.iter().map(|count| count.total).sum::<u32>();

        let c2_percent = c2_sum as f32 / total_sum as f32 * 100.0;
        let c15_percent = c15_sum as f32 / total_sum as f32 * 100.0;

        let mut warnings = vec![];

        if c2_percent < 75.0 {
            warnings.push(Warning::new(
                format!("Percent of class 2 vehicles is less than 75 ({c2_percent})"),
                recordnum,
            ))
        }

        if c15_percent > 10.0 {
            warnings.push(Warning::new(
                format!("Percent of unclassed vehicles is greater than 10 ({c15_percent})"),
                recordnum,
            ))
        }

        Ok(warnings)
    }
}

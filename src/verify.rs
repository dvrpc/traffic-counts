use std::collections::HashMap;

use oracle::sql_type::Timestamp;
use time::{macros::format_description, Date, PrimitiveDateTime, Time};

use crate::denormalize::NonNormalVolCount;
use crate::{Connection, CountError, Direction, TimeBinnedVehicleClassCount};

// If a count is bidirectional, the totals for both directions should be relatively proportional.
// One direction have less than this level is considered abnormal.
const DIR_PROPORTION_LOWER_BOUND: f32 = 0.40;

pub trait Verify {
    /// Verify data meets expectations.
    fn verify_data(record_num: u32, conn: &Connection) -> Result<Vec<Warning>, CountError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

        // Check shares class 2 and class 15
        let c2_sum = counts.iter().map(|count| count.c2).sum::<u32>();
        let c15_sum = counts.iter().map(|count| count.c15).sum::<u32>();
        let total_sum = counts.iter().map(|count| count.total).sum::<u32>();

        let c2_percent = c2_sum as f32 / total_sum as f32 * 100.0;
        let c15_percent = c15_sum as f32 / total_sum as f32 * 100.0;

        let mut warnings = vec![];

        if c2_percent < 75.0 {
            warnings.push(Warning::new(
                format!("Class 2 vehicles are less than 75% ({c2_percent:.1}%) of total."),
                recordnum,
            ))
        }

        if c15_percent > 10.0 {
            warnings.push(Warning::new(
                format!("Unclassed vehicles are greater than 10% ({c15_percent:.1}%) of total."),
                recordnum,
            ))
        }

        // Check proportion of total by direction.
        let mut count_by_dir = HashMap::new();
        for count in counts {
            *count_by_dir.entry(count.dir).or_insert(count.total) += count.total;
        }

        let larger_entry = count_by_dir.iter().max_by(|a, b| a.1.cmp(b.1));
        let smaller_entry = count_by_dir.iter().min_by(|a, b| a.1.cmp(b.1));

        if count_by_dir.keys().len() > 1 {
            if let Some(smaller) = smaller_entry {
                if let Some(larger) = larger_entry {
                    let total = smaller.1 + larger.1;
                    let smaller_share = *smaller.1 as f32 / total as f32;
                    let larger_share = *larger.1 as f32 / total as f32;
                    if smaller_share < DIR_PROPORTION_LOWER_BOUND {
                        warnings.push(Warning::new(
                            format!(
                                "Abnormal direction proportions: {} has {:.1}% of total, {} has {:.1}%.  (Expectation is that proportions are no less/more than {}%/{}%.)",
                                smaller.0,
                                smaller_share * 100_f32,
                                larger.0,
                                larger_share * 100_f32,
                                DIR_PROPORTION_LOWER_BOUND * 100_f32,
                                100_f32 - DIR_PROPORTION_LOWER_BOUND * 100_f32,
                            ),
                            recordnum,
                        ))
                    }
                }
            }
        }

        Ok(warnings)
    }
}

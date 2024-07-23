use std::collections::HashMap;

use oracle::sql_type::Timestamp;
use time::{macros::format_description, Date, PrimitiveDateTime, Time};

use crate::{Connection, CountError, Direction};

// If a count is bidirectional, the totals for both directions should be relatively proportional.
// One direction having less than this level is considered abnormal.
const DIR_PROPORTION_LOWER_BOUND: f32 = 0.40;
// Unusually high count for bicycles in a 15-minute period.
const BIKE_COUNT_MAX: u32 = 20;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClassCountCheck {
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

pub fn check(recordnum: u32, conn: &Connection) -> Result<Vec<Warning>, CountError> {
    // Set up vec to push all warnings into.
    let mut warnings = vec![];

    // Determine what kind of count this is, in order to run the appropriate checks.
    let result = conn.query_row_as::<Option<String>>(
        "select type from tc_header where recordnum = :1",
        &[&recordnum],
    )?;

    let count_type = match result {
        None => {
            warnings.push(Warning::new(
                "Unable to identify type of count; cannot check data.".to_string(),
                recordnum,
            ));
            return Ok(warnings);
        }
        Some(v) => v,
    };

    // Warn if share of unclassed vehicles is too high or class 2 is too low.
    if count_type == "Class".to_string() {
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
            counts.push(ClassCountCheck {
                datetime,
                lane,
                dir: Direction::from_string(direction).unwrap(),
                c2,
                c15,
                total,
            })
        }

        // Check share of class 2 and class 15 of total.
        let c2_sum = counts.iter().map(|count| count.c2).sum::<u32>();
        let c15_sum = counts.iter().map(|count| count.c15).sum::<u32>();
        let total_sum = counts.iter().map(|count| count.total).sum::<u32>();

        let c2_percent = c2_sum as f32 / total_sum as f32 * 100.0;
        let c15_percent = c15_sum as f32 / total_sum as f32 * 100.0;

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
    }

    // Warn if motor vehicle counts don't have relatively even proportion of total per direction.
    if ["Class", "Volume", "15 min Volume"].contains(&count_type.as_str()) {
        // Check proportion of total by direction.
        let results = conn.query_as::<(Timestamp, u32, String)>(
            "select countdate, totalcount, cntdir from tc_volcount where recordnum = :1",
            &[&recordnum],
        )?;

        let mut count_by_dir = HashMap::new();
        for result in results {
            let (count_date, total, direction) = result?;
            *count_by_dir.entry(direction).or_insert(total) += total;
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
    }

    // Warn about bicycle counts having more than 20 in any 15-minute period.
    if count_type.as_str().contains("Bicycle") {
        let results = conn.query_as::<u32>(
            "select total from tc_bikecount where dvrpcnum = :1",
            &[&recordnum],
        )?;

        for result in results {
            let total = result?;
            if total > BIKE_COUNT_MAX {
                warnings.push(Warning::new(
                    format!(
                        "More than {} in a 15-minute period for a bicycle count.",
                        BIKE_COUNT_MAX
                    ),
                    recordnum,
                ));
                break;
            }
        }
    }

    Ok(warnings)
}

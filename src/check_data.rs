//! Checks on data integrity/validity.
use std::collections::HashMap;
use std::env;
use std::fmt::Write;
use std::fs::OpenOptions;
use std::str::FromStr;

use chrono::{NaiveDate, NaiveDateTime};
use log::{Level, LevelFilter};
use oracle::Connection;
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};

use crate::{db, log_msg, CountError, CountKind, LaneDirection};

// If a count is bidirectional, the totals for both directions should be relatively proportional.
// One direction having less than this level is considered abnormal.
const DIR_PROPORTION_LOWER_BOUND: f32 = 0.40;
// Unusually high count for bicycles in a 15-minute period.
const BIKE_COUNT_MAX: u32 = 20;

/// Result of a particular check.
#[derive(Debug)]
struct CheckResult {
    level: Level,
    message: String,
}

/// Used for checking shares by class.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClassCountCheck {
    datetime: NaiveDateTime,
    lane: u8,
    dir: LaneDirection,
    c2: u32,
    c15: u32,
    total: u32,
}

/// Apply various data checks and log any issues found.
pub fn check(recordnum: u32, conn: &Connection) -> Result<(), CountError> {
    // Load file containing environment variables, panic if it doesn't exist.
    dotenvy::dotenv().expect("Unable to load .env file.");

    // Get env var for path where log will be, panic if it doesn't exist.
    let log_dir = env::var("LOG_DIR").expect("Unable to load log directory path from .env file.");
    // Set up logging, panic if it fails.
    let check_config = ConfigBuilder::new().set_time_format_rfc3339().build();
    let data_check_log = CombinedLogger::new(vec![
        TermLogger::new(
            LevelFilter::Debug,
            check_config.clone(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        ),
        WriteLogger::new(
            LevelFilter::Info,
            check_config,
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(format!("{log_dir}/data_check.log"))
                .expect("Could not open log file."),
        ),
    ]);

    // Determine what kind of count this is.
    let count_kind = match db::get_count_kind(conn, recordnum) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return Err(CountError::DataCheckError(
                "unable to identify type of count".to_string(),
            ));
        }
        Err(e) => {
            return Err(CountError::DbError(format!("{e}")));
        }
    };

    // Run checks according to kind of count.
    match count_kind {
        CountKind::Class => {
            match check_share_unclassed_vehicles(recordnum, conn) {
                Ok(v) if v.level == Level::Warn => {
                    log_msg(recordnum, &data_check_log, Level::Warn, &v.message, conn);
                }
                _ => (),
            }
            match check_share_class2_vehicles(recordnum, conn) {
                Ok(v) if v.level == Level::Warn => {
                    log_msg(recordnum, &data_check_log, Level::Warn, &v.message, conn);
                }
                _ => (),
            }
            match check_vehicle_dir_proportionality(recordnum, conn) {
                Ok(v) if v.level == Level::Warn => {
                    log_msg(recordnum, &data_check_log, Level::Warn, &v.message, conn);
                }
                _ => (),
            }
            match check_0_hours(recordnum, &count_kind, conn) {
                Ok(v) if v.level == Level::Warn => {
                    log_msg(recordnum, &data_check_log, Level::Warn, &v.message, conn);
                }
                _ => (),
            }
        }
        CountKind::Volume => {
            match check_vehicle_dir_proportionality(recordnum, conn) {
                Ok(v) if v.level == Level::Warn => {
                    log_msg(recordnum, &data_check_log, Level::Warn, &v.message, conn);
                }
                _ => (),
            }
            match check_0_hours(recordnum, &count_kind, conn) {
                Ok(v) if v.level == Level::Warn => {
                    log_msg(recordnum, &data_check_log, Level::Warn, &v.message, conn);
                }
                _ => (),
            }
        }
        CountKind::FifteenMinVolume => {
            match check_vehicle_dir_proportionality(recordnum, conn) {
                Ok(v) if v.level == Level::Warn => {
                    log_msg(recordnum, &data_check_log, Level::Warn, &v.message, conn);
                }
                _ => (),
            }
            match check_0_hours(recordnum, &count_kind, conn) {
                Ok(v) if v.level == Level::Warn => {
                    log_msg(recordnum, &data_check_log, Level::Warn, &v.message, conn);
                }
                _ => (),
            }
        }
        CountKind::Bicycle1
        | CountKind::Bicycle2
        | CountKind::Bicycle3
        | CountKind::Bicycle4
        | CountKind::Bicycle5
        | CountKind::Bicycle6 => {
            match check_bike_dir_proportionality(recordnum, conn) {
                Ok(v) if v.level == Level::Warn => {
                    log_msg(recordnum, &data_check_log, Level::Warn, &v.message, conn);
                }
                _ => (),
            }
            match check_excessive_bicycles(recordnum, conn) {
                Ok(v) if v.level == Level::Warn => {
                    log_msg(recordnum, &data_check_log, Level::Warn, &v.message, conn);
                }
                _ => (),
            }
            match check_0_hours(recordnum, &count_kind, conn) {
                Ok(v) if v.level == Level::Warn => {
                    log_msg(recordnum, &data_check_log, Level::Warn, &v.message, conn);
                }
                _ => (),
            }
        }
        _ => (),
    }

    Ok(())
}

/// Check if share of class 2 vehicles is too low.
fn check_share_class2_vehicles(
    recordnum: u32,
    conn: &Connection,
) -> Result<CheckResult, CountError> {
    let counts = get_c2_c15_total_counts(recordnum, conn)?;

    // Check share of class 2 of total.
    let c2_sum = counts.iter().map(|count| count.c2).sum::<u32>();
    let total_sum = counts.iter().map(|count| count.total).sum::<u32>();

    let c2_percent = c2_sum as f32 / total_sum as f32 * 100.0;

    if c2_percent < 75.0 {
        Ok(CheckResult {
            level: Level::Warn,
            message: format!("Class 2 vehicles are less than 75% ({c2_percent:.1}%) of total."),
        })
    } else {
        Ok(CheckResult {
            level: Level::Info,
            message: "Share of class 2 vehicles is within expectations".to_string(),
        })
    }
}

/// Check if share of unclassed vehicles is too high.
fn check_share_unclassed_vehicles(
    recordnum: u32,
    conn: &Connection,
) -> Result<CheckResult, CountError> {
    let counts = get_c2_c15_total_counts(recordnum, conn)?;

    // Check share of class 15 of total.
    let c15_sum = counts.iter().map(|count| count.c15).sum::<u32>();
    let total_sum = counts.iter().map(|count| count.total).sum::<u32>();

    let c15_percent = c15_sum as f32 / total_sum as f32 * 100.0;

    if c15_percent > 10.0 {
        Ok(CheckResult {
            level: Level::Warn,
            message: format!(
                "Unclassed vehicles are greater than 10% ({c15_percent:.1}%) of total."
            ),
        })
    } else {
        Ok(CheckResult {
            level: Level::Info,
            message: "Share of unclassed vehicles is within expectations".to_string(),
        })
    }
}

/// Check if motor vehicle counts have relatively even proportion of total per direction.
fn check_vehicle_dir_proportionality(
    recordnum: u32,
    conn: &Connection,
) -> Result<CheckResult, CountError> {
    let results = conn.query_as::<(u32, String)>(
        "select totalcount, cntdir from tc_volcount_new where recordnum = :1",
        &[&recordnum],
    )?;

    let mut count_by_dir = HashMap::new();
    for result in results {
        let (total, direction) = result?;
        *count_by_dir.entry(direction).or_insert(total) += total;
    }

    if count_by_dir.is_empty() {
        return Ok(CheckResult {
            level: Level::Info,
            message: "Count is empty".to_string(),
        });
    }

    let larger = count_by_dir.iter().max_by(|a, b| a.1.cmp(b.1)).unwrap();
    let smaller = count_by_dir.iter().min_by(|a, b| a.1.cmp(b.1)).unwrap();

    if count_by_dir.keys().len() > 1 {
        let total = smaller.1 + larger.1;
        let smaller_share = *smaller.1 as f32 / total as f32;
        let larger_share = *larger.1 as f32 / total as f32;
        if smaller_share < DIR_PROPORTION_LOWER_BOUND {
            let msg =  format!("Abnormal direction proportions: {} has {:.1}% of total, {} has {:.1}%. (Expectation is that proportions are no less/more than {}%/{}%.)",
                smaller.0,
                smaller_share * 100_f32,
                larger.0,
                larger_share * 100_f32,
                DIR_PROPORTION_LOWER_BOUND * 100_f32,
                100_f32 - DIR_PROPORTION_LOWER_BOUND * 100_f32);
            Ok(CheckResult {
                level: Level::Warn,
                message: msg,
            })
        } else {
            Ok(CheckResult {
                level: Level::Info,
                message: "Direction proportions is within expectations".to_string(),
            })
        }
    } else {
        Ok(CheckResult {
            level: Level::Info,
            message: "Skipping disproportional directionality check - count only one direction."
                .to_string(),
        })
    }
}

/// Check if bicycle counts have relatively even proportion of total per direction.
fn check_bike_dir_proportionality(
    recordnum: u32,
    conn: &Connection,
) -> Result<CheckResult, CountError> {
    // Check to see if count is bidirectional.
    let (cntdir, dir1, dir2) = conn.query_row_as::<(String, String, String)>(
        "select cntdir, indir, outdir from tc_header where recordnum = :1",
        &[&recordnum],
    )?;

    if cntdir == *"both" {
        let dir1_sum = conn.query_row_as::<u32>(
            "select sum(volume) from tc_bikecount_new where cntdir = :1 and recordnum = :2",
            &[&dir1, &recordnum],
        )?;
        let dir2_sum = conn.query_row_as::<u32>(
            "select sum(volume) from tc_bikecount_new where cntdir = :1 and recordnum = :2",
            &[&dir2, &recordnum],
        )?;

        let total = dir1_sum as f32 + dir2_sum as f32;
        let dir1_share = dir1_sum as f32 / total;
        let dir2_share = dir2_sum as f32 / total;

        if dir1_share < DIR_PROPORTION_LOWER_BOUND || dir2_share < DIR_PROPORTION_LOWER_BOUND {
            Ok(CheckResult {
                level: Level::Warn,
                message: format!("Abnormal direction proportions: {} has {:.1}% of total, {} has {:.1}%. (Expectation is that proportions are no less/more than {}%/{}%.)",
                            dir1,
                            dir1_share * 100_f32,
                            dir2,
                            dir2_share * 100_f32,
                            DIR_PROPORTION_LOWER_BOUND * 100_f32,
                            100_f32 - DIR_PROPORTION_LOWER_BOUND * 100_f32),
            })
        } else {
            Ok(CheckResult {
                level: Level::Info,
                message: "Direction proportions is within expectations".to_string(),
            })
        }
    } else {
        Ok(CheckResult {
            level: Level::Info,
            message: "Skipping disproportional directionality check - count only one direction."
                .to_string(),
        })
    }
}

/// Check if more than 1 consecutive 0-count/hour between 4am and 10pm for motor vehicles.
///
/// The table the data is pulled from depends on what kind of count it is.
fn check_0_hours(
    recordnum: u32,
    count_kind: &CountKind,
    conn: &Connection,
) -> Result<CheckResult, CountError> {
    let start_hour = 4;
    let end_hour = 22;

    // Set up the query for different tables.
    let queries = ["tc_bikecount_new", "tc_volcount_new"].into_iter().map(|table| {
    format!("select trunc(countdatetime, 'HH24'), cntdir, sum(volume) from {table} where recordnum = :1 and to_char(countdatetime, 'hh24') >= '{start_hour:02}' and to_char(countdatetime, 'hh24') <= '{end_hour}' group by trunc(countdatetime, 'hh24'), cntdir order by cntdir, trunc(countdatetime, 'hh24')")}).collect::<Vec<_>>();

    let results = match count_kind {
        CountKind::Bicycle1
        | CountKind::Bicycle2
        | CountKind::Bicycle3
        | CountKind::Bicycle4
        | CountKind::Bicycle5
        | CountKind::Bicycle6 => {
            conn.query_as::<(NaiveDateTime, String, u32)>(&queries[0], &[&recordnum])?
        }
        CountKind::Class | CountKind::Volume | CountKind::FifteenMinVolume => {
            conn.query_as::<(NaiveDateTime, String, u32)>(&queries[1], &[&recordnum])?
        }
        _ => {
            return Ok(CheckResult {
                level: Level::Info,
                message: "Consecutive zero hour check not implemented for {count_kind}."
                    .to_string(),
            })
        }
    };

    let mut consecutive_zeros = 0_u32;
    for result in results {
        let (_, _, volume) = result?;
        if volume == 0 {
            consecutive_zeros += 1;
        } else {
            consecutive_zeros = 0;
        }
        if consecutive_zeros > 1 {
            return Ok(CheckResult {
                level: Level::Warn,
                message: format!(
                    "Consecutive periods between the hours of {start_hour}:00 and {end_hour}:00 with zero volumes.",
                ),
            });
        }
    }
    // The loop above would have short-circuited if consecutive hours with 0 volume.
    Ok(CheckResult {
        level: Level::Info,
        message: "No counts with consecutive hourly periods of 0 volume counted.".to_string(),
    })
}

/// Check if there is an excessive number of bicycles in any 15-minute period.
fn check_excessive_bicycles(recordnum: u32, conn: &Connection) -> Result<CheckResult, CountError> {
    let results = conn.query_as::<(NaiveDateTime, String, u32)>(
        "select countdatetime, cntdir, volume from tc_bikecount_new where recordnum = :1 order by countdatetime",
        &[&recordnum],
    )?;

    let mut excessive_bicycles = vec![];

    for result in results {
        let (dt, direction, volume) = result?;
        if volume > BIKE_COUNT_MAX {
            excessive_bicycles.push((dt, volume, direction))
        }
    }

    if excessive_bicycles.is_empty() {
        Ok(CheckResult {
            level: Level::Info,
            message: "All counts under excessive threshold".to_string(),
        })
    } else {
        let excessive_bicycles =
            excessive_bicycles
                .iter()
                .fold(String::new(), |mut output, (dt, volume, direction)| {
                    let _ = write!(output, "{}: {} ({}); ", dt, volume, direction);
                    output
                });

        let message = format!("Found more than {BIKE_COUNT_MAX} bicycles counted in the following periods: {excessive_bicycles}");
        Ok(CheckResult {
            level: Level::Warn,
            message,
        })
    }
}

fn get_c2_c15_total_counts(
    recordnum: u32,
    conn: &Connection,
) -> Result<Vec<ClassCountCheck>, CountError> {
    let results = conn.query_as::<(NaiveDate, NaiveDateTime, u8, String, u32, u32, u32)>(
    "select countdate, counttime, countlane, cntdir, total, cars_and_tlrs, unclassified from tc_clacount_new where recordnum = :1",
    &[&recordnum],
)?;

    let mut counts = vec![];
    for result in results {
        let (count_date, count_time, lane, direction, total, c2, c15) = result?;
        let datetime = NaiveDateTime::new(count_date, count_time.time());
        counts.push(ClassCountCheck {
            datetime,
            lane,
            dir: LaneDirection::from_str(&direction).unwrap(),
            c2,
            c15,
            total,
        })
    }

    Ok(counts)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db;

    #[test]
    fn fifteen_min_bicycle_disproportionate_direction_found() {
        let (username, password) = db::get_creds();
        let pool = db::create_pool(username, password).unwrap();
        let conn = pool.get().unwrap();

        let result = check_bike_dir_proportionality(158971, &conn).unwrap();
        assert!(matches!(result.level, Level::Warn))
    }

    #[test]
    fn fifteen_min_bicycle_exessive() {
        let (username, password) = db::get_creds();
        let pool = db::create_pool(username, password).unwrap();
        let conn = pool.get().unwrap();

        let result = check_excessive_bicycles(111722, &conn).unwrap();
        assert!(matches!(result.level, Level::Warn))
    }

    #[test]
    fn bicycle_consecutive_0_hours_found() {
        let (username, password) = db::get_creds();
        let pool = db::create_pool(username, password).unwrap();
        let conn = pool.get().unwrap();
        let result = check_0_hours(167607, &CountKind::Bicycle2, &conn);
        let result = result.unwrap();
        assert!(matches!(result.level, Level::Warn))
    }
}

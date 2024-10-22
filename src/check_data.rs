/// Checks on data integrity/validity.
use std::collections::{BTreeMap, HashMap};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use log::{warn, Level};
use oracle::{sql_type::Timestamp, Connection};

use crate::{
    db::{self, LogEntry},
    CountError, Direction,
};

// If a count is bidirectional, the totals for both directions should be relatively proportional.
// One direction having less than this level is considered abnormal.
const DIR_PROPORTION_LOWER_BOUND: f32 = 0.40;
// Unusually high count for bicycles in a 15-minute period.
const BIKE_COUNT_MAX: u32 = 20;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClassCountCheck {
    datetime: NaiveDateTime,
    lane: u8,
    dir: Direction,
    c2: u32,
    c15: u32,
    total: u32,
}

pub fn check(recordnum: u32, conn: &Connection) -> Result<(), CountError> {
    // Determine what kind of count this is, in order to run the appropriate checks.
    let count_type = match db::get_count_type(conn, recordnum) {
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

    // Warn if share of unclassed vehicles is too high or class 2 is too low.
    if count_type == "Class" {
        let results = conn.query_as::<(Timestamp, Timestamp, u8, String, u32, u32, u32)>(
        "select countdate, counttime, countlane, ctdir, total, cars_and_tlrs, unclassified from tc_clacount where recordnum = :1",
        &[&recordnum],
    )?;

        let mut counts = vec![];
        for result in results {
            let (count_date, count_time, lane, direction, total, c2, c15) = result?;
            let datetime = NaiveDateTime::new(
                NaiveDate::from_ymd_opt(count_date.year(), count_date.month(), count_date.day())
                    .unwrap(),
                NaiveTime::from_hms_opt(count_time.hour(), count_time.minute(), 0).unwrap(),
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
            let msg = format!("Class 2 vehicles are less than 75% ({c2_percent:.1}%) of total.");
            warn!(target: "check", "{recordnum}: {msg}");
            db::insert_import_log_entry(conn, LogEntry::new(recordnum, msg, Level::Warn)).unwrap();
        }

        if c15_percent > 10.0 {
            let msg =
                format!("Unclassed vehicles are greater than 10% ({c15_percent:.1}%) of total.");
            warn!(target: "check", "{recordnum}: {msg}");
            db::insert_import_log_entry(conn, LogEntry::new(recordnum, msg, Level::Warn)).unwrap();
        }
    }

    // Warn if motor vehicle counts don't have relatively even proportion of total per direction.
    if ["Class", "Volume", "15 min Volume"].contains(&count_type.as_str()) {
        let results = conn.query_as::<(u32, String)>(
            "select totalcount, cntdir from tc_volcount where recordnum = :1",
            &[&recordnum],
        )?;

        let mut count_by_dir = HashMap::new();
        for result in results {
            let (total, direction) = result?;
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
                        let msg = format!("Abnormal direction proportions: {} has {:.1}% of total, {} has {:.1}%. (Expectation is that proportions are no less/more than {}%/{}%.)",
                            smaller.0,
                            smaller_share * 100_f32,
                            larger.0,
                            larger_share * 100_f32,
                            DIR_PROPORTION_LOWER_BOUND * 100_f32,
                            100_f32 - DIR_PROPORTION_LOWER_BOUND * 100_f32);
                        warn!(target: "check", "{recordnum}: {msg}");
                        db::insert_import_log_entry(
                            conn,
                            LogEntry::new(recordnum, msg, Level::Warn),
                        )
                        .unwrap();
                    }
                }
            }
        }
    }

    // Warn if more than 1 consecutive 0-count/hour between 4am and 10pm for motor vehicles.
    if ["Class", "Volume", "15 min Volume"].contains(&count_type.as_str()) {
        let results = conn.query_as::<(
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
            Option<u32>,
        )>(
            "select
                am4, am5, am6, am7, am8, am9, am10, am11, pm12, pm1, pm2, pm3, pm4, pm5, pm6,
                pm7, pm8, pm9, pm10
                from tc_volcount where recordnum = :1",
            &[&recordnum],
        )?;
        for result in results {
            let result = result?;
            let mut hourly = BTreeMap::new();
            hourly.insert("am4", result.0);
            hourly.insert("am5", result.1);
            hourly.insert("am6", result.2);
            hourly.insert("am7", result.3);
            hourly.insert("am8", result.4);
            hourly.insert("am9", result.5);
            hourly.insert("am10", result.6);
            hourly.insert("am11", result.7);
            hourly.insert("pm12", result.8);
            hourly.insert("pm1", result.9);
            hourly.insert("pm2", result.10);
            hourly.insert("pm3", result.11);
            hourly.insert("pm4", result.12);
            hourly.insert("pm5", result.13);
            hourly.insert("pm6", result.14);
            hourly.insert("pm7", result.15);
            hourly.insert("pm8", result.16);
            hourly.insert("pm9", result.17);
            hourly.insert("pm10", result.18);

            let mut consecutive_zeros = 0_u32;
            for (hour, count) in hourly {
                if count.is_some_and(|c| c == 0) {
                    consecutive_zeros += 1;
                } else {
                    consecutive_zeros = 0;
                }
                if consecutive_zeros > 1 {
                    let msg = format!("Consecutive period ({hour}) with 0 vehicles counted.");
                    warn!(target: "check", "{recordnum}: {msg}");
                    db::insert_import_log_entry(conn, LogEntry::new(recordnum, msg, Level::Warn))
                        .unwrap();
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
                let msg = format!(
                    "More than {BIKE_COUNT_MAX} in a 15-minute period for a bicycle count."
                );
                warn!(target: "check", "{recordnum}: {msg}");
                db::insert_import_log_entry(conn, LogEntry::new(recordnum, msg, Level::Warn))
                    .unwrap();
                break;
            }
        }
    }

    Ok(())
}

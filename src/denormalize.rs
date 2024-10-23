//! Denormalize count data.
//!
//! Denormalization is done in two ways:
//!  * data from some database tables (see [implementors of Denormalize][Denormalize#implementors])
//!    is transformed into the shape of the TC_VOLCOUNT table ([NonNormalVolCount]).
//!  * raw data, in the form of [IndividualVehicle]s, is
//!    [processed and transformed](create_non_normal_speedavg_count)
//!    into the shape of the TC_SPESUM table ([NonNormalAvgSpeedCount]).
use chrono::NaiveDate;
use oracle::{Connection, RowValue};

use crate::{intermediate::*, *};

/// A trait to handle denormalization.
pub trait Denormalize {
    /// The name of the table that the data will get denormalized from.
    const NORMALIZED_TABLE: &'static str;
    /// Field in table containing the direction of the count.
    const DIR_FIELD: &'static str;
    /// Field containing the (total) volume count.
    const VOL_FIELD: &'static str;

    /// Create denormalized volume counts from [`HourlyCount`]s.
    fn denormalize_vol_count(
        recordnum: u32,
        conn: &Connection,
    ) -> Result<Vec<NonNormalVolCount>, CountError> {
        let counts = hourly_counts(
            recordnum,
            Self::NORMALIZED_TABLE,
            Self::DIR_FIELD,
            Self::VOL_FIELD,
            conn,
        )?;

        let mut non_normal_vol_map: HashMap<NonNormalCountKey, NonNormalVolCountValue> =
            HashMap::new();

        if counts.is_empty() {
            return Ok(vec![]);
        }

        for count in counts {
            let key = NonNormalCountKey {
                recordnum: count.recordnum,
                date: count.datetime.date(),
                direction: count.dir,
                lane: count.lane,
            };

            // Add new entry if necessary, then insert data.
            non_normal_vol_map
                .entry(key)
                .and_modify(|c| {
                    c.totalcount = c
                        .totalcount
                        .map_or(Some(count.count), |c| Some(c + count.count));
                    match count.datetime.hour() {
                        0 => c.am12 = Some(count.count),
                        1 => c.am1 = Some(count.count),
                        2 => c.am2 = Some(count.count),
                        3 => c.am3 = Some(count.count),
                        4 => c.am4 = Some(count.count),
                        5 => c.am5 = Some(count.count),
                        6 => c.am6 = Some(count.count),
                        7 => c.am7 = Some(count.count),
                        8 => c.am8 = Some(count.count),
                        9 => c.am9 = Some(count.count),
                        10 => c.am10 = Some(count.count),
                        11 => c.am11 = Some(count.count),
                        12 => c.pm12 = Some(count.count),
                        13 => c.pm1 = Some(count.count),
                        14 => c.pm2 = Some(count.count),
                        15 => c.pm3 = Some(count.count),
                        16 => c.pm4 = Some(count.count),
                        17 => c.pm5 = Some(count.count),
                        18 => c.pm6 = Some(count.count),
                        19 => c.pm7 = Some(count.count),
                        20 => c.pm8 = Some(count.count),
                        21 => c.pm9 = Some(count.count),
                        22 => c.pm10 = Some(count.count),
                        23 => c.pm11 = Some(count.count),
                        _ => (),
                    };
                })
                .or_insert(NonNormalVolCountValue::first(&count));
        }
        // Convert HashMap to Vec of structs.
        let mut non_normal_vol_count = vec![];
        for (key, value) in non_normal_vol_map {
            non_normal_vol_count.push(NonNormalVolCount {
                recordnum: key.recordnum,
                date: key.date,
                direction: key.direction,
                lane: key.lane,
                setflag: None,
                totalcount: value.totalcount,
                am12: value.am12,
                am1: value.am1,
                am2: value.am2,
                am3: value.am3,
                am4: value.am4,
                am5: value.am5,
                am6: value.am6,
                am7: value.am7,
                am8: value.am8,
                am9: value.am9,
                am10: value.am10,
                am11: value.am11,
                pm12: value.pm12,
                pm1: value.pm1,
                pm2: value.pm2,
                pm3: value.pm3,
                pm4: value.pm4,
                pm5: value.pm5,
                pm6: value.pm6,
                pm7: value.pm7,
                pm8: value.pm8,
                pm9: value.pm9,
                pm10: value.pm10,
                pm11: value.pm11,
            })
        }
        Ok(non_normal_vol_count)
    }
}

impl Denormalize for TimeBinnedVehicleClassCount {
    const NORMALIZED_TABLE: &'static str = "tc_clacount";
    const DIR_FIELD: &'static str = "ctdir";
    const VOL_FIELD: &'static str = "total";
}

impl Denormalize for FifteenMinuteVehicle {
    const NORMALIZED_TABLE: &'static str = "tc_15minvolcount";
    const DIR_FIELD: &'static str = "cntdir";
    const VOL_FIELD: &'static str = "volcount";
}

/// Counts aggregated by hour.
///
/// The datetime is truncated to the top of the hour - 13:00, 14:00, etc.
#[derive(Debug, Clone)]
pub struct HourlyCount {
    pub recordnum: u32,
    pub datetime: NaiveDateTime,
    pub count: u32,
    pub dir: LaneDirection,
    pub lane: u8,
}

/// Non-normalized volume counts.
///
/// Hourly fields are `Option` because traffic counts aren't done from 12am one day to 12am the
/// the following day - can start and stop at any time.
#[derive(Debug, Clone, RowValue)]
pub struct NonNormalVolCount {
    pub recordnum: u32,
    #[row_value(rename = "countdate")]
    pub date: NaiveDate,
    #[row_value(rename = "cntdir")]
    pub direction: LaneDirection,
    #[row_value(rename = "countlane")]
    pub lane: u8,
    pub setflag: Option<i8>,
    pub totalcount: Option<u32>,
    pub am12: Option<u32>,
    pub am1: Option<u32>,
    pub am2: Option<u32>,
    pub am3: Option<u32>,
    pub am4: Option<u32>,
    pub am5: Option<u32>,
    pub am6: Option<u32>,
    pub am7: Option<u32>,
    pub am8: Option<u32>,
    pub am9: Option<u32>,
    pub am10: Option<u32>,
    pub am11: Option<u32>,
    pub pm12: Option<u32>,
    pub pm1: Option<u32>,
    pub pm2: Option<u32>,
    pub pm3: Option<u32>,
    pub pm4: Option<u32>,
    pub pm5: Option<u32>,
    pub pm6: Option<u32>,
    pub pm7: Option<u32>,
    pub pm8: Option<u32>,
    pub pm9: Option<u32>,
    pub pm10: Option<u32>,
    pub pm11: Option<u32>,
}

/// Non-normalized average speed counts.
///
/// Hourly fields are `Option` because traffic counts aren't done from 12am one day to 12am the
/// the following day - can start and stop at any time.
#[derive(Debug, Clone, RowValue)]
pub struct NonNormalAvgSpeedCount {
    pub recordnum: u32,
    #[row_value(rename = "countdate")]
    pub date: NaiveDate,
    #[row_value(rename = "ctdir")]
    pub direction: LaneDirection,
    #[row_value(rename = "countlane")]
    pub lane: u8,
    pub am12: Option<f32>,
    pub am1: Option<f32>,
    pub am2: Option<f32>,
    pub am3: Option<f32>,
    pub am4: Option<f32>,
    pub am5: Option<f32>,
    pub am6: Option<f32>,
    pub am7: Option<f32>,
    pub am8: Option<f32>,
    pub am9: Option<f32>,
    pub am10: Option<f32>,
    pub am11: Option<f32>,
    pub pm12: Option<f32>,
    pub pm1: Option<f32>,
    pub pm2: Option<f32>,
    pub pm3: Option<f32>,
    pub pm4: Option<f32>,
    pub pm5: Option<f32>,
    pub pm6: Option<f32>,
    pub pm7: Option<f32>,
    pub pm8: Option<f32>,
    pub pm9: Option<f32>,
    pub pm10: Option<f32>,
    pub pm11: Option<f32>,
}

/// Create non-normalized average speed counts from [`IndividualVehicle`]s.
pub fn create_non_normal_speedavg_count(
    metadata: FieldMetadata,
    counts: Vec<IndividualVehicle>,
) -> Vec<NonNormalAvgSpeedCount> {
    let mut non_normal_raw_speed_map: HashMap<NonNormalCountKey, NonNormalRawSpeedValue> =
        HashMap::new();
    let mut non_normal_avg_speed_map: HashMap<NonNormalCountKey, NonNormalAvgSpeedValue> =
        HashMap::default();

    if counts.is_empty() {
        return vec![];
    }

    // Collect all the speeds per fields in key.
    for count in counts {
        // Get the direction from the lane of count/metadata of filename.
        let direction = match count.lane {
            1 => metadata.directions.direction1,
            2 => metadata.directions.direction2.unwrap(),
            3 => metadata.directions.direction3.unwrap(),
            _ => {
                error!("Unable to determine lane/direction.");
                continue;
            }
        };

        let key = NonNormalCountKey {
            recordnum: metadata.recordnum,
            date: count.datetime.date(),
            direction,
            lane: count.lane,
        };

        // Add new entry if necessary, then insert data.
        non_normal_raw_speed_map
            .entry(key)
            .and_modify(|c| {
                match count.datetime.time().hour() {
                    0 => c.am12.push(count.speed),
                    1 => c.am1.push(count.speed),
                    2 => c.am2.push(count.speed),
                    3 => c.am3.push(count.speed),
                    4 => c.am4.push(count.speed),
                    5 => c.am5.push(count.speed),
                    6 => c.am6.push(count.speed),
                    7 => c.am7.push(count.speed),
                    8 => c.am8.push(count.speed),
                    9 => c.am9.push(count.speed),
                    10 => c.am10.push(count.speed),
                    11 => c.am11.push(count.speed),
                    12 => c.pm12.push(count.speed),
                    13 => c.pm1.push(count.speed),
                    14 => c.pm2.push(count.speed),
                    15 => c.pm3.push(count.speed),
                    16 => c.pm4.push(count.speed),
                    17 => c.pm5.push(count.speed),
                    18 => c.pm6.push(count.speed),
                    19 => c.pm7.push(count.speed),
                    20 => c.pm8.push(count.speed),
                    21 => c.pm9.push(count.speed),
                    22 => c.pm10.push(count.speed),
                    23 => c.pm11.push(count.speed),
                    _ => (),
                };
            })
            .or_insert(NonNormalRawSpeedValue::first(
                count.datetime.time().hour(),
                count.speed,
            ));
    }

    // Calculate the average speed per date/hour from the vecs.
    for (key, value) in non_normal_raw_speed_map {
        if !value.am12.is_empty() {
            let average = value.am12.iter().sum::<f32>() / value.am12.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c: &mut NonNormalAvgSpeedValue| c.am12 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("am12", average));
        }
        if !value.am1.is_empty() {
            let average = value.am1.iter().sum::<f32>() / value.am1.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.am1 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("am1", average));
        }
        if !value.am2.is_empty() {
            let average = value.am2.iter().sum::<f32>() / value.am2.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.am2 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("am2", average));
        }
        if !value.am3.is_empty() {
            let average = value.am3.iter().sum::<f32>() / value.am3.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.am3 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("am3", average));
        }
        if !value.am4.is_empty() {
            let average = value.am4.iter().sum::<f32>() / value.am4.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.am4 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("am4", average));
        }
        if !value.am5.is_empty() {
            let average = value.am5.iter().sum::<f32>() / value.am5.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.am5 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("am5", average));
        }
        if !value.am6.is_empty() {
            let average = value.am6.iter().sum::<f32>() / value.am6.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.am6 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("am6", average));
        }
        if !value.am7.is_empty() {
            let average = value.am7.iter().sum::<f32>() / value.am7.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.am7 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("am7", average));
        }
        if !value.am8.is_empty() {
            let average = value.am8.iter().sum::<f32>() / value.am8.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.am8 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("am8", average));
        }
        if !value.am9.is_empty() {
            let average = value.am9.iter().sum::<f32>() / value.am9.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.am9 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("am9", average));
        }
        if !value.am10.is_empty() {
            let average = value.am10.iter().sum::<f32>() / value.am10.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.am10 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("am10", average));
        }
        if !value.am11.is_empty() {
            let average = value.am11.iter().sum::<f32>() / value.am11.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.am11 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("am11", average));
        }
        if !value.pm12.is_empty() {
            let average = value.pm12.iter().sum::<f32>() / value.pm12.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.pm12 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("pm12", average));
        }
        if !value.pm1.is_empty() {
            let average = value.pm1.iter().sum::<f32>() / value.pm1.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.pm1 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("pm1", average));
        }
        if !value.pm2.is_empty() {
            let average = value.pm2.iter().sum::<f32>() / value.pm2.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.pm2 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("pm2", average));
        }
        if !value.pm3.is_empty() {
            let average = value.pm3.iter().sum::<f32>() / value.pm3.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.pm3 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("pm3", average));
        }
        if !value.pm4.is_empty() {
            let average = value.pm4.iter().sum::<f32>() / value.pm4.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.pm4 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("pm4", average));
        }
        if !value.pm5.is_empty() {
            let average = value.pm5.iter().sum::<f32>() / value.pm5.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.pm5 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("pm5", average));
        }
        if !value.pm6.is_empty() {
            let average = value.pm6.iter().sum::<f32>() / value.pm6.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.pm6 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("pm6", average));
        }
        if !value.pm7.is_empty() {
            let average = value.pm7.iter().sum::<f32>() / value.pm7.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.pm7 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("pm7", average));
        }
        if !value.pm8.is_empty() {
            let average = value.pm8.iter().sum::<f32>() / value.pm8.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.pm8 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("pm8", average));
        }
        if !value.pm9.is_empty() {
            let average = value.pm9.iter().sum::<f32>() / value.pm9.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.pm9 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("pm9", average));
        }
        if !value.pm10.is_empty() {
            let average = value.pm10.iter().sum::<f32>() / value.pm10.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.pm10 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("pm10", average));
        }
        if !value.pm11.is_empty() {
            let average = value.pm11.iter().sum::<f32>() / value.pm11.len() as f32;
            non_normal_avg_speed_map
                .entry(key)
                .and_modify(|c| c.pm11 = Some(average))
                .or_insert(NonNormalAvgSpeedValue::first("pm11", average));
        }
    }

    // Convert HashMap to Vec of structs.
    let mut non_normal_speed_avg_count = vec![];
    for (key, value) in non_normal_avg_speed_map {
        non_normal_speed_avg_count.push(NonNormalAvgSpeedCount {
            recordnum: key.recordnum,
            date: key.date,
            direction: key.direction,
            lane: key.lane,
            am12: value.am12,
            am1: value.am1,
            am2: value.am2,
            am3: value.am3,
            am4: value.am4,
            am5: value.am5,
            am6: value.am6,
            am7: value.am7,
            am8: value.am8,
            am9: value.am9,
            am10: value.am10,
            am11: value.am11,
            pm12: value.pm12,
            pm1: value.pm1,
            pm2: value.pm2,
            pm3: value.pm3,
            pm4: value.pm4,
            pm5: value.pm5,
            pm6: value.pm6,
            pm7: value.pm7,
            pm8: value.pm8,
            pm9: value.pm9,
            pm10: value.pm10,
            pm11: value.pm11,
        })
    }
    non_normal_speed_avg_count
}

/// Get hourly counts from a database table.
pub fn hourly_counts<'a>(
    recordnum: u32,
    table: &'a str,
    dir_field: &'a str,
    vol_field: &'a str,
    conn: &Connection,
) -> Result<Vec<HourlyCount>, CountError> {
    let results = match conn.query_as::<(NaiveDateTime, NaiveDate, u32, String, u32)>(
        &format!(
            "select TRUNC(counttime, 'HH24'), countdate, sum({}), {}, countlane 
                from {} 
                where recordnum = :1 
                group by (countdate, trunc(counttime, 'HH24')), {}, countlane 
                order by countdate",
            &vol_field, &dir_field, &table, &dir_field
        ),
        &[&recordnum],
    ) {
        Ok(v) => v,
        Err(_) => {
            return Err(CountError::DbError(format!(
                "{recordnum} not found in {table}"
            )))
        }
    };

    let mut hourly_counts = vec![];
    for result in results {
        let (counttime, countdate, count, dir, lane) = result?;

        let datetime = NaiveDateTime::new(countdate, counttime.time());

        hourly_counts.push(HourlyCount {
            recordnum,
            datetime,
            count,
            dir: LaneDirection::from_str(&dir).unwrap(),
            lane: lane as u8,
        });
    }

    Ok(hourly_counts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{create_pool, get_creds};

    #[ignore]
    #[test]
    fn denormalize_vol_count_correct_num_records_and_total_count_166905() {
        let (username, password) = get_creds();
        let pool = create_pool(username, password).unwrap();
        let conn = pool.get().unwrap();

        // two directions, two lanes
        let mut non_normal_count =
            TimeBinnedVehicleClassCount::denormalize_vol_count(166905, &conn).unwrap();
        assert_eq!(non_normal_count.len(), 6);

        // Sort by date, and then lane, so elements of the vec are in an expected order to test.
        non_normal_count.sort_unstable_by_key(|count| (count.date, count.lane));

        // Ensure order is what we expect/count starts at correct times.
        assert_eq!(
            non_normal_count[0].date,
            NaiveDate::parse_from_str("2023-11-06", "%Y-%m-%d").unwrap()
        );
        assert!(non_normal_count[0].am9.is_none());
        assert!(non_normal_count[0].am10.is_some());
        assert_eq!(non_normal_count[0].direction, LaneDirection::East);
        assert_eq!(non_normal_count[0].lane, 1);

        assert_eq!(
            non_normal_count[1].date,
            NaiveDate::parse_from_str("2023-11-06", "%Y-%m-%d").unwrap()
        );
        assert!(non_normal_count[1].am9.is_none());
        assert!(non_normal_count[1].am10.is_some());
        assert_eq!(non_normal_count[1].direction, LaneDirection::West);
        assert_eq!(non_normal_count[1].lane, 2);

        assert!(non_normal_count[4].am10.is_some());
        assert!(non_normal_count[4].am11.is_none());
        assert_eq!(
            non_normal_count[5].date,
            NaiveDate::parse_from_str("2023-11-08", "%Y-%m-%d").unwrap()
        );
        assert!(non_normal_count[5].am10.is_some());
        assert!(non_normal_count[5].am11.is_none());
        assert_eq!(non_normal_count[5].direction, LaneDirection::West);
        assert_eq!(non_normal_count[5].lane, 2);

        // Test total counts.
        assert_eq!(
            non_normal_count[0].totalcount.unwrap() + non_normal_count[1].totalcount.unwrap(),
            2897
        );
        assert_eq!(
            non_normal_count[2].totalcount.unwrap() + non_normal_count[3].totalcount.unwrap(),
            4450
        );
        assert_eq!(
            non_normal_count[4].totalcount.unwrap() + non_normal_count[5].totalcount.unwrap(),
            1359
        );
    }

    #[ignore]
    #[test]
    fn denormalize_vol_count_correct_num_records_and_total_count_165367() {
        let (username, password) = get_creds();
        let pool = create_pool(username, password).unwrap();
        let conn = pool.get().unwrap();

        // one direction, two lanes
        let mut non_normal_count =
            TimeBinnedVehicleClassCount::denormalize_vol_count(165367, &conn).unwrap();
        assert_eq!(non_normal_count.len(), 10);

        // Sort by date, and then lane, so elements of the vec are in an expected order to test.
        non_normal_count.sort_unstable_by_key(|count| (count.date, count.lane));

        // Ensure order is what we expect/count starts at correct times.
        assert_eq!(
            non_normal_count[0].date,
            NaiveDate::parse_from_str("2023-11-06", "%Y-%m-%d").unwrap()
        );
        assert!(non_normal_count[0].am10.is_none());
        assert!(non_normal_count[0].am11.is_some());
        assert_eq!(non_normal_count[0].direction, LaneDirection::East);
        assert_eq!(non_normal_count[0].lane, 1);

        assert_eq!(
            non_normal_count[1].date,
            NaiveDate::parse_from_str("2023-11-06", "%Y-%m-%d").unwrap()
        );
        assert!(non_normal_count[1].am10.is_none());
        assert!(non_normal_count[1].am11.is_some());
        assert_eq!(non_normal_count[1].direction, LaneDirection::East);
        assert_eq!(non_normal_count[1].lane, 2);

        assert_eq!(
            non_normal_count[8].date,
            NaiveDate::parse_from_str("2023-11-10", "%Y-%m-%d").unwrap()
        );
        assert!(non_normal_count[8].am10.is_some());
        assert!(non_normal_count[8].am11.is_none());
        assert_eq!(non_normal_count[8].direction, LaneDirection::East);
        assert_eq!(non_normal_count[8].lane, 1);

        assert_eq!(
            non_normal_count[9].date,
            NaiveDate::parse_from_str("2023-11-10", "%Y-%m-%d").unwrap()
        );
        assert!(non_normal_count[9].am10.is_some());
        assert!(non_normal_count[9].am11.is_none());
        assert_eq!(non_normal_count[9].direction, LaneDirection::East);
        assert_eq!(non_normal_count[9].lane, 2);

        // Test total counts.
        assert_eq!(
            non_normal_count[0].totalcount.unwrap() + non_normal_count[1].totalcount.unwrap(),
            8712
        );
        assert_eq!(
            non_normal_count[2].totalcount.unwrap() + non_normal_count[3].totalcount.unwrap(),
            14751
        );
        assert_eq!(
            non_normal_count[4].totalcount.unwrap() + non_normal_count[5].totalcount.unwrap(),
            15298
        );
        assert_eq!(
            non_normal_count[6].totalcount.unwrap() + non_normal_count[7].totalcount.unwrap(),
            15379
        );
        assert_eq!(
            non_normal_count[8].totalcount.unwrap() + non_normal_count[9].totalcount.unwrap(),
            4278
        );
    }
}

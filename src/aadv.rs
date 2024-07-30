//! Calculate average annual daily volumes and insert them into the database.

use time::{Date, OffsetDateTime};

use crate::{db::YYYY_MM_DD_FMT, *};

/// A trait for calculating and inserting annual average daily volume.
pub trait Aadv {
    /// The name of the table in the db that this corresponds to. Must be time-binned count.
    const BINNED_TABLE: &'static str;
    /// Field in BINNED_TABLE containing the total count for the period.
    const TOTAL_FIELD: &'static str;
    /// Field in BINNED_TABLE with recordnum.
    const BINNED_RECORDNUM_FIELD: &'static str;
    /// Tables that store data in rows per direction (TC_CLACOUNT, TC_15MINVOLCOUNT) will only use
    /// the first element of this tuple, while those that store directions and total in each row
    /// (TC_BIKECOUNT, TC_PEDCOUNT) will use both.    
    const COUNT_DIR_FIELD: (&'static str, Option<&'static str>);
    /// Table containing factors for annual average daily volume calculation.
    const FACTOR_TABLE: &'static str;

    /// Get dates of full-day counts.
    fn get_full_dates(recordnum: u32, conn: &Connection) -> Result<Vec<Date>, CountError> {
        let mut dates = vec![];

        // Although records inserted since the beginning of the use of this library use full
        // datetime for the counttime field, previous records split date into countdate and time
        // into counttime (with no date/default date stored in countime), so we have to use both
        // and get individual component from each for backwards compatibility.
        let results = conn
            .query_as::<(Timestamp, Timestamp)>(
                &format!("select countdate, counttime from {} where {} = :1 order by countdate ASC, counttime ASC", &Self::BINNED_TABLE, &Self::BINNED_RECORDNUM_FIELD),
                &[&recordnum],
            )?;

        let results = results
            .map(|result| {
                let result = result.unwrap();
                let date = result.0;
                let time = result.1;
                Timestamp::new(
                    date.year(),
                    date.month(),
                    date.day(),
                    time.hour(),
                    time.minute(),
                    time.second(),
                    0,
                )
            })
            .collect::<Vec<Timestamp>>();

        if results.is_empty() {
            return Ok(dates);
        }

        // Get first and last datetimes.
        let first_dt = *results.first().unwrap();
        let last_dt = *results.last().unwrap();

        // The first actual day may be an incomplete date, but use this as a starting point.
        let mut first_full_date = Date::parse(
            &format!(
                "{}-{}-{}",
                first_dt.year(),
                first_dt.month(),
                first_dt.day()
            ),
            YYYY_MM_DD_FMT,
        )
        .unwrap();

        if first_dt.hour() != 0 {
            first_full_date = first_full_date.saturating_add(time::Duration::DAY);
        }

        // Determine the interval we are working with - hourly or fifteen-minute, based on that
        // first full day of data.
        // Determine the minute (0 or 45) to use to determine the last interval, ie. should we
        // use 11:00pm (hourly) or 11:45pm (fifteen-min)?
        // This is needed in order to determine the last day.
        let minute_to_use = match results
            .into_iter()
            .filter(|result| {
                result.year() == first_full_date.year()
                    && result.month() == first_full_date.month() as u32
                    && result.day() == first_full_date.day() as u32
            })
            .count()
        {
            24 | 48 => 0,   // 1 dir = 24, 2 dirs = 48
            96 | 192 => 45, // 1 dir = 96, 2 dirs = 192
            _ => return Err(CountError::BadIntervalCount),
        };

        // Use last day (regardless if full or not) as starting point to determine last full day.
        let mut last_full_date = Date::parse(
            &format!("{}-{}-{}", last_dt.year(), last_dt.month(), last_dt.day()),
            YYYY_MM_DD_FMT,
        )
        .unwrap();

        if last_dt.hour() != 23 || last_dt.minute() != minute_to_use {
            last_full_date = last_full_date.saturating_sub(time::Duration::DAY);
        }

        // Get all dates between first and last, inclusive.
        dates.push(first_full_date);
        if first_full_date != last_full_date {
            let mut next_day = first_full_date.saturating_add(time::Duration::DAY);
            while next_day != last_full_date {
                dates.push(next_day);
                next_day = next_day.saturating_add(time::Duration::DAY);
            }
            dates.push(last_full_date);
        }

        Ok(dates)
    }

    /// Get totals by date.
    fn get_total_by_date(
        recordnum: u32,
        conn: &Connection,
    ) -> Result<HashMap<(Date, Option<Direction>), usize>, CountError> {
        // Get dates that have full counts so we only get totals for them.
        let dates = Self::get_full_dates(recordnum, conn)?;

        let results = conn.query_as::<(Timestamp, usize, Option<String>)>(
            &format!(
                "select countdate, sum({}), {} from {} where {} = :1 group by countdate, {}",
                &Self::TOTAL_FIELD,
                &Self::COUNT_DIR_FIELD.0,
                &Self::BINNED_TABLE,
                &Self::BINNED_RECORDNUM_FIELD,
                &Self::COUNT_DIR_FIELD.0,
            ),
            &[&recordnum],
        )?;

        // Create hashmap to collect the total.
        // When the Direction is None in the key, that is the overall total (no directionality)
        // for the date, otherwise its for a particular Direction.
        let mut totals: HashMap<(Date, Option<Direction>), usize> = HashMap::new();
        for result in results {
            let (date, total, direction) = result?;
            let date = Date::parse(
                &format!("{}-{}-{}", date.year(), date.month(), date.day(),),
                YYYY_MM_DD_FMT,
            )
            .unwrap();

            // Don't include any non-full dates.
            if !dates.contains(&date) {
                continue;
            }

            // Insert value for each date/direction.
            if let Some(v) = direction {
                totals.insert((date, Some(Direction::from_string(v)?)), total);
            }

            // Insert or update value date/overall.
            totals
                .entry((date, None))
                .and_modify(|overall_total| *overall_total += total)
                .or_insert(total);
        }

        Ok(totals)
    }

    /// Calculate annual average daily volume.
    fn calculate_aadv(
        recordnum: u32,
        conn: &Connection,
    ) -> Result<HashMap<Option<Direction>, f32>, CountError>;

    // Insert/update the set of AADVs (per direction/overall) into the database.
    fn insert_aadv(recordnum: u32, conn: &Connection) -> Result<(), CountError> {
        let aadv = &Self::calculate_aadv(recordnum, conn)?;
        let date = match OffsetDateTime::now_local() {
            Ok(v) => v.date(),
            Err(_) => OffsetDateTime::now_utc().date(), // fallback to UTC
        };

        let date = Timestamp::new(
            date.year(),
            date.month() as u32,
            date.day() as u32,
            0,
            0,
            0,
            0,
        );

        // Delete any existing AADVs for same recordnum and date
        if conn.execute("delete from tc_aadv where recordnum = :1 and date_calculated = TO_CHAR(:2, 'DD-MON-YY')", &[&recordnum, &date]).is_ok() {
            conn.commit()?;
        };

        for (direction, aadv) in aadv {
            let direction = direction.map(|v| format!("{v}"));
            conn.execute(
                "insert into tc_aadv (recordnum, aadv, direction, date_calculated) VALUES (:1, :2, :3, :4)",
                &[&recordnum, aadv, &direction, &date],
            )?;
        }
        conn.commit()?;
        Ok(())
    }
}

impl Aadv for TimeBinnedVehicleClassCount {
    const BINNED_TABLE: &'static str = "tc_clacount";
    const TOTAL_FIELD: &'static str = "total";
    const BINNED_RECORDNUM_FIELD: &'static str = "recordnum";
    const COUNT_DIR_FIELD: (&'static str, Option<&'static str>) = ("ctdir", None);
    const FACTOR_TABLE: &'static str = "tc_factor";

    fn calculate_aadv(
        recordnum: u32,
        conn: &Connection,
    ) -> Result<HashMap<Option<Direction>, f32>, CountError> {
        // Get day counts for full days.
        let day_counts = Self::get_total_by_date(recordnum, conn)?;

        // Get additional fields required to get factors from two other tables.
        // mcd contains state code
        // fc is "road functional classification"
        let (mcd, fc, count_type) = match conn.query_row_as::<(String, u8, String)>(
            "select mcd, fc, type from TC_HEADER where recordnum = :1",
            &[&recordnum],
        ) {
            Ok(v) => v,
            Err(_) => {
                return Err(CountError::DbError(format!(
                    "{recordnum} not found in tc_header table"
                )))
            }
        };

        // Set column name for factor from factor table.
        let season_factor_col = if mcd.starts_with("42") {
            "pafactor"
        } else if mcd.starts_with("34") {
            "njfactor"
        } else {
            return Err(CountError::InvalidMcd(mcd));
        };

        // Get equipment factor, if any, from the TC_COUNTTYPE table.
        let equipment_factor = conn.query_row_as::<Option<f32>>(
            "select factor2 from tc_counttype where counttype = :1",
            &[&count_type],
        )?;

        let mut daily_aadv: HashMap<(Date, Option<Direction>), f32> = HashMap::new();

        for ((date, direction), total) in day_counts {
            // Get season factor from factor table. No need to get axle factor, as that
            // is only for non-class (and non-loop) types of counts.
            let season_factor = conn.query_row_as::<f32>(
                &format!(
                    "select {} from {} WHERE fc = :1 and year = :2 and month = :3 and dayofweek = :4",
                    season_factor_col,
                    Self::FACTOR_TABLE,
                ),
                &[
                    &fc,
                    &date.year(),
                    &(date.month() as u32),
                    &(date.weekday().number_from_sunday() as u32), // DVRPC uses 1-7 for SUN to SAT
                ],
            )?;

            match equipment_factor {
                None => daily_aadv.insert((date, direction), total as f32 * season_factor),
                Some(v) => daily_aadv.insert((date, direction), total as f32 * season_factor * v),
            };
        }

        // Determine the divisor by which we'll average the counts.
        // First, determine number of unique Option<Direction>s there are - will be 1, 2, or 3.
        // (Old counts have no directionality and so just 1, new counts have at least two (one
        // direction and no direction), but could have three (bidirectional and no direction).)
        let directions_per_day = daily_aadv
            .keys()
            .map(|(_date, direction)| direction)
            .collect::<HashSet<_>>();
        let divisor = (daily_aadv.len() / directions_per_day.len()) as f32;

        // Average totals from each day over each Option<Direction>.
        let mut aadv = HashMap::new();
        for direction in directions_per_day {
            let aadv_per_dir: f32 = daily_aadv
                .iter()
                .filter(|((_date, dir), _total)| dir == direction)
                .map(|((_date, _dir), total)| total)
                .sum();
            aadv.insert(*direction, aadv_per_dir / divisor);
        }

        Ok(aadv)
    }
}

impl Aadv for FifteenMinuteVehicle {
    const BINNED_TABLE: &'static str = "tc_15minvolcount";
    const TOTAL_FIELD: &'static str = "volcount";
    const BINNED_RECORDNUM_FIELD: &'static str = "recordnum";
    const COUNT_DIR_FIELD: (&'static str, Option<&'static str>) = ("cntdir", None);
    const FACTOR_TABLE: &'static str = "tc_factor";

    fn calculate_aadv(
        recordnum: u32,
        conn: &Connection,
    ) -> Result<HashMap<Option<Direction>, f32>, CountError> {
        // Get day counts for full days.
        let day_counts = Self::get_total_by_date(recordnum, conn)?;

        // Get additional fields required to get factors from two other tables.
        // mcd contains state code
        // fc is "road functional classification"
        let (mcd, fc, count_type) = match conn.query_row_as::<(String, u8, String)>(
            "select mcd, fc, type from TC_HEADER where recordnum = :1",
            &[&recordnum],
        ) {
            Ok(v) => v,
            Err(_) => {
                return Err(CountError::DbError(format!(
                    "{recordnum} not found in tc_header table"
                )))
            }
        };

        // Set column names for factors from factor table.
        let (season_factor_col, axle_factor_col) = if mcd.starts_with("42") {
            ("pafactor", "paaxle")
        } else if mcd.starts_with("34") {
            ("njfactor", "njaxle")
        } else {
            return Err(CountError::InvalidMcd(mcd));
        };

        // Get equipment factor, if any, from the TC_COUNTTYPE table.
        let equipment_factor = conn.query_row_as::<Option<f32>>(
            "select factor2 from tc_counttype where counttype = :1",
            &[&count_type],
        )?;

        let mut daily_aadv: HashMap<(Date, Option<Direction>), f32> = HashMap::new();

        for ((date, direction), total) in day_counts {
            // Get season and axle factors from factor table.
            let (season_factor, axle_factor) = conn.query_row_as::<(f32, f32)>(
                &format!(
                    "select {}, {} from {} WHERE fc = :1 and year = :2 and month = :3 and dayofweek = :4",
                    season_factor_col,
                    axle_factor_col,
                    Self::FACTOR_TABLE,
                ),
                &[
                    &fc,
                    &date.year(),
                    &(date.month() as u32),
                    &(date.weekday().number_from_sunday() as u32), // DVRPC uses 1-7 for SUN to SAT
                ],
            )?;

            match equipment_factor {
                None => daily_aadv.insert(
                    (date, direction),
                    total as f32 * season_factor * axle_factor,
                ),
                Some(v) => daily_aadv.insert(
                    (date, direction),
                    total as f32 * season_factor * axle_factor * v,
                ),
            };
        }

        // Determine the divisor by which we'll average the counts.
        // First, determine number of unique Option<Direction>s there are - will be 1, 2, or 3.
        // (Old counts have no directionality and so just 1, new counts have at least two (one
        // direction and no direction), but could have three (bidirectional and no direction).)
        let directions_per_day = daily_aadv
            .keys()
            .map(|(_date, direction)| direction)
            .collect::<HashSet<_>>();
        let divisor = (daily_aadv.len() / directions_per_day.len()) as f32;

        // Average totals from each day over each Option<Direction>.
        let mut aadv = HashMap::new();
        for direction in directions_per_day {
            let aadv_per_dir: f32 = daily_aadv
                .iter()
                .filter(|((_date, dir), _total)| dir == direction)
                .map(|((_date, _dir), total)| total)
                .sum();
            aadv.insert(*direction, aadv_per_dir / divisor);
        }

        Ok(aadv)
    }
}

impl Aadv for FifteenMinuteBicycle {
    const BINNED_TABLE: &'static str = "tc_bikecount";
    const TOTAL_FIELD: &'static str = "total";
    const BINNED_RECORDNUM_FIELD: &'static str = "dvrpcnum";
    const COUNT_DIR_FIELD: (&'static str, Option<&'static str>) = ("INCOUNT", Some("outcount"));
    const FACTOR_TABLE: &'static str = "tc_bikefactor";

    fn get_total_by_date(
        recordnum: u32,
        conn: &Connection,
    ) -> Result<HashMap<(Date, Option<Direction>), usize>, CountError> {
        let dates = Self::get_full_dates(recordnum, conn)?;
        get_total_by_date_bike_ped(
            recordnum,
            dates,
            Self::TOTAL_FIELD,
            Self::BINNED_TABLE,
            Self::COUNT_DIR_FIELD.0,
            Self::COUNT_DIR_FIELD.1.unwrap(),
            Self::BINNED_RECORDNUM_FIELD,
            conn,
        )
    }

    fn calculate_aadv(
        recordnum: u32,
        conn: &Connection,
    ) -> Result<HashMap<Option<Direction>, f32>, CountError> {
        // Get day counts for full days.
        let day_counts = Self::get_total_by_date(recordnum, conn)?;

        // Get additional fields required to get factors from two other tables.
        let (bikepedgroup, count_type) = match conn.query_row_as::<(String, String)>(
            "select bikepedgroup, type from tc_header where recordnum = :1",
            &[&recordnum],
        ) {
            Ok(v) => v,
            Err(_) => {
                return Err(CountError::DbError(format!(
                    "{recordnum} not found in tc_header table"
                )))
            }
        };

        // Get equipment factor, if any, from the TC_COUNTTYPE table.
        let equipment_factor = conn.query_row_as::<Option<f32>>(
            "select factor2 from tc_counttype where counttype = :1",
            &[&count_type],
        )?;

        let mut daily_aadv: HashMap<(Date, Option<Direction>), f32> = HashMap::new();

        for ((date, direction), total) in day_counts {
            // Get season factor from factor table.
            let season_factor = conn.query_row_as::<f32>(
                &format!(
                    "select factor from {} WHERE type = :1 and year = :2 and monthnum = :3 and dayofweeknum = :4",
                    Self::FACTOR_TABLE,
                ),
                &[
                    &bikepedgroup,
                    &date.year(),
                    &(date.month() as u32),
                    &(date.weekday().number_from_sunday() as u32), // DVRPC uses 1-7 for SUN to SAT
                ],
            )?;

            match equipment_factor {
                None => daily_aadv.insert((date, direction), total as f32 * season_factor),
                Some(v) => daily_aadv.insert((date, direction), total as f32 * season_factor * v),
            };
        }

        // Determine the divisor by which we'll average the counts.
        let directions_per_day = daily_aadv
            .keys()
            .map(|(_date, direction)| direction)
            .collect::<HashSet<_>>();
        let divisor = (daily_aadv.len() / directions_per_day.len()) as f32;

        // Average totals from each day over each Option<Direction>.
        let mut aadv = HashMap::new();
        for direction in directions_per_day {
            let aadv_per_dir: f32 = daily_aadv
                .iter()
                .filter(|((_date, dir), _total)| dir == direction)
                .map(|((_date, _dir), total)| total)
                .sum();
            aadv.insert(*direction, aadv_per_dir / divisor);
        }

        Ok(aadv)
    }
}

impl Aadv for FifteenMinutePedestrian {
    const BINNED_TABLE: &'static str = "tc_pedcount";
    const TOTAL_FIELD: &'static str = "total";
    const BINNED_RECORDNUM_FIELD: &'static str = "dvrpcnum";
    const COUNT_DIR_FIELD: (&'static str, Option<&'static str>) = ("IN", Some("OUT"));
    const FACTOR_TABLE: &'static str = "tc_pedfactor";

    fn get_total_by_date(
        recordnum: u32,
        conn: &Connection,
    ) -> Result<HashMap<(Date, Option<Direction>), usize>, CountError> {
        let dates = Self::get_full_dates(recordnum, conn)?;
        get_total_by_date_bike_ped(
            recordnum,
            dates,
            Self::TOTAL_FIELD,
            Self::BINNED_TABLE,
            Self::COUNT_DIR_FIELD.0,
            Self::COUNT_DIR_FIELD.1.unwrap(),
            Self::BINNED_RECORDNUM_FIELD,
            conn,
        )
    }

    fn calculate_aadv(
        recordnum: u32,
        conn: &Connection,
    ) -> Result<HashMap<Option<Direction>, f32>, CountError> {
        // Get day counts for full days.
        let day_counts = Self::get_total_by_date(recordnum, conn)?;

        // Get additional fields required to get factors from equipment factor table.
        let count_type = match conn.query_row_as::<String>(
            "select type from tc_header where recordnum = :1",
            &[&recordnum],
        ) {
            Ok(v) => v,
            Err(_) => {
                return Err(CountError::DbError(format!(
                    "{recordnum} not found in tc_header table"
                )))
            }
        };

        // Get equipment factor, if any, from the TC_COUNTTYPE table.
        let equipment_factor = conn.query_row_as::<Option<f32>>(
            "select factor2 from tc_counttype where counttype = :1",
            &[&count_type],
        )?;

        let mut daily_aadv: HashMap<(Date, Option<Direction>), f32> = HashMap::new();

        for ((date, direction), total) in day_counts {
            // Get season factor from factor table.
            let season_factor = conn.query_row_as::<f32>(
                &format!("select factor from {} WHERE month = :1", Self::FACTOR_TABLE,),
                &[&(date.month() as u32)],
            )?;

            match equipment_factor {
                None => daily_aadv.insert((date, direction), total as f32 * season_factor),
                Some(v) => daily_aadv.insert((date, direction), total as f32 * season_factor * v),
            };
        }

        // Determine the divisor by which we'll average the counts.
        let directions_per_day = daily_aadv
            .keys()
            .map(|(_date, direction)| direction)
            .collect::<HashSet<_>>();
        let divisor = (daily_aadv.len() / directions_per_day.len()) as f32;

        // Average totals from each day over each Option<Direction>.
        let mut aadv = HashMap::new();
        for direction in directions_per_day {
            let aadv_per_dir: f32 = daily_aadv
                .iter()
                .filter(|((_date, dir), _total)| dir == direction)
                .map(|((_date, _dir), total)| total)
                .sum();
            aadv.insert(*direction, aadv_per_dir / divisor);
        }

        Ok(aadv)
    }
}

/// Get totals by date for bicycle and pedestrian counts.
#[allow(clippy::too_many_arguments)]
fn get_total_by_date_bike_ped<'a, 'conn>(
    recordnum: u32,
    dates: Vec<Date>,
    total_field: &'a str,
    binned_table: &'a str,
    in_field: &'a str,
    out_field: &'a str,
    recordnum_field: &'a str,
    conn: &'conn Connection,
) -> Result<HashMap<(Date, Option<Direction>), usize>, CountError<'conn>> {
    // Get direction of incount and outcount.
    let (incount_dir, outcount_dir) = match conn.query_row_as::<(Option<String>, Option<String>)>(
        "select indir, outdir from tc_header where recordnum = :1",
        &[&recordnum],
    ) {
        Ok(v) => v,
        Err(_) => {
            return Err(CountError::DbError(format!(
                "{recordnum} not found in tc_header table"
            )))
        }
    };

    if incount_dir.is_none() || outcount_dir.is_none() {
        return Err(CountError::DbError(format!(
            "NULL value for 'indir' or 'outdir' field in tc_header table for {recordnum}"
        )));
    }

    let incount_dir = Direction::from_string(incount_dir.unwrap())?;
    let outcount_dir = Direction::from_string(outcount_dir.unwrap())?;

    let results = conn.query_as::<(Timestamp, usize, usize, usize)>(
        &format!(
            "select countdate, sum({}), sum(\"{}\"), sum({}) from {} where {} = :1 group by countdate",
            &total_field,
            &in_field,
            &out_field,
            &binned_table,
            &recordnum_field
        ),
        &[&recordnum],
    )?;

    // Create hashmap to collect the total.
    // When the Direction is None in the key, that is the overall total (no directionality)
    // for the date, otherwise its for a particular Direction.
    let mut totals: HashMap<(Date, Option<Direction>), usize> = HashMap::new();
    for result in results {
        let (date, total, incount, outcount) = result?;
        let date = Date::parse(
            &format!("{}-{}-{}", date.year(), date.month(), date.day(),),
            YYYY_MM_DD_FMT,
        )
        .unwrap();

        // Don't include any non-full dates.
        if !dates.contains(&date) {
            continue;
        }
        // Insert for each date/direction and date/overall.
        totals.insert((date, Some(incount_dir)), incount);
        totals.insert((date, Some(outcount_dir)), outcount);
        totals.insert((date, None), total);
    }
    Ok(totals)
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{create_pool, get_creds};
    use time::macros::date;

    #[ignore]
    #[test]
    fn full_dates_correct() {
        let (username, password) = get_creds();
        let pool = create_pool(username, password).unwrap();
        let conn = pool.get().unwrap();

        // let expected_dates = vec![date!(2023 - 11 - 07)];
        assert_eq!(
            TimeBinnedVehicleClassCount::get_full_dates(166905, &conn).unwrap(),
            vec![date!(2023 - 11 - 07)]
        );

        assert_eq!(
            TimeBinnedVehicleClassCount::get_full_dates(165367, &conn).unwrap(),
            vec![
                date!(2023 - 11 - 07),
                date![2023 - 11 - 08],
                date![2023 - 11 - 09]
            ]
        );
        assert_eq!(
            FifteenMinuteVehicle::get_full_dates(155381, &conn).unwrap(),
            vec![date!(2021 - 09 - 28)]
        );
        assert_eq!(
            FifteenMinuteVehicle::get_full_dates(147582, &conn).unwrap(),
            vec![
                date!(2019 - 03 - 14),
                date!(2019 - 03 - 15),
                date!(2019 - 03 - 16),
                date!(2019 - 03 - 17),
                date!(2019 - 03 - 18),
                date!(2019 - 03 - 19),
                date!(2019 - 03 - 20),
                date!(2019 - 03 - 21),
                date!(2019 - 03 - 22),
                date!(2019 - 03 - 23),
                date!(2019 - 03 - 24),
                date!(2019 - 03 - 25),
                date!(2019 - 03 - 26),
                date!(2019 - 03 - 27),
                date!(2019 - 03 - 28),
                date!(2019 - 03 - 29),
                date!(2019 - 03 - 30),
                date!(2019 - 03 - 31),
                date!(2019 - 04 - 01),
                date!(2019 - 04 - 02),
            ]
        );

        assert_eq!(
            FifteenMinuteBicycle::get_full_dates(156238, &conn).unwrap(),
            vec![
                date!(2020 - 11 - 22),
                date!(2020 - 11 - 23),
                date!(2020 - 11 - 24),
                date!(2020 - 11 - 25),
                date!(2020 - 11 - 26),
                date!(2020 - 11 - 27),
                date!(2020 - 11 - 28)
            ]
        );

        assert_eq!(
            FifteenMinutePedestrian::get_full_dates(136271, &conn).unwrap(),
            vec![
                date!(2015 - 10 - 15),
                date!(2015 - 10 - 16),
                date!(2015 - 10 - 17),
                date!(2015 - 10 - 18),
                date!(2015 - 10 - 19),
                date!(2015 - 10 - 20),
                date!(2015 - 10 - 21)
            ]
        );
    }

    #[ignore]
    #[test]
    fn totals_correct() {
        let (username, password) = get_creds();
        let pool = create_pool(username, password).unwrap();
        let conn = pool.get().unwrap();

        let day_counts = TimeBinnedVehicleClassCount::get_total_by_date(166905, &conn).unwrap();

        assert_eq!(
            day_counts.get(&(date!(2023 - 11 - 07), None)).unwrap(),
            &4450
        );
        assert_eq!(
            day_counts
                .get(&(date!(2023 - 11 - 07), Some(Direction::East)))
                .unwrap(),
            &2045
        );
        assert_eq!(
            day_counts
                .get(&(date!(2023 - 11 - 07), Some(Direction::West)))
                .unwrap(),
            &2405
        );

        let day_counts = FifteenMinuteVehicle::get_total_by_date(168193, &conn).unwrap();
        assert_eq!(
            day_counts.get(&(date!(2024 - 01 - 04), None)).unwrap(),
            &8527
        );
        assert_eq!(
            day_counts
                .get(&(date!(2024 - 01 - 04), Some(Direction::East)))
                .unwrap(),
            &4170
        );
        assert_eq!(
            day_counts
                .get(&(date!(2024 - 01 - 04), Some(Direction::West)))
                .unwrap(),
            &4357
        );

        let day_counts = FifteenMinuteBicycle::get_total_by_date(156238, &conn).unwrap();
        assert_eq!(day_counts.len(), 21);
        assert_eq!(day_counts.get(&(date!(2020 - 11 - 22), None)).unwrap(), &84);
        assert_eq!(
            day_counts
                .get(&(date!(2020 - 11 - 22), Some(Direction::East)))
                .unwrap(),
            &50
        );
        assert_eq!(
            day_counts
                .get(&(date!(2020 - 11 - 22), Some(Direction::West)))
                .unwrap(),
            &34
        );
        assert_eq!(day_counts.get(&(date!(2020 - 11 - 23), None)).unwrap(), &23);
        assert_eq!(
            day_counts
                .get(&(date!(2020 - 11 - 23), Some(Direction::East)))
                .unwrap(),
            &16
        );
        assert_eq!(
            day_counts
                .get(&(date!(2020 - 11 - 23), Some(Direction::West)))
                .unwrap(),
            &7
        );
        assert_eq!(day_counts.get(&(date!(2020 - 11 - 24), None)).unwrap(), &40);
        assert_eq!(
            day_counts
                .get(&(date!(2020 - 11 - 24), Some(Direction::East)))
                .unwrap(),
            &32
        );
        assert_eq!(
            day_counts
                .get(&(date!(2020 - 11 - 24), Some(Direction::West)))
                .unwrap(),
            &8
        );
        assert_eq!(day_counts.get(&(date!(2020 - 11 - 25), None)).unwrap(), &67);
        assert_eq!(
            day_counts
                .get(&(date!(2020 - 11 - 25), Some(Direction::East)))
                .unwrap(),
            &43
        );
        assert_eq!(
            day_counts
                .get(&(date!(2020 - 11 - 25), Some(Direction::West)))
                .unwrap(),
            &24
        );
        assert_eq!(day_counts.get(&(date!(2020 - 11 - 26), None)).unwrap(), &23);
        assert_eq!(
            day_counts
                .get(&(date!(2020 - 11 - 26), Some(Direction::East)))
                .unwrap(),
            &17
        );
        assert_eq!(
            day_counts
                .get(&(date!(2020 - 11 - 26), Some(Direction::West)))
                .unwrap(),
            &6
        );
        assert_eq!(day_counts.get(&(date!(2020 - 11 - 27), None)).unwrap(), &92);
        assert_eq!(
            day_counts
                .get(&(date!(2020 - 11 - 27), Some(Direction::East)))
                .unwrap(),
            &67
        );
        assert_eq!(
            day_counts
                .get(&(date!(2020 - 11 - 27), Some(Direction::West)))
                .unwrap(),
            &25
        );
        assert_eq!(day_counts.get(&(date!(2020 - 11 - 28), None)).unwrap(), &83);
        assert_eq!(
            day_counts
                .get(&(date!(2020 - 11 - 28), Some(Direction::East)))
                .unwrap(),
            &53
        );
        assert_eq!(
            day_counts
                .get(&(date!(2020 - 11 - 28), Some(Direction::West)))
                .unwrap(),
            &30
        );

        let day_counts = FifteenMinuteBicycle::get_total_by_date(160252, &conn).unwrap();
        assert_eq!(day_counts.len(), 21);
        assert_eq!(day_counts.get(&(date!(2021 - 12 - 14), None)).unwrap(), &41);
        assert_eq!(
            day_counts
                .get(&(date!(2021 - 12 - 14), Some(Direction::South)))
                .unwrap(),
            &39
        );
        assert_eq!(
            day_counts
                .get(&(date!(2021 - 12 - 14), Some(Direction::North)))
                .unwrap(),
            &2
        );
        assert_eq!(day_counts.get(&(date!(2021 - 12 - 15), None)).unwrap(), &37);
        assert_eq!(
            day_counts
                .get(&(date!(2021 - 12 - 15), Some(Direction::South)))
                .unwrap(),
            &36
        );
        assert_eq!(
            day_counts
                .get(&(date!(2021 - 12 - 15), Some(Direction::North)))
                .unwrap(),
            &1
        );
        assert_eq!(
            day_counts.get(&(date!(2021 - 12 - 16), None)).unwrap(),
            &110
        );
        assert_eq!(
            day_counts
                .get(&(date!(2021 - 12 - 16), Some(Direction::South)))
                .unwrap(),
            &105
        );
        assert_eq!(
            day_counts
                .get(&(date!(2021 - 12 - 16), Some(Direction::North)))
                .unwrap(),
            &5
        );
        assert_eq!(day_counts.get(&(date!(2021 - 12 - 17), None)).unwrap(), &40);
        assert_eq!(
            day_counts
                .get(&(date!(2021 - 12 - 17), Some(Direction::South)))
                .unwrap(),
            &40
        );
        assert_eq!(
            day_counts
                .get(&(date!(2021 - 12 - 17), Some(Direction::North)))
                .unwrap(),
            &0
        );
        assert_eq!(day_counts.get(&(date!(2021 - 12 - 18), None)).unwrap(), &22);
        assert_eq!(
            day_counts
                .get(&(date!(2021 - 12 - 18), Some(Direction::South)))
                .unwrap(),
            &22
        );
        assert_eq!(
            day_counts
                .get(&(date!(2021 - 12 - 18), Some(Direction::North)))
                .unwrap(),
            &0
        );
        assert_eq!(day_counts.get(&(date!(2021 - 12 - 19), None)).unwrap(), &14);
        assert_eq!(
            day_counts
                .get(&(date!(2021 - 12 - 19), Some(Direction::South)))
                .unwrap(),
            &13
        );
        assert_eq!(
            day_counts
                .get(&(date!(2021 - 12 - 19), Some(Direction::North)))
                .unwrap(),
            &1
        );
        assert_eq!(day_counts.get(&(date!(2021 - 12 - 20), None)).unwrap(), &40);
        assert_eq!(
            day_counts
                .get(&(date!(2021 - 12 - 20), Some(Direction::South)))
                .unwrap(),
            &37
        );
        assert_eq!(
            day_counts
                .get(&(date!(2021 - 12 - 20), Some(Direction::North)))
                .unwrap(),
            &3
        );

        let day_counts = FifteenMinutePedestrian::get_total_by_date(136271, &conn).unwrap();
        assert_eq!(day_counts.len(), 21);

        assert_eq!(day_counts.get(&(date!(2015 - 10 - 15), None)).unwrap(), &36);
        assert_eq!(
            day_counts
                .get(&(date!(2015 - 10 - 15), Some(Direction::South)))
                .unwrap(),
            &21
        );
        assert_eq!(
            day_counts
                .get(&(date!(2015 - 10 - 15), Some(Direction::North)))
                .unwrap(),
            &15
        );
        assert_eq!(day_counts.get(&(date!(2015 - 10 - 16), None)).unwrap(), &22);
        assert_eq!(
            day_counts
                .get(&(date!(2015 - 10 - 16), Some(Direction::South)))
                .unwrap(),
            &13
        );
        assert_eq!(
            day_counts
                .get(&(date!(2015 - 10 - 16), Some(Direction::North)))
                .unwrap(),
            &9
        );
        assert_eq!(day_counts.get(&(date!(2015 - 10 - 17), None)).unwrap(), &68);
        assert_eq!(
            day_counts
                .get(&(date!(2015 - 10 - 17), Some(Direction::South)))
                .unwrap(),
            &36
        );
        assert_eq!(
            day_counts
                .get(&(date!(2015 - 10 - 17), Some(Direction::North)))
                .unwrap(),
            &32
        );
        assert_eq!(day_counts.get(&(date!(2015 - 10 - 18), None)).unwrap(), &81);
        assert_eq!(
            day_counts
                .get(&(date!(2015 - 10 - 18), Some(Direction::South)))
                .unwrap(),
            &38
        );
        assert_eq!(
            day_counts
                .get(&(date!(2015 - 10 - 18), Some(Direction::North)))
                .unwrap(),
            &43
        );
        assert_eq!(day_counts.get(&(date!(2015 - 10 - 19), None)).unwrap(), &25);
        assert_eq!(
            day_counts
                .get(&(date!(2015 - 10 - 19), Some(Direction::South)))
                .unwrap(),
            &14
        );
        assert_eq!(
            day_counts
                .get(&(date!(2015 - 10 - 19), Some(Direction::North)))
                .unwrap(),
            &11
        );
        assert_eq!(
            day_counts.get(&(date!(2015 - 10 - 20), None)).unwrap(),
            &134
        );
        assert_eq!(
            day_counts
                .get(&(date!(2015 - 10 - 20), Some(Direction::South)))
                .unwrap(),
            &110
        );
        assert_eq!(
            day_counts
                .get(&(date!(2015 - 10 - 20), Some(Direction::North)))
                .unwrap(),
            &24
        );
        assert_eq!(day_counts.get(&(date!(2015 - 10 - 21), None)).unwrap(), &76);
        assert_eq!(
            day_counts
                .get(&(date!(2015 - 10 - 21), Some(Direction::South)))
                .unwrap(),
            &52
        );
        assert_eq!(
            day_counts
                .get(&(date!(2015 - 10 - 21), Some(Direction::North)))
                .unwrap(),
            &24
        );
    }

    #[ignore]
    #[test]
    fn aadv_correct() {
        let (username, password) = get_creds();
        let pool = create_pool(username, password).unwrap();
        let conn = pool.get().unwrap();

        let aadv = TimeBinnedVehicleClassCount::calculate_aadv(166905, &conn).unwrap();
        assert_eq!(aadv.get(&None).unwrap().round() as u32, 3880);
        assert_eq!(
            aadv.get(&Some(Direction::East)).unwrap().round() as u32,
            1783
        );
        assert_eq!(
            aadv.get(&Some(Direction::West)).unwrap().round() as u32,
            2097
        );

        // 141216: fc 17, PA, 2018-08-01 (4th day of week from Sunday) only full day
        // pafactor = 0.863; paaxle = 0.976; total for 2018-08-01 (no directionality, because
        // not done by previous import process): 7460
        let aadv = FifteenMinuteVehicle::calculate_aadv(141216, &conn).unwrap();
        assert_eq!(aadv.get(&None).unwrap().round() as u32, 6283);

        // 156238: bikpedgroup Mixed; full days from Nov 22, 2020 to Nov 28, 2020 inclusive
        /* Here's how it was manually calculated:
        // full day date, total, in, out, dayofweek, factor (tc_bikefactor)
        let data = [
            (2020 - 11 - 22, 84, 50, 34, 1, 2.068),
            (2020 - 11 - 23, 23, 16, 7, 2, 1.654),
            (2020 - 11 - 24, 40, 32, 8, 3, 1.835),
            (2020 - 11 - 25, 67, 43, 24, 4, 2.017),
            (2020 - 11 - 26, 23, 17, 6, 5, 2.316),
            (2020 - 11 - 27, 92, 67, 25, 6, 2.169),
            (2020 - 11 - 28, 83, 53, 30, 7, 2.006),
        ];
        let equipment_factor = 1.02; // from tc_counttype
        let manual_aadv_overall: f32 = data
            .iter()
            .map(|each| each.1 as f32 * each.5 * equipment_factor)
            .sum();
        let manual_aadv_east: f32 = data
            .iter()
            .map(|each| each.2 as f32 * each.5 * equipment_factor)
            .sum();
        let manual_aadv_west: f32 = data
            .iter()
            .map(|each| each.3 as f32 * each.5 * equipment_factor)
            .sum();
        let manual_aadv_overall = manual_aadv_overall / 7.0;
        let manual_aadv_east = manual_aadv_east / 7.0;
        let manual_aadv_west = manual_aadv_west / 7.0;
        dbg!(&manual_aadv_overall); // 122.343
        dbg!(&manual_aadv_east); //  82.522
        dbg!(&manual_aadv_west); //  39.821
        */
        let aadv = FifteenMinuteBicycle::calculate_aadv(156238, &conn).unwrap();
        assert_eq!(aadv.get(&None).unwrap().round() as u32, 122);
        assert_eq!(aadv.get(&Some(Direction::East)).unwrap().round() as u32, 83);
        assert_eq!(aadv.get(&Some(Direction::West)).unwrap().round() as u32, 40);

        // 136271: full days from Oct 15, 2015 to Oct 21, 2015, inclusive.
        /* Here's how it was manually calculated:
        // full day date, total, in, out
        let data = [
            (2015 - 10 - 15, 36, 21, 15),
            (2015 - 10 - 16, 22, 13, 9),
            (2015 - 10 - 17, 68, 36, 32),
            (2015 - 10 - 18, 81, 38, 43),
            (2015 - 10 - 19, 25, 14, 11),
            (2015 - 10 - 20, 134, 110, 24),
            (2015 - 10 - 21, 76, 52, 24),
        ];
        let factor = 0.968; // for October
        let equipment_factor = 1.0622; // from tc_counttype
        let manual_aadv_overall: f32 = data
            .iter()
            .map(|each| each.1 as f32 * factor * equipment_factor)
            .sum();
        let manual_aadv_south: f32 = data
            .iter()
            .map(|each| each.2 as f32 * factor * equipment_factor)
            .sum();
        let manual_aadv_north: f32 = data
            .iter()
            .map(|each| each.3 as f32 * factor * equipment_factor)
            .sum();
        let manual_aadv_overall = manual_aadv_overall / 7.0;
        let manual_aadv_south = manual_aadv_south / 7.0;
        let manual_aadv_north = manual_aadv_north / 7.0;
        dbg!(&manual_aadv_overall); // 64.924
        dbg!(&manual_aadv_south); // 41.716
        dbg!(&manual_aadv_north); // 23.208
        */
        let aadv = FifteenMinutePedestrian::calculate_aadv(136271, &conn).unwrap();
        assert_eq!(aadv.get(&None).unwrap().round() as u32, 65);
        assert_eq!(
            aadv.get(&Some(Direction::South)).unwrap().round() as u32,
            42
        );
        assert_eq!(
            aadv.get(&Some(Direction::North)).unwrap().round() as u32,
            23
        );
    }
}

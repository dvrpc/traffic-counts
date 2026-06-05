//! Data structures and functions related to permanent bicycle/pedestrian counts.
use chrono::{NaiveDate, NaiveDateTime};

use crate::CountError;

#[derive(Debug, Clone)]
pub struct PermBikePedCount {
    pub location_id: i32,
    pub datetime: NaiveDateTime,
    pub total: Option<i32>,
    pub ped_in: Option<i32>,
    pub ped_out: Option<i32>,
    pub bike_in: Option<i32>,
    pub bike_out: Option<i32>,
}

impl PermBikePedCount {
    pub fn new(
        location_id: i32,
        datetime: NaiveDateTime,
        counts: &[Option<i32>],
        ped: bool,
        bike: bool,
    ) -> Result<PermBikePedCount, CountError> {
        let mut ped_in = None;
        let mut ped_out = None;
        let mut bike_in = None;
        let mut bike_out = None;

        // EcoVizio does not properly report the bike data for the two bike-lane-only counters:
        // It produces 3 fields for them, but they are total, pedin, and pedout, rather than
        // what they should be: total, bikein, bikeout. Fortunately, the total = bikein,
        // so manually handle that.
        if location_id == 24 || location_id == 25 {
            bike_in = counts[0];

            // On top of this, the webmap for this -
            // <https://www.dvrpc.org/webmaps/permbikeped/>
            // requires missing data to be encoded as 0, so...
            bike_out = Some(0);
        } else {
            // `counts` is a slice from the whole row, starting with total (index 0) and followed by
            // either a ped or bike pair (in/out) or both (usually both)
            if counts.len() == 5 {
                if !bike && !ped {
                    return Err(CountError::TooManyPermBikePedFields);
                }
                ped_in = counts[1];
                ped_out = counts[2];
                bike_in = counts[3];
                bike_out = counts[4];
            } else if counts.len() == 3 {
                if bike && ped {
                    return Err(CountError::TooFewPermBikePedFields);
                }
                if ped && !bike {
                    ped_in = counts[1];
                    ped_out = counts[2];
                }
                if !ped && bike {
                    bike_in = counts[1];
                    bike_out = counts[2];
                }
            } else {
                return Err(CountError::UnexpectedNumberOfPermBikePedFields);
            }
        }

        Ok(Self {
            location_id,
            datetime,
            total: counts[0],
            ped_in,
            ped_out,
            bike_in,
            bike_out,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AggregatedPermBikePedCount {
    pub location_id: i32,
    pub date: NaiveDate,
    pub total_ped: Option<i32>,
    pub total_bike: Option<i32>,
    pub total: Option<i32>,
}

impl AggregatedPermBikePedCount {
    pub fn new(
        location_id: i32,
        date: NaiveDate,
        total_ped: Option<i32>,
        total_bike: Option<i32>,
        total: Option<i32>,
    ) -> Self {
        Self {
            location_id,
            date,
            total_ped,
            total_bike,
            total,
        }
    }
}

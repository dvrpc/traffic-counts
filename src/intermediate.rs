//! Intermediate data types.
//!
//! As data records are iterated over, HashMaps are used to associate keys with values. However,
//! in the end Vecs are a better representation of the data (a row in a table). The following
//! "formulas" show how each is used.
//!
//! HashMap Key + HashMap Value = Vec
//!
//! [`BinnedCountKey`] + [`VehicleClassCount`] = [`crate::TimeBinnedVehicleClassCount`].
//!
//! [`BinnedCountKey`] + [`SpeedRangeCount`] = [`crate::TimeBinnedSpeedRangeCount`].
//!
//! [`NonNormalCountKey`] + [`NonNormalAvgSpeedValue`] = [`crate::denormalize::NonNormalAvgSpeedCount`].
//!
//! [`NonNormalCountKey`] + [`NonNormalVolCountValue`] = [`crate::denormalize::NonNormalVolCount`].
use time::{Date, PrimitiveDateTime};

use crate::{denormalize::HourlyCount, Direction, VehicleClass, Weather};

/// The key for records of the TC_SPECOUNT and TC_CLACOUNT tables.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct BinnedCountKey {
    pub datetime: PrimitiveDateTime,
    pub channel: u8,
}

/// The rest of the fields for the TC_CLACOUNT table.
///
/// This is generally - but not always - for 15-minute intervals.
///
/// Note: unclassified vehicles are counted in `c15` field, but also are included in the `c2`
/// (Passenger Cars). Thus, a simple sum of fields `c1` through `c15` would double-count
/// unclassified vehicles.
#[derive(Debug, Clone, Copy)]
pub struct VehicleClassCount {
    pub record_num: u32,
    pub direction: Direction,
    pub c1: u32,
    pub c2: u32,
    pub c3: u32,
    pub c4: u32,
    pub c5: u32,
    pub c6: u32,
    pub c7: u32,
    pub c8: u32,
    pub c9: u32,
    pub c10: u32,
    pub c11: u32,
    pub c12: u32,
    pub c13: u32,
    pub c15: u32,
    pub total: u32,
}

impl VehicleClassCount {
    /// Create one with 0 count for all classes.
    pub fn new(record_num: u32, direction: Direction) -> Self {
        Self {
            record_num,
            direction,
            c1: 0,
            c2: 0,
            c3: 0,
            c4: 0,
            c5: 0,
            c6: 0,
            c7: 0,
            c8: 0,
            c9: 0,
            c10: 0,
            c11: 0,
            c12: 0,
            c13: 0,
            c15: 0,
            total: 0,
        }
    }
    /// Create one with its first count inserted.
    pub fn first(record_num: u32, direction: Direction, class: VehicleClass) -> Self {
        let mut count = Self::new(record_num, direction);
        count.insert(class);
        count
    }
    /// Insert individual counted vehicles into count.
    pub fn insert(&mut self, class: VehicleClass) {
        match class {
            VehicleClass::Motorcycles => self.c1 += 1,
            VehicleClass::PassengerCars => self.c2 += 1,
            VehicleClass::OtherFourTireSingleUnitVehicles => self.c3 += 1,
            VehicleClass::Buses => self.c4 += 1,
            VehicleClass::TwoAxleSixTireSingleUnitTrucks => self.c5 += 1,
            VehicleClass::ThreeAxleSingleUnitTrucks => self.c6 += 1,
            VehicleClass::FourOrMoreAxleSingleUnitTrucks => self.c7 += 1,
            VehicleClass::FourOrFewerAxleSingleTrailerTrucks => self.c8 += 1,
            VehicleClass::FiveAxleSingleTrailerTrucks => self.c9 += 1,
            VehicleClass::SixOrMoreAxleSingleTrailerTrucks => self.c10 += 1,
            VehicleClass::FiveOrFewerAxleMultiTrailerTrucks => self.c11 += 1,
            VehicleClass::SixAxleMultiTrailerTrucks => self.c12 += 1,
            VehicleClass::SevenOrMoreAxleMultiTrailerTrucks => self.c13 += 1,
            VehicleClass::UnclassifiedVehicle => {
                // Unclassified vehicles get included with class 2 and also counted on their own.
                self.c2 += 1;
                self.c15 += 1;
            }
        }
        self.total += 1;
    }
}

/// The rest of the fields for the TC_SPECOUNT table.
///
/// This is generally - but not always - for 15-minute intervals.
#[derive(Debug, Clone, Copy)]
pub struct SpeedRangeCount {
    pub record_num: u32,
    pub direction: Direction,
    pub s1: u32,
    pub s2: u32,
    pub s3: u32,
    pub s4: u32,
    pub s5: u32,
    pub s6: u32,
    pub s7: u32,
    pub s8: u32,
    pub s9: u32,
    pub s10: u32,
    pub s11: u32,
    pub s12: u32,
    pub s13: u32,
    pub s14: u32,
    pub total: u32,
}

impl SpeedRangeCount {
    /// Create one with 0 count for all speed ranges.
    pub fn new(record_num: u32, direction: Direction) -> Self {
        Self {
            record_num,
            direction,
            s1: 0,
            s2: 0,
            s3: 0,
            s4: 0,
            s5: 0,
            s6: 0,
            s7: 0,
            s8: 0,
            s9: 0,
            s10: 0,
            s11: 0,
            s12: 0,
            s13: 0,
            s14: 0,
            total: 0,
        }
    }

    /// Create one with its first count inserted.
    pub fn first(record_num: u32, direction: Direction, speed: f32) -> Self {
        let mut value = Self::new(record_num, direction);
        value.insert(speed);
        value
    }
    /// Insert individual speed into count.
    pub fn insert(&mut self, speed: f32) {
        // The end of the ranges are inclusive to the number's .0 decimal;
        // that is:
        // 0-15: 0.0 to 15.0
        // >15-20: 15.1 to 20.0, etc.

        // Unfortunately, using floats as tests in pattern matching will be an error in a future
        // Rust release, so need to do if/else rather than match.
        // <https://github.com/rust-lang/rust/issues/41620>
        if speed.is_sign_negative() {
            // This shouldn't be necessary, but I saw a -0.0 in one of the files.
            self.s1 += 1
        } else if (0.0..=15.0).contains(&speed) {
            self.s1 += 1;
        } else if (15.1..=20.0).contains(&speed) {
            self.s2 += 1;
        } else if (20.1..=25.0).contains(&speed) {
            self.s3 += 1;
        } else if (25.1..=30.0).contains(&speed) {
            self.s4 += 1;
        } else if (30.1..=35.0).contains(&speed) {
            self.s5 += 1;
        } else if (35.1..=40.0).contains(&speed) {
            self.s6 += 1;
        } else if (40.1..=45.0).contains(&speed) {
            self.s7 += 1;
        } else if (45.1..=50.0).contains(&speed) {
            self.s8 += 1;
        } else if (50.1..=55.0).contains(&speed) {
            self.s9 += 1;
        } else if (55.1..=60.0).contains(&speed) {
            self.s10 += 1;
        } else if (60.1..=65.0).contains(&speed) {
            self.s11 += 1;
        } else if (65.1..=70.0).contains(&speed) {
            self.s12 += 1;
        } else if (70.1..=75.0).contains(&speed) {
            self.s13 += 1;
        } else if (75.1..).contains(&speed) {
            self.s14 += 1;
        }
        self.total += 1;
    }
}

/// The key for records of the TC_VOLCOUNT and TC_SPESUM tables.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct NonNormalCountKey {
    pub record_num: u32,
    pub date: Date,
    pub direction: Direction,
    pub channel: u8,
}

/// The rest of the fields in the TC_VOLCOUNT table.
///
/// Hourly fields are `Option` because traffic counts aren't done from 12am one day to 12am the
/// the following day - can start and stop at any time.
#[derive(Debug, Clone, Default)]
pub struct NonNormalVolCountValue {
    pub setflag: Option<i8>,
    pub totalcount: Option<u32>,
    pub weather: Option<Weather>,
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

impl NonNormalVolCountValue {
    /// Create a NonNormalVolCountValue with `None` for everything except
    /// the total and the first hour/count.
    /// (For the first time a new key is created in a HashMap.)
    pub fn first(count: &HourlyCount) -> Self {
        let mut value = Self {
            ..Default::default()
        };

        let volume = count.count;

        value.totalcount = Some(volume);

        match count.datetime.hour() {
            0 => value.am12 = Some(volume),
            1 => value.am1 = Some(volume),
            2 => value.am2 = Some(volume),
            3 => value.am3 = Some(volume),
            4 => value.am4 = Some(volume),
            5 => value.am5 = Some(volume),
            6 => value.am6 = Some(volume),
            7 => value.am7 = Some(volume),
            8 => value.am8 = Some(volume),
            9 => value.am9 = Some(volume),
            10 => value.am10 = Some(volume),
            11 => value.am11 = Some(volume),
            12 => value.pm12 = Some(volume),
            13 => value.pm1 = Some(volume),
            14 => value.pm2 = Some(volume),
            15 => value.pm3 = Some(volume),
            16 => value.pm4 = Some(volume),
            17 => value.pm5 = Some(volume),
            18 => value.pm6 = Some(volume),
            19 => value.pm7 = Some(volume),
            20 => value.pm8 = Some(volume),
            21 => value.pm9 = Some(volume),
            22 => value.pm10 = Some(volume),
            23 => value.pm11 = Some(volume),
            _ => (), // ok, because time.hour() can only be 0-23
        }
        value
    }
}

/// The rest of the fields in the TC_SPESUM table.
///
/// Hourly fields are `Option` because traffic counts aren't done from 12am one day to 12am the
/// the following day - can start and stop at any time.
#[derive(Debug, Clone, Default)]
pub struct NonNormalAvgSpeedValue {
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

impl NonNormalAvgSpeedValue {
    // Create new NonNormalAvgSpeedValue, including the first hourly average we calculate.
    // Subsequent hourly averages are added by modifying this instance (via the key in
    // the HashMap this is the value for).
    pub fn first(hour_as_str: &str, average_speed: f32) -> Self {
        let mut value = Self {
            ..Default::default()
        };

        match hour_as_str {
            "am12" => value.am12 = Some(average_speed),
            "am1" => value.am1 = Some(average_speed),
            "am2" => value.am2 = Some(average_speed),
            "am3" => value.am3 = Some(average_speed),
            "am4" => value.am4 = Some(average_speed),
            "am5" => value.am5 = Some(average_speed),
            "am6" => value.am6 = Some(average_speed),
            "am7" => value.am7 = Some(average_speed),
            "am8" => value.am8 = Some(average_speed),
            "am9" => value.am9 = Some(average_speed),
            "am10" => value.am10 = Some(average_speed),
            "am11" => value.am11 = Some(average_speed),
            "pm12" => value.pm12 = Some(average_speed),
            "pm1" => value.pm1 = Some(average_speed),
            "pm2" => value.pm2 = Some(average_speed),
            "pm3" => value.pm3 = Some(average_speed),
            "pm4" => value.pm4 = Some(average_speed),
            "pm5" => value.pm5 = Some(average_speed),
            "pm6" => value.pm6 = Some(average_speed),
            "pm7" => value.pm7 = Some(average_speed),
            "pm8" => value.pm8 = Some(average_speed),
            "pm9" => value.pm9 = Some(average_speed),
            "pm10" => value.pm10 = Some(average_speed),
            "pm11" => value.pm11 = Some(average_speed),
            _ => (),
        }
        value
    }
}

/// Raw speeds, used to create averages.
#[derive(Debug, Clone, Default)]
pub struct NonNormalRawSpeedValue {
    pub am12: Vec<f32>,
    pub am1: Vec<f32>,
    pub am2: Vec<f32>,
    pub am3: Vec<f32>,
    pub am4: Vec<f32>,
    pub am5: Vec<f32>,
    pub am6: Vec<f32>,
    pub am7: Vec<f32>,
    pub am8: Vec<f32>,
    pub am9: Vec<f32>,
    pub am10: Vec<f32>,
    pub am11: Vec<f32>,
    pub pm12: Vec<f32>,
    pub pm1: Vec<f32>,
    pub pm2: Vec<f32>,
    pub pm3: Vec<f32>,
    pub pm4: Vec<f32>,
    pub pm5: Vec<f32>,
    pub pm6: Vec<f32>,
    pub pm7: Vec<f32>,
    pub pm8: Vec<f32>,
    pub pm9: Vec<f32>,
    pub pm10: Vec<f32>,
    pub pm11: Vec<f32>,
}
impl NonNormalRawSpeedValue {
    /// Create a `NonNormalAvgSpeedValue` with empty Vecs.
    /// (For the first time a new key is created in a HashMap.)
    pub fn first(hour: u8, speed: f32) -> Self {
        let mut value = Self {
            ..Default::default()
        };

        match hour {
            0 => value.am12 = vec![speed],
            1 => value.am1 = vec![speed],
            2 => value.am2 = vec![speed],
            3 => value.am3 = vec![speed],
            4 => value.am4 = vec![speed],
            5 => value.am5 = vec![speed],
            6 => value.am6 = vec![speed],
            7 => value.am7 = vec![speed],
            8 => value.am8 = vec![speed],
            9 => value.am9 = vec![speed],
            10 => value.am10 = vec![speed],
            11 => value.am11 = vec![speed],
            12 => value.pm12 = vec![speed],
            13 => value.pm1 = vec![speed],
            14 => value.pm2 = vec![speed],
            15 => value.pm3 = vec![speed],
            16 => value.pm4 = vec![speed],
            17 => value.pm5 = vec![speed],
            18 => value.pm6 = vec![speed],
            19 => value.pm7 = vec![speed],
            20 => value.pm8 = vec![speed],
            21 => value.pm9 = vec![speed],
            22 => value.pm10 = vec![speed],
            23 => value.pm11 = vec![speed],
            _ => (), // ok, because time.hour() can only be 0-23
        }
        value
    }
}

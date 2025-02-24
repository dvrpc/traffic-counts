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
use chrono::{NaiveDate, NaiveDateTime};

use crate::{LaneDirection, VehicleClass};

/// The key for records of the TC_SPECOUNT and TC_CLACOUNT tables.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct BinnedCountKey {
    pub date: NaiveDate,
    pub time: NaiveDateTime,
    pub lane: u8,
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
    pub recordnum: u32,
    pub direction: LaneDirection,
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
    pub fn new(recordnum: u32, direction: LaneDirection) -> Self {
        Self {
            recordnum,
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
    pub fn first(recordnum: u32, direction: LaneDirection, class: VehicleClass) -> Self {
        let mut count = Self::new(recordnum, direction);
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
    pub recordnum: u32,
    pub direction: LaneDirection,
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
    pub fn new(recordnum: u32, direction: LaneDirection) -> Self {
        Self {
            recordnum,
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
    pub fn first(recordnum: u32, direction: LaneDirection, speed: f32) -> Self {
        let mut value = Self::new(recordnum, direction);
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

//! Library for defining data structures related to DVRPC's traffic counts, extracting data
//! from files (see [`InputCount`] for the types of counts that can be handled as inputs),
//! and doing various calculations like average annual daily traffic.
//!
//! An associated program at src/bin/import.rs implements the data extraction from files
//! and inserts the data into our database.
//!
//! This library also lays the foundations for the inverse need - getting data from the database
//! in order to act on it or display it in some way, whether for CRUD user interfaces or other
//! uses.
//!
//! See <https://www.dvrpc.org/traffic/> for additional information about traffic counting.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;

use log::error;
use thiserror::Error;
use time::{Date, PrimitiveDateTime, Time};

pub mod annual_avg;
pub mod extract_from_file;

use annual_avg::GetDate;

// headers stripped of double quotes and spaces
// TODO: the headers for FifteenMinuteBicycle and FifteenMinutePedestrian
// still need to be added
const FIFTEEN_MINUTE_VEHICLE_HEADER: &str = "Number,Date,Time,Channel1";
const INDIVIDUAL_VEHICLE_HEADER: &str = "Veh.No.,Date,Time,Channel,Class,Speed";

/// Various errors that can occur.
#[derive(Debug, Error)]
pub enum CountError<'a> {
    #[error("problem with file or directory path")]
    BadPath(&'a Path),
    #[error("unable to open file `{0}`")]
    CannotOpenFile(#[from] io::Error),
    #[error("the filename at {path:?} is not to specification: {problem:?}")]
    InvalidFileName {
        problem: FileNameProblem,
        path: &'a Path,
    },
    #[error("no matching count type for directory `{0}`")]
    BadLocation(String),
    #[error("no matching count type for header in `{0}`")]
    BadHeader(&'a Path),
    #[error("mismatch in count types between file location (`{0}`) and header of that file")]
    LocationHeaderMisMatch(&'a Path),
    #[error("no such vehicle class '{0}'")]
    BadVehicleClass(u8),
    #[error("error converting header row to string")]
    HeadertoStringRecordError(#[from] csv::Error),
}

/// Identifying the problem when there's an error with a filename.
#[derive(Debug)]
pub enum FileNameProblem {
    TooManyParts,
    TooFewParts,
    InvalidTech,
    InvalidRecordNum,
    InvalidDirections,
    InvalidCounterID,
    InvalidSpeedLimit,
}

/// The kinds of counts this library can handle as inputs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InputCount {
    /// Eco-Counter
    FifteenMinuteBicycle,
    /// Eco-Counter
    FifteenMinutePedestrian,
    /// 15-minute binned data for the simple volume counts from StarNext/Jamar.
    ///
    /// See [`FifteenMinuteVehicle`], the corresponding type.
    FifteenMinuteVehicle,
    /// Individual vehicles from StarNext/Jamar prior to any binning.
    ///
    /// See [`IndividualVehicle`], the corresponding type.
    IndividualVehicle,
}

impl InputCount {
    /// Get the `InputCount` variant from the parent directory where a file is located.
    pub fn from_parent_dir(path: &Path) -> Result<Self, CountError> {
        // Get the directory immediately above the file.
        let parent = path
            .parent()
            .ok_or(CountError::BadPath(path))?
            .components()
            .last()
            .ok_or(CountError::BadPath(path))?
            .as_os_str()
            .to_str()
            .ok_or(CountError::BadPath(path))?;

        match parent {
            "15minutebicycle" => Ok(InputCount::FifteenMinuteBicycle),
            "15minutepedestrian" => Ok(InputCount::FifteenMinutePedestrian),
            "15minutevehicle" => Ok(InputCount::FifteenMinuteVehicle),
            "vehicle" => Ok(InputCount::IndividualVehicle),
            _ => Err(CountError::BadLocation(parent.to_string())),
        }
    }

    /// Get `InputCount` variant based on the header of a file.
    pub fn from_header(path: &Path) -> Result<InputCount, CountError> {
        let (count_type, _) = count_type_and_num_nondata_rows(path)?;
        Ok(count_type)
    }

    /// Get `InputCount` variant from both parent directory and the header of the file.
    ///
    /// If the variant differs between the two methods, we can't be sure which is correct,
    /// so return an error.
    pub fn from_parent_dir_and_header(path: &Path) -> Result<InputCount, CountError> {
        let count_type_from_location = InputCount::from_parent_dir(path)?;
        let count_type_from_header = InputCount::from_header(path)?;
        if count_type_from_location != count_type_from_header {
            return Err(CountError::LocationHeaderMisMatch(path));
        }
        Ok(count_type_from_location)
    }
}

/// An individual vehicle that has been counted, with no binning applied to it.
#[derive(Debug, Clone)]
pub struct IndividualVehicle {
    pub date: Date,
    pub time: Time,
    pub channel: u8,
    pub class: VehicleClass,
    pub speed: f32,
}

impl GetDate for IndividualVehicle {
    fn get_date(&self) -> Date {
        self.date.to_owned()
    }
}

impl IndividualVehicle {
    pub fn new(
        date: Date,
        time: Time,
        channel: u8,
        class: u8,
        speed: f32,
    ) -> Result<Self, CountError<'static>> {
        let class = VehicleClass::from_num(class)?;
        Ok(Self {
            date,
            time,
            channel,
            class,
            speed,
        })
    }
}

///  Pre-binned, simple volume counts in 15-minute intervals.
#[derive(Debug, Clone)]
pub struct FifteenMinuteVehicle {
    pub date: Date,
    pub time: Time,
    pub count: u16,
    pub direction: Direction,
}

impl GetDate for FifteenMinuteVehicle {
    fn get_date(&self) -> Date {
        self.date.to_owned()
    }
}

impl FifteenMinuteVehicle {
    pub fn new(
        date: Date,
        time: Time,
        count: u16,
        direction: Direction,
    ) -> Result<Self, CountError<'static>> {
        Ok(Self {
            date,
            time,
            count,
            direction,
        })
    }
}

/// The metadata of an input count.
#[derive(Debug, Clone, PartialEq)]
pub struct CountMetadata {
    pub technician: String, // initials
    pub dvrpc_num: i32,
    pub directions: Directions,
    pub counter_id: i32,
    pub speed_limit: Option<i32>,
}

impl CountMetadata {
    /// Get an input count's metadata from its path.
    ///
    /// In the filename, each field is separate by a dash (-).
    /// technician-dvrpc_num-directions-counter_id-speed_limit.csv/txt
    /// e.g. rc-166905-ew-40972-35.txt
    pub fn from_path(path: &Path) -> Result<Self, CountError> {
        let parts: Vec<&str> = path
            .file_stem()
            .ok_or(CountError::BadPath(path))?
            .to_str()
            .ok_or(CountError::BadPath(path))?
            .split('-')
            .collect();

        if parts.len() < 5 {
            return Err(CountError::InvalidFileName {
                problem: FileNameProblem::TooFewParts,
                path,
            });
        }
        if parts.len() > 5 {
            return Err(CountError::InvalidFileName {
                problem: FileNameProblem::TooManyParts,
                path,
            });
        }

        // `technician` should be letters. If parseable as int, then they aren't letters.
        if parts[0].parse::<i32>().is_ok() {
            return Err(CountError::InvalidFileName {
                problem: FileNameProblem::InvalidTech,
                path,
            });
        }

        let technician = parts[0].to_string();

        let dvrpc_num = match parts[1].parse() {
            Ok(v) => v,
            Err(_) => {
                return Err(CountError::InvalidFileName {
                    problem: FileNameProblem::InvalidRecordNum,
                    path,
                })
            }
        };

        let directions: Directions = match parts[2] {
            "ns" => Directions::new(Direction::North, Some(Direction::South)),
            "sn" => Directions::new(Direction::South, Some(Direction::North)),
            "ew" => Directions::new(Direction::East, Some(Direction::West)),
            "we" => Directions::new(Direction::West, Some(Direction::East)),
            "nn" => Directions::new(Direction::North, Some(Direction::North)),
            "ss" => Directions::new(Direction::South, Some(Direction::South)),
            "ee" => Directions::new(Direction::East, Some(Direction::East)),
            "ww" => Directions::new(Direction::West, Some(Direction::West)),
            "n" => Directions::new(Direction::North, None),
            "s" => Directions::new(Direction::South, None),
            "e" => Directions::new(Direction::East, None),
            "w" => Directions::new(Direction::West, None),
            _ => {
                return Err(CountError::InvalidFileName {
                    problem: FileNameProblem::InvalidDirections,
                    path,
                })
            }
        };

        let counter_id = match parts[3].parse() {
            Ok(v) => v,
            Err(_) => {
                return Err(CountError::InvalidFileName {
                    problem: FileNameProblem::InvalidCounterID,
                    path,
                })
            }
        };

        let speed_limit = if parts[4] == "na" {
            None
        } else {
            match parts[4].parse() {
                Ok(v) => Some(v),
                Err(_) => {
                    return Err(CountError::InvalidFileName {
                        problem: FileNameProblem::InvalidSpeedLimit,
                        path,
                    })
                }
            }
        };

        let metadata = Self {
            technician,
            dvrpc_num,
            directions,
            counter_id,
            speed_limit,
        };

        Ok(metadata)
    }
}

/// The direction of a lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    North,
    East,
    South,
    West,
}

/// The [`Direction`]s that a count could contain.
#[derive(Debug, Clone, PartialEq)]
pub struct Directions {
    pub direction1: Direction,
    pub direction2: Option<Direction>,
}

impl Directions {
    pub fn new(direction1: Direction, direction2: Option<Direction>) -> Self {
        Self {
            direction1,
            direction2,
        }
    }
}

/// Names of the 15 classifications from the FWA.
///
/// See:
///  * <https://www.fhwa.dot.gov/policyinformation/vehclass.cfm>
///  * <https://www.fhwa.dot.gov/policyinformation/tmguide/tmg_2013/vehicle-types.cfm>
///  * <https://www.fhwa.dot.gov/publications/research/infrastructure/pavements/ltpp/13091/002.cfm>
#[derive(Debug, Clone)]
pub enum VehicleClass {
    Motorcycles,                        // 1
    PassengerCars,                      // 2
    OtherFourTireSingleUnitVehicles,    // 3
    Buses,                              // 4
    TwoAxleSixTireSingleUnitTrucks,     // 5
    ThreeAxleSingleUnitTrucks,          // 6
    FourOrMoreAxleSingleUnitTrucks,     // 7
    FourOrFewerAxleSingleTrailerTrucks, // 8
    FiveAxleSingleTrailerTrucks,        // 9
    SixOrMoreAxleSingleTrailerTrucks,   // 10
    FiveOrFewerAxleMultiTrailerTrucks,  // 11
    SixAxleMultiTrailerTrucks,          // 12
    SevenOrMoreAxleMultiTrailerTrucks,  // 13
    UnclassifiedVehicle,                // 15 (there is an "Unused" class group at 14)
}

impl VehicleClass {
    /// Create a VehicleClass from a number.
    pub fn from_num(num: u8) -> Result<Self, CountError<'static>> {
        match num {
            1 => Ok(VehicleClass::Motorcycles),
            2 => Ok(VehicleClass::PassengerCars),
            3 => Ok(VehicleClass::OtherFourTireSingleUnitVehicles),
            4 => Ok(VehicleClass::Buses),
            5 => Ok(VehicleClass::TwoAxleSixTireSingleUnitTrucks),
            6 => Ok(VehicleClass::ThreeAxleSingleUnitTrucks),
            7 => Ok(VehicleClass::FourOrMoreAxleSingleUnitTrucks),
            8 => Ok(VehicleClass::FourOrFewerAxleSingleTrailerTrucks),
            9 => Ok(VehicleClass::FiveAxleSingleTrailerTrucks),
            10 => Ok(VehicleClass::SixOrMoreAxleSingleTrailerTrucks),
            11 => Ok(VehicleClass::FiveOrFewerAxleMultiTrailerTrucks),
            12 => Ok(VehicleClass::SixAxleMultiTrailerTrucks),
            13 => Ok(VehicleClass::SevenOrMoreAxleMultiTrailerTrucks),
            0 | 14 => Ok(VehicleClass::UnclassifiedVehicle), // TODO: verify this
            other => Err(CountError::BadVehicleClass(other)),
        }
    }
}

/// Count of vehicles by vehicle class (see [`VehicleClass`]) over some time period.
///
/// This is generally - but not always - for 15-minute intervals.
///
/// Note: unclassified vehicles are counted in `c15` field, but also are included in the `c2`
/// (Passenger Cars). Thus, a simple sum of fields `c1` through `c15` would double-count
/// unclassified vehicles.
#[derive(Debug, Clone, Copy)]
pub struct VehicleClassCount {
    pub dvrpc_num: i32,
    pub direction: Direction,
    pub c1: i32,
    pub c2: i32,
    pub c3: i32,
    pub c4: i32,
    pub c5: i32,
    pub c6: i32,
    pub c7: i32,
    pub c8: i32,
    pub c9: i32,
    pub c10: i32,
    pub c11: i32,
    pub c12: i32,
    pub c13: i32,
    pub c15: i32,
    pub total: i32,
}

impl VehicleClassCount {
    /// Create a new count.
    pub fn first(dvrpc_num: i32, direction: Direction, class: VehicleClass) -> Self {
        let mut count = Self {
            dvrpc_num,
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
            total: 1,
        };
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
    }
}

/// Count of vehicles by speed range over some time period.
///
/// This is generally - but not always - for 15-minute intervals.
#[derive(Debug, Clone, Copy)]
pub struct SpeedRangeCount {
    pub dvrpc_num: i32,
    pub direction: Direction,
    pub s1: i32,
    pub s2: i32,
    pub s3: i32,
    pub s4: i32,
    pub s5: i32,
    pub s6: i32,
    pub s7: i32,
    pub s8: i32,
    pub s9: i32,
    pub s10: i32,
    pub s11: i32,
    pub s12: i32,
    pub s13: i32,
    pub s14: i32,
    pub total: i32,
}

impl SpeedRangeCount {
    /// Create a SpeedRangeCount with 0 count for all speed ranges.
    pub fn first(dvrpc_num: i32, direction: Direction, speed: f32) -> Self {
        let mut value = Self {
            dvrpc_num,
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
        };
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

/// Represents rows in the TC_CLACOUNT table.
type FifteenMinuteVehicleClassCount = HashMap<BinnedCountKey, VehicleClassCount>;

/// Represents rows in the TC_SPECOUNT table.
type FifteenMinuteSpeedRangeCount = HashMap<BinnedCountKey, SpeedRangeCount>;

/// Identifies the time and lane for binning vehicle class/speeds.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct BinnedCountKey {
    pub datetime: PrimitiveDateTime,
    pub channel: u8,
}

/// Create the 15-minute binned speed and class counts from [`IndividualVehicle`]s.
pub fn create_speed_and_class_count(
    metadata: CountMetadata,
    counts: Vec<IndividualVehicle>,
) -> (FifteenMinuteSpeedRangeCount, FifteenMinuteVehicleClassCount) {
    let mut fifteen_min_speed_range_count: FifteenMinuteSpeedRangeCount = HashMap::new();
    let mut fifteen_min_vehicle_class_count: FifteenMinuteVehicleClassCount = HashMap::new();

    for count in counts {
        // Get the direction from the channel of count/metadata of filename.
        // Channel 1 is first direction, Channel 2 is the second (if any)
        let direction = match count.channel {
            1 => metadata.directions.direction1,
            2 => metadata.directions.direction2.unwrap(),
            _ => {
                error!("Unable to determine channel/direction.");
                continue;
            }
        };

        // create a key for the Hashmap for 15-minute periods
        let time_part = match time_bin(count.time) {
            Ok(v) => v,
            Err(e) => {
                error!("{e}");
                continue;
            }
        };
        let key = BinnedCountKey {
            datetime: PrimitiveDateTime::new(count.date, time_part),
            channel: count.channel,
        };

        // Add new entry to 15-min speed range count or increment existing one.
        fifteen_min_speed_range_count
            .entry(key)
            .and_modify(|c| {
                c.total += 1;
                c.insert(count.speed)
            })
            .or_insert(SpeedRangeCount::first(
                metadata.dvrpc_num,
                direction,
                count.speed,
            ));

        // Add new entry to 15-min vehicle class count or increment existing one.
        fifteen_min_vehicle_class_count
            .entry(key)
            .and_modify(|c| {
                c.total += 1;
                c.insert(count.class.clone())
            })
            .or_insert(VehicleClassCount::first(
                metadata.dvrpc_num,
                direction,
                count.class,
            ));
    }

    (
        fifteen_min_speed_range_count,
        fifteen_min_vehicle_class_count,
    )
}

/// The key for records of the TC_VOLCOUNT and TC_SPESUM tables.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct NonNormalCountKey {
    pub dvrpc_num: i32,
    pub date: Date,
    pub direction: Direction,
    pub channel: u8,
}

/// Represents rows in the TC_VOLCOUNT table, which does not have hour fields normalized, but a
/// different field for each hour of the day.
type NonNormalVolCount = HashMap<NonNormalCountKey, NonNormalVolCountValue>;

/// Possible weather values.
// TODO: needs fixed - this is just a guess
// TODO: eventually how weather is entered needs overhauled
#[derive(Debug, Clone, Copy)]
pub enum Weather {
    Fair,
    Rain,
    Sunny,
}

/// The rest of the fields in the TC_VOLCOUNT table.
///
/// Hourly fields are `Option` because traffic counts aren't done from 12am one day to 12am the
/// the following day - can start and stop at any time.
#[derive(Debug, Clone, Copy, Default)]
pub struct NonNormalVolCountValue {
    pub setflag: Option<i32>,
    pub totalcount: Option<i32>,
    pub weather: Option<Weather>,
    pub am12: Option<i32>,
    pub am1: Option<i32>,
    pub am2: Option<i32>,
    pub am3: Option<i32>,
    pub am4: Option<i32>,
    pub am5: Option<i32>,
    pub am6: Option<i32>,
    pub am7: Option<i32>,
    pub am8: Option<i32>,
    pub am9: Option<i32>,
    pub am10: Option<i32>,
    pub am11: Option<i32>,
    pub pm12: Option<i32>,
    pub pm1: Option<i32>,
    pub pm2: Option<i32>,
    pub pm3: Option<i32>,
    pub pm4: Option<i32>,
    pub pm5: Option<i32>,
    pub pm6: Option<i32>,
    pub pm7: Option<i32>,
    pub pm8: Option<i32>,
    pub pm9: Option<i32>,
    pub pm10: Option<i32>,
    pub pm11: Option<i32>,
}

impl NonNormalVolCountValue {
    /// Create a NonNormalVolCountValue with `None` for everything except
    /// the total and the first hour/count, which will be `Some(1)`.
    /// (For the first time a new key is created in a HashMap.)
    pub fn first(hour: u8) -> Self {
        let mut value = Self {
            ..Default::default()
        };

        value.totalcount = Some(1);

        match hour {
            0 => value.am12 = Some(1),
            1 => value.am1 = Some(1),
            2 => value.am2 = Some(1),
            3 => value.am3 = Some(1),
            4 => value.am4 = Some(1),
            5 => value.am5 = Some(1),
            6 => value.am6 = Some(1),
            7 => value.am7 = Some(1),
            8 => value.am8 = Some(1),
            9 => value.am9 = Some(1),
            10 => value.am10 = Some(1),
            11 => value.am11 = Some(1),
            12 => value.pm12 = Some(1),
            13 => value.pm1 = Some(1),
            14 => value.pm2 = Some(1),
            15 => value.pm3 = Some(1),
            16 => value.pm4 = Some(1),
            17 => value.pm5 = Some(1),
            18 => value.pm6 = Some(1),
            19 => value.pm7 = Some(1),
            20 => value.pm8 = Some(1),
            21 => value.pm9 = Some(1),
            22 => value.pm10 = Some(1),
            23 => value.pm11 = Some(1),
            _ => (), // ok, because time.hour() can only be 0-23
        }
        value
    }
}

/// Aggregate [`IndividualVehicle`]s into the shape of the TC_VOLCOUNT table.
///
/// This excludes the first and last hour of the overall count,
/// as they are unlikely to be a full hour of data.
pub fn create_non_normal_vol_count(
    metadata: CountMetadata,
    counts: Vec<IndividualVehicle>,
) -> NonNormalVolCount {
    let mut non_normal_vol_count: NonNormalVolCount = HashMap::new();

    if counts.is_empty() {
        return non_normal_vol_count;
    }

    // Determine first and last date/hour of count.
    // (The unwraps are ok, b/c only would be `None` (which would cause panic in following set of
    // assignments) if `counts` empty, and we explicitly check this above and return early if that
    // is the case.)
    let first_count = counts
        .iter()
        .min_by_key(|x| PrimitiveDateTime::new(x.date, x.time))
        .unwrap();

    let last_count = counts
        .iter()
        .max_by_key(|x| PrimitiveDateTime::new(x.date, x.time))
        .unwrap();

    let first_date = first_count.date;
    let first_hour = first_count.time.hour();
    let last_date = last_count.date;
    let last_hour = last_count.time.hour();

    for count in counts {
        // Ignore counts in first and last hour of overall count.
        if (count.date == first_date && count.time.hour() == first_hour)
            || (count.date == last_date && count.time.hour() == last_hour)
        {
            continue;
        }

        // Get the direction from the channel of count/metadata of filename.
        // Channel 1 is first direction, Channel 2 is the second (if any)
        let direction = match count.channel {
            1 => metadata.directions.direction1,
            2 => metadata.directions.direction2.unwrap(),
            _ => {
                error!("Unable to determine channel/direction.");
                continue;
            }
        };

        let key = NonNormalCountKey {
            dvrpc_num: metadata.dvrpc_num,
            date: count.date,
            direction,
            channel: count.channel,
        };

        // Add new entry if necessary, then insert data.
        non_normal_vol_count
            .entry(key)
            .and_modify(|c| {
                c.totalcount = c.totalcount.map_or(Some(1), |c| Some(c + 1));
                match count.time.hour() {
                    0 => c.am12 = c.am12.map_or(Some(1), |c| Some(c + 1)),
                    1 => c.am1 = c.am1.map_or(Some(1), |c| Some(c + 1)),
                    2 => c.am2 = c.am2.map_or(Some(1), |c| Some(c + 1)),
                    3 => c.am3 = c.am3.map_or(Some(1), |c| Some(c + 1)),
                    4 => c.am4 = c.am4.map_or(Some(1), |c| Some(c + 1)),
                    5 => c.am5 = c.am5.map_or(Some(1), |c| Some(c + 1)),
                    6 => c.am6 = c.am6.map_or(Some(1), |c| Some(c + 1)),
                    7 => c.am7 = c.am7.map_or(Some(1), |c| Some(c + 1)),
                    8 => c.am8 = c.am8.map_or(Some(1), |c| Some(c + 1)),
                    9 => c.am9 = c.am9.map_or(Some(1), |c| Some(c + 1)),
                    10 => c.am10 = c.am10.map_or(Some(1), |c| Some(c + 1)),
                    11 => c.am11 = c.am11.map_or(Some(1), |c| Some(c + 1)),
                    12 => c.pm12 = c.pm12.map_or(Some(1), |c| Some(c + 1)),
                    13 => c.pm1 = c.pm1.map_or(Some(1), |c| Some(c + 1)),
                    14 => c.pm2 = c.pm2.map_or(Some(1), |c| Some(c + 1)),
                    15 => c.pm3 = c.pm3.map_or(Some(1), |c| Some(c + 1)),
                    16 => c.pm4 = c.pm4.map_or(Some(1), |c| Some(c + 1)),
                    17 => c.pm5 = c.pm5.map_or(Some(1), |c| Some(c + 1)),
                    18 => c.pm6 = c.pm6.map_or(Some(1), |c| Some(c + 1)),
                    19 => c.pm7 = c.pm7.map_or(Some(1), |c| Some(c + 1)),
                    20 => c.pm8 = c.pm8.map_or(Some(1), |c| Some(c + 1)),
                    21 => c.pm9 = c.pm9.map_or(Some(1), |c| Some(c + 1)),
                    22 => c.pm10 = c.pm10.map_or(Some(1), |c| Some(c + 1)),
                    23 => c.pm11 = c.pm11.map_or(Some(1), |c| Some(c + 1)),
                    _ => (),
                };
            })
            .or_insert(NonNormalVolCountValue::first(count.time.hour()));
    }
    non_normal_vol_count
}

/// Represents rows in the TC_SPESUM table, which does not have hour fields normalized, but a
/// different field for each hour of the day.
type NonNormalSpeedAvgCount = HashMap<NonNormalCountKey, NonNormalSpeedAvgValue>;

// Intermediate map, used to calculate averages
type NonNormalSpeedRawCount = HashMap<NonNormalCountKey, NonNormalSpeedRawValue>;

/// The rest of the fields in the TC_SPESUM table.
///
/// Hourly fields are `Option` because traffic counts aren't done from 12am one day to 12am the
/// the following day - can start and stop at any time.
#[derive(Debug, Clone, Default)]
pub struct NonNormalSpeedAvgValue {
    // pub lane: i32,
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

impl NonNormalSpeedAvgValue {
    /// Create a NonNormalSpeedAvgValue with empty Vecs.
    /// (For the first time a new key is created in a HashMap.)
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
            _ => (), // ok, because time.hour() can only be 0-23
        }
        value
    }
}

#[derive(Debug, Clone, Default)]
struct NonNormalSpeedRawValue {
    // pub lane: i32,
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
impl NonNormalSpeedRawValue {
    /// Create a NonNormalSpeedAvgValue with empty Vecs.
    /// (For the first time a new key is created in a HashMap.)
    fn first(hour: u8, speed: f32) -> Self {
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

/// Aggregate [`IndividualVehicle`]s into the shape of the TC_SPESUM table.
///
/// This excludes the first and last hour of the overall count,
/// as they are unlikely to be a full hour of data.
pub fn create_non_normal_speedavg_count(
    metadata: CountMetadata,
    counts: Vec<IndividualVehicle>,
) -> NonNormalSpeedAvgCount {
    let mut non_normal_speed_raw_count: NonNormalSpeedRawCount = HashMap::new();
    let mut non_normal_speed_avg_count: NonNormalSpeedAvgCount = HashMap::default();

    if counts.is_empty() {
        return non_normal_speed_avg_count;
    }

    // Determine first and last date/hour of count.
    // (The unwraps are ok, b/c only would be `None` (which would cause panic in following set of
    // assignments) if `counts` empty, and we explicitly check this above and return early if that
    // is the case.)
    let first_count = counts
        .iter()
        .min_by_key(|x| PrimitiveDateTime::new(x.date, x.time))
        .unwrap();

    let last_count = counts
        .iter()
        .max_by_key(|x| PrimitiveDateTime::new(x.date, x.time))
        .unwrap();

    let first_date = first_count.date;
    let first_hour = first_count.time.hour();
    let last_date = last_count.date;
    let last_hour = last_count.time.hour();

    for count in counts {
        // Ignore counts in first and last hour of overall count.
        if (count.date == first_date && count.time.hour() == first_hour)
            || (count.date == last_date && count.time.hour() == last_hour)
        {
            continue;
        }

        // Get the direction from the channel of count/metadata of filename.
        // Channel 1 is first direction, Channel 2 is the second (if any)
        let direction = match count.channel {
            1 => metadata.directions.direction1,
            2 => metadata.directions.direction2.unwrap(),
            _ => {
                error!("Unable to determine channel/direction.");
                continue;
            }
        };

        let key = NonNormalCountKey {
            dvrpc_num: metadata.dvrpc_num,
            date: count.date,
            direction,
            channel: count.channel,
        };

        // Add new entry if necessary, then insert data.
        non_normal_speed_raw_count
            .entry(key)
            .and_modify(|c| {
                match count.time.hour() {
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
            .or_insert(NonNormalSpeedRawValue::first(
                count.time.hour(),
                count.speed,
            ));
    }

    // Now calculate the average from the vecs.
    for (key, value) in non_normal_speed_raw_count {
        if !value.am12.is_empty() {
            let average = value.am12.iter().sum::<f32>() / value.am12.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.am12 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("am12", average));
        }
        if !value.am1.is_empty() {
            let average = value.am1.iter().sum::<f32>() / value.am1.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.am1 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("am1", average));
        }
        if !value.am2.is_empty() {
            let average = value.am2.iter().sum::<f32>() / value.am2.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.am2 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("am2", average));
        }
        if !value.am3.is_empty() {
            let average = value.am3.iter().sum::<f32>() / value.am3.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.am3 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("am3", average));
        }
        if !value.am4.is_empty() {
            let average = value.am4.iter().sum::<f32>() / value.am4.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.am4 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("am4", average));
        }
        if !value.am5.is_empty() {
            let average = value.am5.iter().sum::<f32>() / value.am5.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.am5 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("am5", average));
        }
        if !value.am6.is_empty() {
            let average = value.am6.iter().sum::<f32>() / value.am6.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.am6 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("am6", average));
        }
        if !value.am7.is_empty() {
            let average = value.am7.iter().sum::<f32>() / value.am7.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.am7 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("am7", average));
        }
        if !value.am8.is_empty() {
            let average = value.am8.iter().sum::<f32>() / value.am8.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.am8 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("am8", average));
        }
        if !value.am9.is_empty() {
            let average = value.am9.iter().sum::<f32>() / value.am9.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.am9 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("am9", average));
        }
        if !value.am10.is_empty() {
            let average = value.am10.iter().sum::<f32>() / value.am10.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.am10 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("am10", average));
        }
        if !value.am11.is_empty() {
            let average = value.am11.iter().sum::<f32>() / value.am11.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.am11 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("am11", average));
        }
        if !value.pm12.is_empty() {
            let average = value.pm12.iter().sum::<f32>() / value.pm12.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.pm12 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("pm12", average));
        }
        if !value.pm1.is_empty() {
            let average = value.pm1.iter().sum::<f32>() / value.pm1.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.pm1 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("pm1", average));
        }
        if !value.pm2.is_empty() {
            let average = value.pm2.iter().sum::<f32>() / value.pm2.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.pm2 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("pm2", average));
        }
        if !value.pm3.is_empty() {
            let average = value.pm3.iter().sum::<f32>() / value.pm3.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.pm3 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("pm3", average));
        }
        if !value.pm4.is_empty() {
            let average = value.pm4.iter().sum::<f32>() / value.pm4.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.pm4 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("pm4", average));
        }
        if !value.pm5.is_empty() {
            let average = value.pm5.iter().sum::<f32>() / value.pm5.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.pm5 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("pm5", average));
        }
        if !value.pm6.is_empty() {
            let average = value.pm6.iter().sum::<f32>() / value.pm6.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.pm6 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("pm6", average));
        }
        if !value.pm7.is_empty() {
            let average = value.pm7.iter().sum::<f32>() / value.pm7.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.pm7 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("pm7", average));
        }
        if !value.pm8.is_empty() {
            let average = value.pm8.iter().sum::<f32>() / value.pm8.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.pm8 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("pm8", average));
        }
        if !value.pm9.is_empty() {
            let average = value.pm9.iter().sum::<f32>() / value.pm9.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.pm9 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("pm9", average));
        }
        if !value.pm10.is_empty() {
            let average = value.pm10.iter().sum::<f32>() / value.pm10.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.pm10 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("pm10", average));
        }
        if !value.pm11.is_empty() {
            let average = value.pm11.iter().sum::<f32>() / value.pm11.len() as f32;
            non_normal_speed_avg_count
                .entry(key)
                .and_modify(|c| c.pm11 = Some(average))
                .or_insert(NonNormalSpeedAvgValue::first("pm11", average));
        }
    }

    non_normal_speed_avg_count
}

/// Put time into four bins per hour.
pub fn time_bin(time: Time) -> Result<Time, time::error::ComponentRange> {
    let time = time.replace_second(0)?;
    match time.minute() {
        0..=14 => Ok(time.replace_minute(0)?),
        15..=29 => Ok(time.replace_minute(15)?),
        30..=44 => Ok(time.replace_minute(30)?),
        _ => Ok(time.replace_minute(45)?), // minute is always 0-59, so this is 45-59
    }
}

/// Get `InputCount` and number of rows in file before data starts (metadata rows + header).
/*
  This is a rather naive solution - it simply checks that the exact string (
  stripped of double quotes and spaces) of one of the potential headers (and thus `InputCount`)
  is in the file. To make it somewhat performant, it limits the search to the first 50 lines, which
  is an egregiously large number to ensure that we will never miss the header and prevents the
  search going through tens of thousands of lines, which is the typical number in files.

  Two values are returned because the exact same procedure is needed to determine
  either of them. Convenience functions using it are also available to get only one value.
*/
fn count_type_and_num_nondata_rows(path: &Path) -> Result<(InputCount, usize), CountError> {
    let mut num_rows = 0;
    let contents = fs::read_to_string(path)?;
    for line in contents.lines().take(50) {
        num_rows += 1;
        let line = line.replace(['"', ' '], "");
        if line.contains(FIFTEEN_MINUTE_VEHICLE_HEADER) {
            return Ok((InputCount::FifteenMinuteVehicle, num_rows));
        } else if line.contains(INDIVIDUAL_VEHICLE_HEADER) {
            return Ok((InputCount::IndividualVehicle, num_rows));
        }
        // TODO: other header checks/types to be added
    }
    Err(CountError::BadHeader(path))
}

/// Get the number of nondata rows in a file.
pub fn num_nondata_rows(path: &Path) -> Result<usize, CountError> {
    let (_, num_rows) = count_type_and_num_nondata_rows(path)?;
    Ok(num_rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_binning_is_correct() {
        // 1st 15-minute bin
        let time = Time::from_hms(10, 0, 0).unwrap();

        let binned = time_bin(time).unwrap();
        assert_eq!(binned, Time::from_hms(10, 0, 0).unwrap());
        assert_ne!(binned, Time::from_hms(10, 10, 0).unwrap());

        let time = Time::from_hms(10, 14, 00).unwrap();
        let binned = time_bin(time).unwrap();
        assert_eq!(binned, Time::from_hms(10, 0, 0).unwrap());

        // 2nd 15-minute bin
        let time = Time::from_hms(10, 25, 00).unwrap();

        let binned = time_bin(time).unwrap();
        assert_eq!(binned, Time::from_hms(10, 15, 0).unwrap());

        let time = Time::from_hms(10, 29, 00).unwrap();

        let binned = time_bin(time).unwrap();
        assert_eq!(binned, Time::from_hms(10, 15, 0).unwrap());

        // 3rd 15-minute bin
        let time = Time::from_hms(10, 31, 00).unwrap();

        let binned = time_bin(time).unwrap();
        assert_eq!(binned, Time::from_hms(10, 30, 0).unwrap());

        let time = Time::from_hms(10, 44, 00).unwrap();

        let binned = time_bin(time).unwrap();
        assert_eq!(binned, Time::from_hms(10, 30, 0).unwrap());

        // 4th 15-minute bin
        let time = Time::from_hms(10, 45, 00).unwrap();

        let binned = time_bin(time).unwrap();
        assert_eq!(binned, Time::from_hms(10, 45, 0).unwrap());

        let time = Time::from_hms(10, 59, 00).unwrap();

        let binned = time_bin(time).unwrap();
        assert_eq!(binned, Time::from_hms(10, 45, 0).unwrap());
    }

    #[test]
    fn count_type_from_location_correct_ind_veh() {
        let count_type = InputCount::from_parent_dir(Path::new("/vehicle/count_data.csv")).unwrap();
        assert_eq!(count_type, InputCount::IndividualVehicle)
    }

    #[test]
    fn count_type_from_location_correct_15min_veh() {
        let count_type =
            InputCount::from_parent_dir(Path::new("/15minutevehicle/count_data.csv")).unwrap();
        assert_eq!(count_type, InputCount::FifteenMinuteVehicle)
    }

    #[test]
    fn count_type_from_location_correct_15min_bicycle() {
        let count_type =
            InputCount::from_parent_dir(Path::new("/15minutebicycle/count_data.csv")).unwrap();
        assert_eq!(count_type, InputCount::FifteenMinuteBicycle)
    }

    #[test]
    fn count_type_from_location_correct_15min_ped() {
        let count_type =
            InputCount::from_parent_dir(Path::new("/15minutepedestrian/count_data.csv")).unwrap();
        assert_eq!(count_type, InputCount::FifteenMinutePedestrian)
    }

    #[test]
    fn count_type_from_location_errs_if_invalid_dir() {
        let count_type = InputCount::from_parent_dir(Path::new("/not_count_dir/count_data.csv"));
        assert!(matches!(count_type, Err(CountError::BadLocation(_))))
    }

    #[test]
    fn count_type_and_num_nondata_rows_correct_15min_veh_sample() {
        let path = Path::new("test_files/15minutevehicle/rc-168193-ew-39352-na.txt");
        let (count_type, num_rows) = count_type_and_num_nondata_rows(path).unwrap();
        assert_eq!(count_type, InputCount::FifteenMinuteVehicle);
        assert_eq!(num_rows, 5);
    }

    #[test]
    fn count_type_and_num_nondata_rows_correct_ind_veh_sample() {
        let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
        let (count_type, num_rows) = count_type_and_num_nondata_rows(path).unwrap();
        assert_eq!(count_type, InputCount::IndividualVehicle);
        assert_eq!(num_rows, 4);
    }

    #[test]
    fn count_type_and_num_nondata_rows_errs_if_no_matching_header() {
        let path = Path::new("test_files/bad_header.txt");
        assert!(matches!(
            count_type_and_num_nondata_rows(path),
            Err(CountError::BadHeader(_))
        ))
    }

    #[test]
    fn num_nondata_rows_correct() {
        let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
        let num_rows = num_nondata_rows(path).unwrap();
        assert_eq!(num_rows, 4);
    }

    #[test]
    fn count_type_ind_veh_from_header_correct() {
        let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
        let ct_from_header = InputCount::from_header(path).unwrap();
        assert_eq!(ct_from_header, InputCount::IndividualVehicle);
    }

    #[test]
    fn count_type_15min_veh_from_header_correct() {
        let path = Path::new("test_files/15minutevehicle/rc-168193-ew-39352-na.txt");
        let ct_from_header = InputCount::from_header(path).unwrap();
        assert_eq!(ct_from_header, InputCount::FifteenMinuteVehicle);
    }

    #[test]
    fn count_type_from_header_errs_if_non_extant_file() {
        let path = Path::new("test_files/not_a_file.csv");
        assert!(matches!(
            InputCount::from_header(path),
            Err(CountError::CannotOpenFile { .. })
        ))
    }

    #[test]
    fn count_type_from_header_errs_if_no_matching_header() {
        let path = Path::new("test_files/bad_header.txt");
        assert!(matches!(
            InputCount::from_header(path),
            Err(CountError::BadHeader(_))
        ))
    }

    #[test]
    fn count_type_from_parent_dir_and_header_15min_veh_correct() {
        let path = Path::new("test_files/15minutevehicle/rc-168193-ew-39352-na.txt");
        let count_type = InputCount::from_parent_dir_and_header(path).unwrap();
        assert_eq!(count_type, InputCount::FifteenMinuteVehicle);
    }

    #[test]
    fn count_type_from_parent_dir_and_header_ind_veh_correct() {
        let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
        let count_type = InputCount::from_parent_dir_and_header(path).unwrap();
        assert_eq!(count_type, InputCount::IndividualVehicle);
    }

    #[test]
    fn count_type_from_parent_dir_and_errs_if_mismatch1() {
        let path = Path::new("test_files/15minutevehicle/ind_veh_count.txt");
        let count_type = InputCount::from_parent_dir_and_header(path);
        assert!(matches!(
            count_type,
            Err(CountError::LocationHeaderMisMatch(_))
        ))
    }

    #[test]
    fn count_type_from_parent_dir_and_errs_if_mismatch2() {
        let path = Path::new("test_files/vehicle/15min_veh_count.txt");
        let count_type = InputCount::from_parent_dir_and_header(path);
        assert!(matches!(
            count_type,
            Err(CountError::LocationHeaderMisMatch(_))
        ))
    }
}

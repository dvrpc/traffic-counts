//! This library contains data structures related to DVRPC's traffic counts,
//! traits for [extracting][`extract_from_file::Extract`] data from files
//! and [interacting with the database][`db::CountTable`],
//! and operations like calculating the average annual daily traffic.
//!
//! The binary [import](../import/index.html) program implements extracting data from files
//! and inserting it into our database. See its documentation for further details, including
//! the filename specification and the types of counts it can create.
//!
//! This library also lays the foundations for the inverse need - getting data from the database
//! in order to act on it or display it in some way, whether for CRUD user interfaces or other
//! uses.
//!
//! See <https://www.dvrpc.org/traffic/> for additional information about traffic counting.

use std::collections::HashMap;
use std::fmt::Display;
use std::fs;
use std::io;
use std::path::Path;

use log::error;
use thiserror::Error;
use time::{Date, Duration, PrimitiveDateTime, Time};

pub mod annual_avg;
pub mod db;
pub mod extract_from_file;
pub mod intermediate;
use annual_avg::GetDate;
use intermediate::*;

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
    #[error("database error `{0}`")]
    DbError(#[from] oracle::Error),
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
    /// Pre-binned, 15-minute volume counts from StarNext/Jamar.
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

/// An individual vehicle that has been counted, with no binning applied to it,
/// including vehicle classification and speed.
///
/// Four different kinds of counts can be derived from this type of data:
///   - volume by class per time period ([`TimeBinnedVehicleClassCount`])
///   - volume by speed range per time period ([`TimeBinnedSpeedRangeCount`])
///   - volume per hour of the day ([`NonNormalVolCount`])
///   - average speed per hour of the day ([`NonNormalAvgSpeedCount`])
///
/// See [`create_speed_and_class_count`], [`create_non_normal_vol_count`], and
/// [`create_non_normal_speedavg_count`].
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

/// Pre-binned, 15-minute volume counts (TC_15MINVOLCOUNT table).
#[derive(Debug, Clone)]
pub struct FifteenMinuteVehicle {
    pub dvrpc_num: i32,
    pub date: Date,
    pub time: Time,
    pub count: u16,
    pub direction: Direction,
    pub channel: u8,
}

impl GetDate for FifteenMinuteVehicle {
    fn get_date(&self) -> Date {
        self.date.to_owned()
    }
}

impl FifteenMinuteVehicle {
    pub fn new(
        dvrpc_num: i32,
        date: Date,
        time: Time,
        count: u16,
        direction: Direction,
        channel: u8,
    ) -> Result<Self, CountError<'static>> {
        Ok(Self {
            dvrpc_num,
            date,
            time,
            count,
            direction,
            channel,
        })
    }
}

/// The metadata of an input count, including technician, id, direction(s), count machine id,
/// and - potentially - the speed limit.
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

impl Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let dir = match self {
            Direction::North => "north".to_string(),
            Direction::East => "east".to_string(),
            Direction::South => "south".to_string(),
            Direction::West => "west".to_string(),
        };
        write!(f, "{}", dir)
    }
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
/// NOTE: There is an "Unused" class at 14, which is excluded (presumably its for a future, yet
/// undefined, class). However, JAMAR/StarNext uses "14" for unclassfied vehicles, and doesn't use
/// 15. To cover both cases, 14 and 15 are considered unclassified in `from_num`.
///
/// See:
///  * <https://www.fhwa.dot.gov/policyinformation/vehclass.cfm>
///  * <https://www.fhwa.dot.gov/policyinformation/tmguide/tmg_2013/vehicle-types.cfm>
///  * <https://www.fhwa.dot.gov/publications/research/infrastructure/pavements/ltpp/13091/002.cfm>
#[repr(u8)]
#[derive(Debug, Clone)]
pub enum VehicleClass {
    Motorcycles = 1,
    PassengerCars = 2,
    OtherFourTireSingleUnitVehicles = 3,
    Buses = 4,
    TwoAxleSixTireSingleUnitTrucks = 5,
    ThreeAxleSingleUnitTrucks = 6,
    FourOrMoreAxleSingleUnitTrucks = 7,
    FourOrFewerAxleSingleTrailerTrucks = 8,
    FiveAxleSingleTrailerTrucks = 9,
    SixOrMoreAxleSingleTrailerTrucks = 10,
    FiveOrFewerAxleMultiTrailerTrucks = 11,
    SixAxleMultiTrailerTrucks = 12,
    SevenOrMoreAxleMultiTrailerTrucks = 13,
    UnclassifiedVehicle = 15,
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
            0 | 14 | 15 => Ok(VehicleClass::UnclassifiedVehicle),
            other => Err(CountError::BadVehicleClass(other)),
        }
    }
}

/// Count of [vehicles by class][`VehicleClass`],
/// binned into 15-minute or hourly intervals (TC_CLACOUNT table).
///
/// We almost always want fifteen-minute counts, but hourly is also an option.
#[derive(Debug, Clone)]
pub struct TimeBinnedVehicleClassCount {
    pub datetime: PrimitiveDateTime,
    pub channel: u8,
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

/// Count of vehicles by speed range,
/// binned into 15-minute or hourly intervals (TC_SPECOUNT table).
///
/// We almost always want fifteen-minute counts, but hourly is also an option.
#[derive(Debug, Clone)]
pub struct TimeBinnedSpeedRangeCount {
    pub datetime: PrimitiveDateTime,
    pub channel: u8,
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

/// Create time-binned speed and class counts from [`IndividualVehicle`]s.
///
/// The first and last records are dropped, as they likely contain incomplete data.
pub fn create_speed_and_class_count(
    metadata: CountMetadata,
    mut counts: Vec<IndividualVehicle>,
    interval: TimeInterval,
) -> (
    Vec<TimeBinnedSpeedRangeCount>,
    Vec<TimeBinnedVehicleClassCount>,
) {
    if counts.is_empty() {
        return (vec![], vec![]);
    }

    let mut speed_range_map: HashMap<BinnedCountKey, SpeedRangeCount> = HashMap::new();
    let mut vehicle_class_map: HashMap<BinnedCountKey, VehicleClassCount> = HashMap::new();

    for count in counts.clone() {
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

        // Create a key for the Hashmap for time intervals
        let time_part = match bin_time(count.time, interval) {
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

        // Add new entry to 15-min speed range map or increment existing one.
        speed_range_map
            .entry(key)
            .and_modify(|c| c.insert(count.speed))
            .or_insert(SpeedRangeCount::first(
                metadata.dvrpc_num,
                direction,
                count.speed,
            ));

        // Add new entry to 15-min vehicle class map or increment existing one.
        vehicle_class_map
            .entry(key)
            .and_modify(|c| c.insert(count.class.clone()))
            .or_insert(VehicleClassCount::first(
                metadata.dvrpc_num,
                direction,
                count.class,
            ));
    }

    /*
      If there was some time period (whose length is `interval`) where no vehicle was counted,
      there will be no corresponding entry in our HashMap for it. However, that's because of the
      data we are using - `IndividualVehicle`s, which are vehicles that were counted - not because
      there is missing data for that time period. So create those where necessary.
    */

    // Sort counts by date and time, get range, check if number of records is less than expected
    // for every period to be included, insert any missing.
    counts.sort_unstable_by_key(|c| (c.date, c.time));

    let first_dt =
        PrimitiveDateTime::new(counts.first().unwrap().date, counts.first().unwrap().time);
    let last_dt = PrimitiveDateTime::new(counts.last().unwrap().date, counts.last().unwrap().time);

    let all_datetimes = create_time_bins(first_dt, last_dt, interval);

    if all_datetimes.len() < speed_range_map.len() {
        let mut all_keys = vec![];
        let all_channels = if metadata.directions.direction2.is_some() {
            vec![1, 2]
        } else {
            vec![1]
        };

        // construct all possible keys
        for datetime in all_datetimes.clone() {
            for channel in all_channels.iter() {
                all_keys.push(BinnedCountKey {
                    datetime,
                    channel: *channel,
                })
            }
        }
        // Add missing periods for speed range count
        for key in all_keys {
            let direction = match key.channel {
                1 => metadata.directions.direction1,
                2 => metadata.directions.direction2.unwrap(),
                _ => {
                    error!("Unable to determine channel/direction.");
                    continue;
                }
            };
            speed_range_map
                .entry(key)
                .or_insert(SpeedRangeCount::new(metadata.dvrpc_num, direction));
            vehicle_class_map
                .entry(key)
                .or_insert(VehicleClassCount::new(metadata.dvrpc_num, direction));
        }
    }

    // Convert speed range count from HashMap to Vec.
    let mut speed_range_count = vec![];
    for (key, value) in speed_range_map {
        speed_range_count.push(TimeBinnedSpeedRangeCount {
            datetime: key.datetime,
            channel: key.channel,
            dvrpc_num: value.dvrpc_num,
            direction: value.direction,
            s1: value.s1,
            s2: value.s2,
            s3: value.s3,
            s4: value.s4,
            s5: value.s5,
            s6: value.s6,
            s7: value.s7,
            s8: value.s8,
            s9: value.s9,
            s10: value.s10,
            s11: value.s11,
            s12: value.s12,
            s13: value.s13,
            s14: value.s14,
            total: value.total,
        });
    }

    // Convert vehicle class from HashMap to Vec.
    let mut vehicle_class_count = vec![];
    for (key, value) in vehicle_class_map {
        vehicle_class_count.push(TimeBinnedVehicleClassCount {
            datetime: key.datetime,
            channel: key.channel,
            dvrpc_num: value.dvrpc_num,
            direction: value.direction,
            c1: value.c1,
            c2: value.c2,
            c3: value.c3,
            c4: value.c4,
            c5: value.c5,
            c6: value.c6,
            c7: value.c7,
            c8: value.c8,
            c9: value.c9,
            c10: value.c10,
            c11: value.c11,
            c12: value.c12,
            c13: value.c13,
            c15: value.c15,
            total: value.total,
        });
    }

    // Drop the first and last one periods (will have incomplete data).
    // May need to drop two records if there are counts in both directions.
    speed_range_count.sort_unstable_by_key(|c| (c.datetime, c.channel));
    vehicle_class_count.sort_unstable_by_key(|c| (c.datetime, c.channel));
    if metadata.directions.direction2.is_some() {
        speed_range_count.remove(0);
        speed_range_count.remove(0);
        speed_range_count.pop();
        speed_range_count.pop();

        vehicle_class_count.remove(0);
        vehicle_class_count.remove(0);
        vehicle_class_count.pop();
        vehicle_class_count.pop();
    } else {
        speed_range_count.remove(0);
        speed_range_count.pop();

        vehicle_class_count.remove(0);
        vehicle_class_count.pop();
    }

    (speed_range_count, vehicle_class_count)
}

/// Possible weather values.
// TODO: needs fixed - this is just a guess
// TODO: eventually how weather is entered needs overhauled
#[derive(Debug, Clone)]
pub enum Weather {
    Fair,
    Rain,
    Sunny,
}

/// Non-normalized volume counts (TC_VOLCOUNT table).
///
/// Hourly fields are `Option` because traffic counts aren't done from 12am one day to 12am the
/// the following day - can start and stop at any time.
#[derive(Debug, Clone)]
pub struct NonNormalVolCount {
    pub dvrpc_num: i32,
    pub date: Date,
    pub direction: Direction,
    pub channel: u8,
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

/// Create non-normalized volume counts from [`IndividualVehicle`]s.
///
/// This excludes the first and last hour of the overall count,
/// as they are unlikely to be a full hour of data.
pub fn create_non_normal_vol_count(
    metadata: CountMetadata,
    counts: Vec<IndividualVehicle>,
) -> Vec<NonNormalVolCount> {
    let mut non_normal_vol_map: HashMap<NonNormalCountKey, NonNormalVolCountValue> = HashMap::new();

    if counts.is_empty() {
        return vec![];
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
        non_normal_vol_map
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
    // Convert HashMap to Vec of structs.
    let mut non_normal_vol_count = vec![];
    for (key, value) in non_normal_vol_map {
        non_normal_vol_count.push(NonNormalVolCount {
            dvrpc_num: key.dvrpc_num,
            date: key.date,
            direction: key.direction,
            channel: key.channel,
            setflag: None,
            totalcount: value.totalcount,
            weather: value.weather,
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
    non_normal_vol_count
}

/// Non-normalized average speed counts (TC_SPESUM table).
///
/// Hourly fields are `Option` because traffic counts aren't done from 12am one day to 12am the
/// the following day - can start and stop at any time.
#[derive(Debug, Clone)]
pub struct NonNormalAvgSpeedCount {
    pub dvrpc_num: i32,
    pub date: Date,
    pub direction: Direction,
    pub channel: u8,
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
///
/// This excludes the first and last hour of the overall count,
/// as they are unlikely to be a full hour of data.
pub fn create_non_normal_speedavg_count(
    metadata: CountMetadata,
    counts: Vec<IndividualVehicle>,
) -> Vec<NonNormalAvgSpeedCount> {
    let mut non_normal_raw_speed_map: HashMap<NonNormalCountKey, NonNormalRawSpeedValue> =
        HashMap::new();
    let mut non_normal_avg_speed_map: HashMap<NonNormalCountKey, NonNormalAvgSpeedValue> =
        HashMap::default();

    if counts.is_empty() {
        return vec![];
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

    // Collect all the speeds per fields in key.
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
        non_normal_raw_speed_map
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
            .or_insert(NonNormalRawSpeedValue::first(
                count.time.hour(),
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
            dvrpc_num: key.dvrpc_num,
            date: key.date,
            direction: key.direction,
            channel: key.channel,
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

/// Time interval to bin data by.
#[derive(Clone, Copy)]
pub enum TimeInterval {
    Hour,
    FifteenMin,
}

/// Bin time by fifteen-minute or hourly intervals by changing the minute.
pub fn bin_time(time: Time, interval: TimeInterval) -> Result<Time, time::error::ComponentRange> {
    let time = time.replace_second(0)?;

    match interval {
        TimeInterval::Hour => Ok(time.replace_minute(0)?),
        TimeInterval::FifteenMin => {
            match time.minute() {
                0..=14 => Ok(time.replace_minute(0)?),
                15..=29 => Ok(time.replace_minute(15)?),
                30..=44 => Ok(time.replace_minute(30)?),
                _ => Ok(time.replace_minute(45)?), // minute is always 0-59, so this is 45-59
            }
        }
    }
}

/// Create all intervals between (and including) a first and last datetime.
pub fn create_time_bins(
    first_dt: PrimitiveDateTime,
    last_dt: PrimitiveDateTime,
    interval: TimeInterval,
) -> Vec<PrimitiveDateTime> {
    let first_bin = PrimitiveDateTime::new(
        first_dt.date(),
        bin_time(first_dt.time(), interval).unwrap(),
    );

    let last_bin =
        PrimitiveDateTime::new(last_dt.date(), bin_time(last_dt.time(), interval).unwrap());

    let mut dts: Vec<PrimitiveDateTime> = vec![];

    let mut current_bin = first_bin;

    let time_to_add = match interval {
        TimeInterval::Hour => Duration::HOUR,
        TimeInterval::FifteenMin => Duration::minutes(15),
    };

    while current_bin <= last_bin {
        dts.push(current_bin);
        current_bin = current_bin.saturating_add(time_to_add);
    }
    dts
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
    use time::macros::datetime;

    #[test]
    fn time_binning_fifteen_min_is_correct() {
        // 1st 15-minute bin
        let time = Time::from_hms(10, 0, 0).unwrap();

        let binned = bin_time(time, TimeInterval::FifteenMin).unwrap();
        assert_eq!(binned, Time::from_hms(10, 0, 0).unwrap());
        assert_ne!(binned, Time::from_hms(10, 10, 0).unwrap());

        let time = Time::from_hms(10, 14, 00).unwrap();
        let binned = bin_time(time, TimeInterval::FifteenMin).unwrap();
        assert_eq!(binned, Time::from_hms(10, 0, 0).unwrap());

        // 2nd 15-minute bin
        let time = Time::from_hms(10, 25, 00).unwrap();

        let binned = bin_time(time, TimeInterval::FifteenMin).unwrap();
        assert_eq!(binned, Time::from_hms(10, 15, 0).unwrap());

        let time = Time::from_hms(10, 29, 00).unwrap();

        let binned = bin_time(time, TimeInterval::FifteenMin).unwrap();
        assert_eq!(binned, Time::from_hms(10, 15, 0).unwrap());

        // 3rd 15-minute bin
        let time = Time::from_hms(10, 31, 00).unwrap();

        let binned = bin_time(time, TimeInterval::FifteenMin).unwrap();
        assert_eq!(binned, Time::from_hms(10, 30, 0).unwrap());

        let time = Time::from_hms(10, 44, 00).unwrap();

        let binned = bin_time(time, TimeInterval::FifteenMin).unwrap();
        assert_eq!(binned, Time::from_hms(10, 30, 0).unwrap());

        // 4th 15-minute bin
        let time = Time::from_hms(10, 45, 00).unwrap();

        let binned = bin_time(time, TimeInterval::FifteenMin).unwrap();
        assert_eq!(binned, Time::from_hms(10, 45, 0).unwrap());

        let time = Time::from_hms(10, 59, 00).unwrap();

        let binned = bin_time(time, TimeInterval::FifteenMin).unwrap();
        assert_eq!(binned, Time::from_hms(10, 45, 0).unwrap());
    }

    #[test]
    fn time_binning_hourly_is_correct() {
        // the time we are trying to bin to
        let expected = Time::from_hms(10, 0, 0).unwrap();
        // the interval to use
        let interval = TimeInterval::Hour;

        assert_eq!(
            bin_time(Time::from_hms(10, 0, 0).unwrap(), interval).unwrap(),
            expected
        );
        assert_eq!(
            bin_time(Time::from_hms(10, 15, 0).unwrap(), interval).unwrap(),
            expected
        );
        assert_eq!(
            bin_time(Time::from_hms(10, 16, 0).unwrap(), interval).unwrap(),
            expected
        );
        assert_eq!(
            bin_time(Time::from_hms(10, 31, 0).unwrap(), interval).unwrap(),
            expected
        );
        assert_eq!(
            bin_time(Time::from_hms(10, 59, 0).unwrap(), interval).unwrap(),
            expected
        );
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

    #[test]
    fn create_time_bins_correct() {
        let first_dt = datetime!(2024 - 04 - 08 7:00);
        let last_dt = datetime!(2024 - 04 - 08 7:14);
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 1);
        assert_eq!(keys_hour.len(), 1);

        let first_dt = datetime!(2024 - 04 - 08 7:00);
        let last_dt = datetime!(2024 - 04 - 08 7:15);
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 2);
        assert_eq!(keys_hour.len(), 1);

        let first_dt = datetime!(2024 - 04 - 08 7:00);
        let last_dt = datetime!(2024 - 04 - 08 7:59);
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 4);
        assert_eq!(keys_hour.len(), 1);

        let first_dt = datetime!(2024 - 04 - 08 7:00);
        let last_dt = datetime!(2024 - 04 - 08 8:59);
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 8);
        assert_eq!(keys_hour.len(), 2);

        let first_dt = datetime!(2024-04-08 0:00);
        let last_dt = datetime!(2024-04-08 23:59);
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 96);
        assert_eq!(keys_hour.len(), 24);

        let first_dt = datetime!(2024-04-08 0:00);
        let last_dt = datetime!(2024-04-09 0:00);
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 97);
        assert_eq!(keys_hour.len(), 25);

        // spanning two months
        let first_dt = datetime!(2024-03-31 23:00);
        let last_dt = datetime!(2024-04-01 00:01);
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 5);
        assert_eq!(keys_hour.len(), 2);

        // spanning two years
        let first_dt = datetime!(2023-12-31 23:00);
        let last_dt = datetime!(2024-01-01 00:01);
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 5);
        assert_eq!(keys_hour.len(), 2);
    }
}

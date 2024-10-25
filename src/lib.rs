//! This library contains data structures related to DVRPC's traffic counts
//! and enables performing various kinds of operations on them, like
//! [extracting][extract_from_file] data from files,
//! [CRUD db operations][db::crud],
//! [denormalizing][denormalize] count data,
//! and [calculating/inserting][aadv] the annual average daily traffic volumes.
//!
//! The [import](../import/index.html) program implements extracting data from files
//! and inserting it into our database. See its documentation for further details, including
//! the filename specification and the types of counts it can create.
//!
//! Another program, currently named [upsert_factors](../upsert_factors/index.html) - though
//! that may be changed in the future to better reflect what it does - updates factors used to
//! calculate annual average daily volumes. Extended documentation forthcoming.
//!
//! Finally, a [web interface](../webui/index.html) is currently under development for viewing and
//! administering the database.
//!
//! See <https://www.dvrpc.org/traffic/> for additional information about traffic counting.

use std::collections::HashMap;
use std::fmt::Display;
use std::io;
use std::num::ParseIntError;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Timelike};
use log::error;
use oracle::RowValue;
use thiserror::Error;

pub mod aadv;
pub mod check_data;
pub mod db;
pub mod denormalize;
pub mod extract_from_file;
pub mod intermediate;
use intermediate::*;

/// A trait for getting a [`NaiveDate`](https://docs.rs/chrono/latest/chrono/struct.NaiveDate.html)
/// from a type.
pub trait GetDate {
    fn get_date(&self) -> NaiveDate;
}

/// Various errors that can occur.
#[derive(Debug, Error)]
pub enum CountError {
    #[error("unknown count type '{0}'")]
    UnknownCountType(String),
    #[error("problem with file or directory path")]
    BadPath(PathBuf),
    #[error("unable to open file '{0}'")]
    CannotOpenFile(#[from] io::Error),
    #[error("the filename at {path:?} is not to specification: {problem:?}")]
    InvalidFileName {
        problem: FileNameProblem,
        path: PathBuf,
    },
    #[error("no matching count type for directory '{0}'")]
    BadLocation(String),
    #[error("no matching count type for header in '{0}'")]
    BadHeader(PathBuf),
    #[error("no such direction '{0}'")]
    BadDirection(String),
    #[error("mismatch in count types between file location ('{0}') and header of that file")]
    LocationHeaderMisMatch(PathBuf),
    #[error("mismatch in number of directions between filename ('{0}') and data in that file")]
    DirectionLenMisMatch(PathBuf),
    #[error("cannot parse value as number")]
    ParseError(#[from] ParseIntError),
    #[error("no such vehicle class '{0}'")]
    BadVehicleClass(u8),
    #[error("unable to determine interval from count")]
    BadIntervalCount,
    #[error("error converting header row to string")]
    HeadertoStringRecordError(#[from] csv::Error),
    #[error("invalid MCD ({0})")]
    InvalidMcd(String),
    #[error("inconsistent data in database")]
    InconsistentData,
    // Errors from database specifically handled/custom error messages.
    #[error("{0}")]
    DbError(String),
    // Errors from database passed through transparently without specific handling.
    #[error("database error '{0}'")]
    OracleError(#[from] oracle::Error),
    #[error("{0}")]
    DataCheckError(String),
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

/// All of the kinds of counts.
///
/// These are all the types that are in both tc_header and tc_counttype tables.
/// tc_countype doesn't include Video, that's only in tc_header.
/// tc_header doesn't include EightDay or Loop, they're only in tc_counttype.
#[derive(Debug, PartialEq, Clone)]
pub enum CountKind {
    Bicycle1,
    Bicycle2,
    Bicycle3,
    Bicycle4,
    Bicycle5,
    Bicycle6,
    Pedestrian,
    Pedestrian2,
    Crosswalk,
    Volume,
    FifteenMinVolume,
    Class,
    ManualClass,
    Speed,
    EightDay,
    Loop,
    TurningMovement,
    Video,
}

impl FromStr for CountKind {
    type Err = CountError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Bicycle 1" => Ok(CountKind::Bicycle1),
            "Bicycle 2" => Ok(CountKind::Bicycle2),
            "Bicycle 3" => Ok(CountKind::Bicycle3),
            "Bicycle 4" => Ok(CountKind::Bicycle4),
            "Bicycle 5" => Ok(CountKind::Bicycle5),
            "Bicycle 6" => Ok(CountKind::Bicycle6),
            "Pedestrian" => Ok(CountKind::Pedestrian),
            "Pedestrian 2" => Ok(CountKind::Pedestrian2),
            "Crosswalk" => Ok(CountKind::Crosswalk),
            "Volume" => Ok(CountKind::Volume),
            "15 min Volume" => Ok(CountKind::FifteenMinVolume),
            "Class" => Ok(CountKind::Class),
            "Manual Class" => Ok(CountKind::ManualClass),
            "Speed" => Ok(CountKind::Speed),
            "8 Day" => Ok(CountKind::EightDay),
            "Loop" => Ok(CountKind::Loop),
            "Turning Movement" => Ok(CountKind::TurningMovement),
            "Video" => Ok(CountKind::Video),
            _ => Err(CountError::UnknownCountType(s.to_string())),
        }
    }
}

impl Display for CountKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CountKind::Bicycle1 => write!(f, "Bicycle 1"),
            CountKind::Bicycle2 => write!(f, "Bicycle 2"),
            CountKind::Bicycle3 => write!(f, "Bicycle 3"),
            CountKind::Bicycle4 => write!(f, "Bicycle 4"),
            CountKind::Bicycle5 => write!(f, "Bicycle 5"),
            CountKind::Bicycle6 => write!(f, "Bicycle 6"),
            CountKind::Pedestrian => write!(f, "Pedestrian"),
            CountKind::Pedestrian2 => write!(f, "Pedestrian 2"),
            CountKind::Crosswalk => write!(f, "Crosswalk"),
            CountKind::Volume => write!(f, "Volume"),
            CountKind::FifteenMinVolume => write!(f, "15 min Volume"),
            CountKind::Class => write!(f, "Class"),
            CountKind::ManualClass => write!(f, "Manual Class"),
            CountKind::Speed => write!(f, "Speed"),
            CountKind::EightDay => write!(f, "8 Day"),
            CountKind::Loop => write!(f, "Loop"),
            CountKind::TurningMovement => write!(f, "Turning Movement"),
            CountKind::Video => write!(f, "Video"),
        }
    }
}

/// An individual vehicle that has been counted, including
/// [vehicle classification](VehicleClass) and speed,
/// with no binning applied to it.
///
/// Three kinds of counts can be derived from this type of data:
///   - [TimeBinnedVehicleClassCount] by [create_speed_and_class_count]
///   - [TimeBinnedSpeedRangeCount] also by [create_speed_and_class_count]  
///   - [NonNormalAvgSpeedCount](denormalize::NonNormalAvgSpeedCount) by [denormalize::create_non_normal_speedavg_count]
#[derive(Debug, Clone)]
pub struct IndividualVehicle {
    pub date: NaiveDate,
    pub time: NaiveDateTime,
    pub lane: u8,
    pub class: VehicleClass,
    pub speed: f32,
}

impl GetDate for IndividualVehicle {
    fn get_date(&self) -> NaiveDate {
        self.date
    }
}

impl IndividualVehicle {
    pub fn new(
        date: NaiveDate,
        time: NaiveDateTime,
        lane: u8,
        class: u8,
        speed: f32,
    ) -> Result<Self, CountError> {
        let class = VehicleClass::from_num(class)?;
        Ok(Self {
            date,
            time,
            lane,
            class,
            speed,
        })
    }
}

/// Pre-binned, 15-minute bicycle volume counts.
#[derive(Debug, Clone, RowValue)]
pub struct FifteenMinuteBicycle {
    #[row_value(rename = "dvrpcnum")]
    pub recordnum: u32,
    #[row_value(rename = "countdate")]
    pub date: NaiveDate,
    #[row_value(rename = "counttime")]
    pub time: NaiveDateTime,
    pub total: u16,
    #[row_value(rename = "incount")]
    pub indir: Option<u16>,
    #[row_value(rename = "outcount")]
    pub outdir: Option<u16>,
}

impl GetDate for FifteenMinuteBicycle {
    fn get_date(&self) -> NaiveDate {
        self.date
    }
}

impl FifteenMinuteBicycle {
    pub fn new(
        recordnum: u32,
        date: NaiveDate,
        time: NaiveDateTime,
        total: u16,
        indir: Option<u16>,
        outdir: Option<u16>,
    ) -> Result<Self, CountError> {
        Ok(Self {
            recordnum,
            date,
            time,
            total,
            indir,
            outdir,
        })
    }
}

/// Pre-binned, 15-minute pedestrian volume counts.
#[derive(Debug, Clone, RowValue)]
pub struct FifteenMinutePedestrian {
    #[row_value(rename = "dvrpcnum")]
    pub recordnum: u32,
    #[row_value(rename = "countdate")]
    pub date: NaiveDate,
    #[row_value(rename = "counttime")]
    pub time: NaiveDateTime,
    pub total: u16,
    #[row_value(rename = "incount")]
    pub indir: Option<u16>,
    #[row_value(rename = "outcount")]
    pub outdir: Option<u16>,
}

impl GetDate for FifteenMinutePedestrian {
    fn get_date(&self) -> NaiveDate {
        self.date
    }
}

impl FifteenMinutePedestrian {
    pub fn new(
        recordnum: u32,
        date: NaiveDate,
        time: NaiveDateTime,
        total: u16,
        indir: Option<u16>,
        outdir: Option<u16>,
    ) -> Result<Self, CountError> {
        Ok(Self {
            recordnum,
            date,
            time,
            total,
            indir,
            outdir,
        })
    }
}

/// Pre-binned, 15-minute motor vehicle volume counts.
#[derive(Debug, Clone, RowValue)]
pub struct FifteenMinuteVehicle {
    pub recordnum: u32,
    #[row_value(rename = "countdate")]
    pub date: NaiveDate,
    #[row_value(rename = "counttime")]
    pub time: NaiveDateTime,
    #[row_value(rename = "volcount")]
    pub count: u16,
    #[row_value(rename = "cntdir")]
    pub direction: Option<LaneDirection>,
    #[row_value(rename = "countlane")]
    pub lane: Option<u8>,
}

impl GetDate for FifteenMinuteVehicle {
    fn get_date(&self) -> NaiveDate {
        self.date
    }
}

impl FifteenMinuteVehicle {
    pub fn new(
        recordnum: u32,
        date: NaiveDate,
        time: NaiveDateTime,
        count: u16,
        direction: Option<LaneDirection>,
        lane: Option<u8>,
    ) -> Result<Self, CountError> {
        Ok(Self {
            recordnum,
            date,
            time,
            count,
            direction,
            lane,
        })
    }
}

/// The full metadata of a count, which corresponds to the "tc_header" table in the database.
#[derive(Debug, Clone, PartialEq, RowValue)]
pub struct Metadata {
    pub amending: Option<String>,
    pub ampeak: Option<f32>,
    pub bikepeddesc: Option<String>,
    pub bikepedfacility: Option<String>,
    pub bikepedgroup: Option<String>,
    pub cntdir: Option<RoadDirection>,
    pub comments: Option<String>,
    #[row_value(rename = "type")]
    pub count_kind: Option<CountKind>,
    pub counterid: Option<u32>,
    pub createheaderdate: Option<NaiveDate>,
    pub datelastcounted: Option<NaiveDate>,
    pub description: Option<String>,
    pub fc: Option<u32>,
    pub fromlmt: Option<String>,
    pub importdatadate: Option<NaiveDate>,
    pub indir: Option<LaneDirection>,
    pub isurban: Option<String>,
    pub latitude: Option<f32>,
    pub longitude: Option<f32>,
    pub mcd: Option<String>,
    pub mp: Option<String>,
    pub offset: Option<String>,
    pub outdir: Option<LaneDirection>,
    pub pmending: Option<String>,
    pub pmpeak: Option<f32>,
    pub prj: Option<String>,
    pub program: Option<String>,
    pub recordnum: u32,
    pub rdprefix: Option<String>,
    pub rdsuffix: Option<String>,
    pub road: Option<String>,
    pub route: Option<u32>,
    pub seg: Option<String>,
    pub sidewalk: Option<String>,
    pub speedlimit: Option<u8>,
    pub source: Option<String>,
    pub sr: Option<String>,
    pub sri: Option<String>,
    pub stationid: Option<String>,
    #[row_value(rename = "takenby")]
    pub technician: Option<String>,
    pub tolmt: Option<String>,
    pub trafdir: Option<RoadDirection>,
    pub x: Option<f32>,
    pub y: Option<f32>,
}

/// The field metadata of an input count, which is a subset of the full [`Metadata`] and includes
/// technician, id, direction(s), count machine id, and - potentially - the speed limit.
///
/// See the [import](../import/index.html) program for filename specification.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldMetadata {
    pub technician: String, // initials
    pub recordnum: u32,
    pub directions: Directions,
    pub counter_id: u32,
    pub speed_limit: Option<u8>,
}

impl FieldMetadata {
    /// Get an input count's metadata from its path.
    pub fn from_path(path: &Path) -> Result<Self, CountError> {
        let parts: Vec<&str> = path
            .file_stem()
            .ok_or(CountError::BadPath(path.to_owned()))?
            .to_str()
            .ok_or(CountError::BadPath(path.to_owned()))?
            .split('-')
            .collect();

        if parts.len() < 5 {
            return Err(CountError::InvalidFileName {
                problem: FileNameProblem::TooFewParts,
                path: path.to_owned(),
            });
        }
        if parts.len() > 5 {
            return Err(CountError::InvalidFileName {
                problem: FileNameProblem::TooManyParts,
                path: path.to_owned(),
            });
        }

        // `technician` should be letters. If parseable as int, then they aren't letters.
        if parts[0].parse::<u32>().is_ok() {
            return Err(CountError::InvalidFileName {
                problem: FileNameProblem::InvalidTech,
                path: path.to_owned(),
            });
        }

        let technician = parts[0].to_string();

        let recordnum = match parts[1].parse() {
            Ok(v) => v,
            Err(_) => {
                return Err(CountError::InvalidFileName {
                    problem: FileNameProblem::InvalidRecordNum,
                    path: path.to_owned(),
                })
            }
        };

        let directions: Directions = match parts[2] {
            "nnn" => Directions::new(
                LaneDirection::North,
                Some(LaneDirection::North),
                Some(LaneDirection::North),
            ),
            "sss" => Directions::new(
                LaneDirection::South,
                Some(LaneDirection::South),
                Some(LaneDirection::South),
            ),
            "eee" => Directions::new(
                LaneDirection::East,
                Some(LaneDirection::East),
                Some(LaneDirection::East),
            ),
            "www" => Directions::new(
                LaneDirection::West,
                Some(LaneDirection::West),
                Some(LaneDirection::West),
            ),
            "ns" => Directions::new(LaneDirection::North, Some(LaneDirection::South), None),
            "sn" => Directions::new(LaneDirection::South, Some(LaneDirection::North), None),
            "ew" => Directions::new(LaneDirection::East, Some(LaneDirection::West), None),
            "we" => Directions::new(LaneDirection::West, Some(LaneDirection::East), None),
            "nn" => Directions::new(LaneDirection::North, Some(LaneDirection::North), None),
            "ss" => Directions::new(LaneDirection::South, Some(LaneDirection::South), None),
            "ee" => Directions::new(LaneDirection::East, Some(LaneDirection::East), None),
            "ww" => Directions::new(LaneDirection::West, Some(LaneDirection::West), None),
            "n" => Directions::new(LaneDirection::North, None, None),
            "s" => Directions::new(LaneDirection::South, None, None),
            "e" => Directions::new(LaneDirection::East, None, None),
            "w" => Directions::new(LaneDirection::West, None, None),
            _ => {
                return Err(CountError::InvalidFileName {
                    problem: FileNameProblem::InvalidDirections,
                    path: path.to_owned(),
                })
            }
        };

        let counter_id = match parts[3].parse() {
            Ok(v) => v,
            Err(_) => {
                return Err(CountError::InvalidFileName {
                    problem: FileNameProblem::InvalidCounterID,
                    path: path.to_owned(),
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
                        path: path.to_owned(),
                    })
                }
            }
        };

        let metadata = Self {
            technician,
            recordnum,
            directions,
            counter_id,
            speed_limit,
        };

        Ok(metadata)
    }
}

/// The direction of a road.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum RoadDirection {
    North,
    East,
    South,
    West,
    Both,
}

impl FromStr for RoadDirection {
    type Err = CountError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "north" | "n" => Ok(RoadDirection::North),
            "east" | "e" => Ok(RoadDirection::East),
            "south" | "s" => Ok(RoadDirection::South),
            "west" | "w" => Ok(RoadDirection::West),
            "both" | "b" => Ok(RoadDirection::Both),
            _ => Err(CountError::BadDirection(s.to_string())),
        }
    }
}

impl Display for RoadDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let dir = match self {
            RoadDirection::North => "north".to_string(),
            RoadDirection::East => "east".to_string(),
            RoadDirection::South => "south".to_string(),
            RoadDirection::West => "west".to_string(),
            RoadDirection::Both => "both".to_string(),
        };
        write!(f, "{}", dir)
    }
}

/// The direction of a lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum LaneDirection {
    North,
    East,
    South,
    West,
}

impl FromStr for LaneDirection {
    type Err = CountError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "north" | "n" => Ok(LaneDirection::North),
            "east" | "e" => Ok(LaneDirection::East),
            "south" | "s" => Ok(LaneDirection::South),
            "west" | "w" => Ok(LaneDirection::West),
            _ => Err(CountError::BadDirection(s.to_string())),
        }
    }
}

impl Display for LaneDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let dir = match self {
            LaneDirection::North => "north".to_string(),
            LaneDirection::East => "east".to_string(),
            LaneDirection::South => "south".to_string(),
            LaneDirection::West => "west".to_string(),
        };
        write!(f, "{}", dir)
    }
}

/// The [`LaneDirection`]s that a count could contain.
#[derive(Debug, Clone, PartialEq)]
pub struct Directions {
    pub direction1: LaneDirection,
    pub direction2: Option<LaneDirection>,
    pub direction3: Option<LaneDirection>,
}

impl Directions {
    pub fn new(
        direction1: LaneDirection,
        direction2: Option<LaneDirection>,
        direction3: Option<LaneDirection>,
    ) -> Self {
        Self {
            direction1,
            direction2,
            direction3,
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
    pub fn from_num(num: u8) -> Result<Self, CountError> {
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

/// Count of [vehicles by class][`VehicleClass`], binned into 15-minute or hourly intervals.
///
/// We almost always want fifteen-minute counts, but hourly is also an option.
#[derive(Debug, Clone, RowValue)]
pub struct TimeBinnedVehicleClassCount {
    #[row_value(rename = "countdate")]
    pub date: NaiveDate,
    #[row_value(rename = "counttime")]
    pub time: NaiveDateTime,
    #[row_value(rename = "countlane")]
    pub lane: Option<u8>,
    pub recordnum: u32,
    #[row_value(rename = "ctdir")]
    pub direction: Option<LaneDirection>,
    #[row_value(rename = "bikes")]
    pub c1: u32,
    #[row_value(rename = "cars_and_tlrs")]
    pub c2: u32,
    #[row_value(rename = "ax2_long")]
    pub c3: u32,
    #[row_value(rename = "buses")]
    pub c4: u32,
    #[row_value(rename = "ax2_6_tire")]
    pub c5: u32,
    #[row_value(rename = "ax3_single")]
    pub c6: u32,
    #[row_value(rename = "ax4_single")]
    pub c7: u32,
    #[row_value(rename = "lt_5_ax_double")]
    pub c8: u32,
    #[row_value(rename = "ax5_double")]
    pub c9: u32,
    #[row_value(rename = "gt_5_ax_double")]
    pub c10: u32,
    #[row_value(rename = "lt_6_ax_multi")]
    pub c11: u32,
    #[row_value(rename = "ax6_multi")]
    pub c12: u32,
    #[row_value(rename = "gt_6_ax_multi")]
    pub c13: u32,
    #[row_value(rename = "unclassified")]
    pub c15: Option<u32>,
    pub total: u32,
}

/// Count of vehicles by speed range, binned into 15-minute or hourly intervals.
///
/// We almost always want fifteen-minute counts, but hourly is also an option.
#[derive(Debug, Clone, RowValue)]
pub struct TimeBinnedSpeedRangeCount {
    #[row_value(rename = "countdate")]
    pub date: NaiveDate,
    #[row_value(rename = "counttime")]
    pub time: NaiveDateTime,
    #[row_value(rename = "countlane")]
    pub lane: Option<u8>,
    pub recordnum: u32,
    #[row_value(rename = "ctdir")]
    pub direction: Option<LaneDirection>,
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

/// Create time-binned speed and class counts from [`IndividualVehicle`]s.
pub fn create_speed_and_class_count(
    metadata: FieldMetadata,
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

        // Create a key for the Hashmap for time intervals
        let time_part = bin_time(count.time.time(), interval);
        let key = BinnedCountKey {
            date: count.date,
            time: NaiveDateTime::new(count.date, time_part),
            lane: count.lane,
        };

        // Add new entry to 15-min speed range map or increment existing one.
        speed_range_map
            .entry(key)
            .and_modify(|c| c.insert(count.speed))
            .or_insert(SpeedRangeCount::first(
                metadata.recordnum,
                direction,
                count.speed,
            ));

        // Add new entry to 15-min vehicle class map or increment existing one.
        vehicle_class_map
            .entry(key)
            .and_modify(|c| c.insert(count.class.clone()))
            .or_insert(VehicleClassCount::first(
                metadata.recordnum,
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
    counts.sort_unstable_by_key(|c| (c.date, c.time.time()));

    let first_dt = NaiveDateTime::new(
        counts.first().unwrap().date,
        counts.first().unwrap().time.time(),
    );
    let last_dt = NaiveDateTime::new(
        counts.last().unwrap().date,
        counts.last().unwrap().time.time(),
    );

    let all_datetimes = create_time_bins(first_dt, last_dt, interval);

    if all_datetimes.len() < speed_range_map.len() {
        let mut all_keys = vec![];
        let all_lanes = if metadata.directions.direction3.is_some() {
            vec![1, 2, 3]
        } else if metadata.directions.direction3.is_none()
            && metadata.directions.direction2.is_some()
        {
            vec![1, 2]
        } else {
            vec![1]
        };

        // construct all possible keys
        for datetime in all_datetimes.clone() {
            for lane in all_lanes.iter() {
                all_keys.push(BinnedCountKey {
                    date: datetime.date(),
                    time: datetime,
                    lane: *lane,
                })
            }
        }
        // Add missing periods for speed range count
        for key in all_keys {
            let direction = match key.lane {
                1 => metadata.directions.direction1,
                2 => metadata.directions.direction2.unwrap(),
                3 => metadata.directions.direction3.unwrap(),
                _ => {
                    error!("Unable to determine lane/direction.");
                    continue;
                }
            };
            speed_range_map
                .entry(key)
                .or_insert(SpeedRangeCount::new(metadata.recordnum, direction));
            vehicle_class_map
                .entry(key)
                .or_insert(VehicleClassCount::new(metadata.recordnum, direction));
        }
    }

    // Convert speed range count from HashMap to Vec.
    let mut speed_range_count = vec![];
    for (key, value) in speed_range_map {
        speed_range_count.push(TimeBinnedSpeedRangeCount {
            date: key.date,
            time: key.time,
            lane: Some(key.lane),
            recordnum: value.recordnum,
            direction: Some(value.direction),
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
            date: key.date,
            time: key.time,
            lane: Some(key.lane),
            recordnum: value.recordnum,
            direction: Some(value.direction),
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
            c15: Some(value.c15),
            total: value.total,
        });
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

/// Time interval to bin data by.
#[derive(Clone, Copy)]
pub enum TimeInterval {
    Hour,
    FifteenMin,
}

/// Bin time by fifteen-minute or hourly intervals by changing the minute.
pub fn bin_time(time: NaiveTime, interval: TimeInterval) -> NaiveTime {
    let time = time.with_second(0).unwrap();

    match interval {
        TimeInterval::Hour => time.with_minute(0).unwrap(),
        TimeInterval::FifteenMin => {
            match time.minute() {
                0..=14 => time.with_minute(0).unwrap(),
                15..=29 => time.with_minute(15).unwrap(),
                30..=44 => time.with_minute(30).unwrap(),
                _ => time.with_minute(45).unwrap(), // minute is always 0-59, so this is 45-59
            }
        }
    }
}

/// Create all intervals between (and including) a first and last datetime.
pub fn create_time_bins(
    first_dt: NaiveDateTime,
    last_dt: NaiveDateTime,
    interval: TimeInterval,
) -> Vec<NaiveDateTime> {
    let first_bin = NaiveDateTime::new(first_dt.date(), bin_time(first_dt.time(), interval));

    let last_bin = NaiveDateTime::new(last_dt.date(), bin_time(last_dt.time(), interval));

    let mut dts: Vec<NaiveDateTime> = vec![];

    let mut current_bin = first_bin;

    let time_to_add = match interval {
        TimeInterval::Hour => TimeDelta::hours(1),
        TimeInterval::FifteenMin => TimeDelta::minutes(15),
    };

    while current_bin <= last_bin {
        dts.push(current_bin);
        current_bin = current_bin.checked_add_signed(time_to_add).unwrap();
    }
    dts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_binning_fifteen_min_is_correct() {
        // 1st 15-minute bin
        let time = NaiveTime::from_hms_opt(10, 0, 0).unwrap();

        let binned = bin_time(time, TimeInterval::FifteenMin);
        assert_eq!(binned, NaiveTime::from_hms_opt(10, 0, 0).unwrap());
        assert_ne!(binned, NaiveTime::from_hms_opt(10, 10, 0).unwrap());

        let time = NaiveTime::from_hms_opt(10, 14, 0).unwrap();
        let binned = bin_time(time, TimeInterval::FifteenMin);
        assert_eq!(binned, NaiveTime::from_hms_opt(10, 0, 0).unwrap());

        // 2nd 15-minute bin
        let time = NaiveTime::from_hms_opt(10, 25, 0).unwrap();

        let binned = bin_time(time, TimeInterval::FifteenMin);
        assert_eq!(binned, NaiveTime::from_hms_opt(10, 15, 0).unwrap());

        let time = NaiveTime::from_hms_opt(10, 29, 0).unwrap();

        let binned = bin_time(time, TimeInterval::FifteenMin);
        assert_eq!(binned, NaiveTime::from_hms_opt(10, 15, 0).unwrap());

        // 3rd 15-minute bin
        let time = NaiveTime::from_hms_opt(10, 31, 0).unwrap();

        let binned = bin_time(time, TimeInterval::FifteenMin);
        assert_eq!(binned, NaiveTime::from_hms_opt(10, 30, 0).unwrap());

        let time = NaiveTime::from_hms_opt(10, 44, 0).unwrap();

        let binned = bin_time(time, TimeInterval::FifteenMin);
        assert_eq!(binned, NaiveTime::from_hms_opt(10, 30, 0).unwrap());

        // 4th 15-minute bin
        let time = NaiveTime::from_hms_opt(10, 45, 0).unwrap();

        let binned = bin_time(time, TimeInterval::FifteenMin);
        assert_eq!(binned, NaiveTime::from_hms_opt(10, 45, 0).unwrap());

        let time = NaiveTime::from_hms_opt(10, 59, 0).unwrap();

        let binned = bin_time(time, TimeInterval::FifteenMin);
        assert_eq!(binned, NaiveTime::from_hms_opt(10, 45, 0).unwrap());
    }

    #[test]
    fn time_binning_hourly_is_correct() {
        // the time we are trying to bin to
        let expected = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        // the interval to use
        let interval = TimeInterval::Hour;

        assert_eq!(
            bin_time(NaiveTime::from_hms_opt(10, 0, 0).unwrap(), interval),
            expected
        );
        assert_eq!(
            bin_time(NaiveTime::from_hms_opt(10, 15, 0).unwrap(), interval),
            expected
        );
        assert_eq!(
            bin_time(NaiveTime::from_hms_opt(10, 16, 0).unwrap(), interval),
            expected
        );
        assert_eq!(
            bin_time(NaiveTime::from_hms_opt(10, 31, 0).unwrap(), interval),
            expected
        );
        assert_eq!(
            bin_time(NaiveTime::from_hms_opt(10, 59, 0).unwrap(), interval),
            expected
        );
    }

    #[test]
    fn create_time_bins_correct() {
        let first_dt = NaiveDateTime::parse_from_str("2024-04-08 7:00", "%Y-%m-%d %-H:%M").unwrap();
        let last_dt = NaiveDateTime::parse_from_str("2024-04-08 7:14", "%Y-%m-%d %-H:%M").unwrap();
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 1);
        assert_eq!(keys_hour.len(), 1);

        let first_dt = NaiveDateTime::parse_from_str("2024-04-08 7:00", "%Y-%m-%d %-H:%M").unwrap();
        let last_dt = NaiveDateTime::parse_from_str("2024-04-08 7:15", "%Y-%m-%d %-H:%M").unwrap();
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 2);
        assert_eq!(keys_hour.len(), 1);

        let first_dt = NaiveDateTime::parse_from_str("2024-04-08 7:00", "%Y-%m-%d %-H:%M").unwrap();
        let last_dt = NaiveDateTime::parse_from_str("2024-04-08 7:59", "%Y-%m-%d %-H:%M").unwrap();
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 4);
        assert_eq!(keys_hour.len(), 1);

        let first_dt = NaiveDateTime::parse_from_str("2024-04-08 7:00", "%Y-%m-%d %-H:%M").unwrap();
        let last_dt = NaiveDateTime::parse_from_str("2024-04-08 8:59", "%Y-%m-%d %-H:%M").unwrap();
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 8);
        assert_eq!(keys_hour.len(), 2);

        let first_dt = NaiveDateTime::parse_from_str("2024-04-08 0:00", "%Y-%m-%d %-H:%M").unwrap();
        let last_dt = NaiveDateTime::parse_from_str("2024-04-08 23:59", "%Y-%m-%d %H:%M").unwrap();
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 96);
        assert_eq!(keys_hour.len(), 24);

        let first_dt = NaiveDateTime::parse_from_str("2024-04-08 0:00", "%Y-%m-%d %-H:%M").unwrap();
        let last_dt = NaiveDateTime::parse_from_str("2024-04-09 0:00", "%Y-%m-%d %-H:%M").unwrap();
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 97);
        assert_eq!(keys_hour.len(), 25);

        // spanning two months
        let first_dt = NaiveDateTime::parse_from_str("2024-03-31 23:00", "%Y-%m-%d %H:%M").unwrap();
        let last_dt = NaiveDateTime::parse_from_str("2024-04-01 00:01", "%Y-%m-%d %H:%M").unwrap();
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 5);
        assert_eq!(keys_hour.len(), 2);

        // spanning two years
        let first_dt = NaiveDateTime::parse_from_str("2023-12-31 23:00", "%Y-%m-%d %H:%M").unwrap();
        let last_dt = NaiveDateTime::parse_from_str("2024-01-01 00:01", "%Y-%m-%d %H:%M").unwrap();
        let keys_15 = create_time_bins(first_dt, last_dt, TimeInterval::FifteenMin);
        let keys_hour = create_time_bins(first_dt, last_dt, TimeInterval::Hour);
        assert_eq!(keys_15.len(), 5);
        assert_eq!(keys_hour.len(), 2);
    }
}

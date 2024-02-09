//! See <https://www.dvrpc.org/traffic/> for additional information about traffic counting.
//! Naming convention in this program:
//!  - "CountedVehicle" is the individual vehicle that got counted
//!    "VehicleCount" is the overall count spanning multiple days

use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use csv::{Reader, ReaderBuilder};
use log::{error, info, LevelFilter};
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};
use thiserror::Error;
use time::{macros::format_description, Date, PrimitiveDateTime, Time};

const INDIVIDUAL_VEHICLE_COUNT_HEADER: &str = "Veh. No.,Date,Time,Channel,Class,Speed";
const FIFTEEN_MINUTE_VEHICLE_COUNT_HEADER1: &str = "Number,Date,Time,Channel 1";
const FIFTEEN_MINUTE_VEHICLE_COUNT_HEADER2: &str = "Number,Date,Time,Channel 1,Channel 2";
const FIFTEEN_MINUTE_BICYCLE_COUNT_HEADER: &str = "";
const FIFTEEN_MINUTE_PEDESTRIAN_COUNT_HEADER: &str = "";

const LOG: &str = "log.txt";

#[derive(Debug, Error)]
enum CountError<'a> {
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
    #[error("invalid speed '{0}'")]
    InvalidSpeed(f32),
    #[error("cannot locate header in '{0}'")]
    MissingHeader(&'a Path),
    #[error("error converting header row to string")]
    HeadertoStringRecordError(#[from] csv::Error),
}

#[derive(Debug)]
enum FileNameProblem {
    TooManyParts,
    TooFewParts,
    InvalidTech,
    InvalidRecordNum,
    InvalidDirections,
    InvalidCounterID,
    InvalidSpeedLimit,
}
trait Extract {
    type Item;

    fn extract(path: &Path) -> Result<Vec<Self::Item>, CountError>;
}

/* Names of the 15 classifications from the FWA. See:
 * <https://www.fhwa.dot.gov/policyinformation/vehclass.cfm>
 * <https://www.fhwa.dot.gov/policyinformation/tmguide/tmg_2013/vehicle-types.cfm>
 * <https://www.fhwa.dot.gov/publications/research/infrastructure/pavements/ltpp/13091/002.cfm>
*/
#[derive(Debug, Clone)]
enum VehicleClass {
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
    fn from_num(num: u8) -> Result<Self, CountError<'static>> {
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

#[derive(Debug, Clone, Copy, PartialEq)]
enum CountType {
    FifteenMinuteBicycle,    // Eco-Counter
    FifteenMinutePedestrian, // Eco-Counter
    FifteenMinuteVehicle,    // 15-min binned data for the simple volume counts from StarNext/Jamar
    IndividualVehicle,       // Individual vehicles from StarNext/Jamar prior to any binning
}

impl CountType {
    fn from_location(path: &Path) -> Result<CountType, CountError> {
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

        match parent.to_lowercase().as_str() {
            "15minutebicycle" => Ok(CountType::FifteenMinuteBicycle),
            "15minutepedestrian" => Ok(CountType::FifteenMinutePedestrian),
            "15minutevehicle" => Ok(CountType::FifteenMinuteVehicle),
            "vehicle" => Ok(CountType::IndividualVehicle),
            _ => Err(CountError::BadLocation(format!("{path:?}"))),
        }
    }

    fn from_header(path: &Path, location_count_type: CountType) -> Result<CountType, CountError> {
        // `location_count_type` is what we expect this file to be, based off its location. We use
        // this because different types of counts can have a variable number of metadata rows.

        let file = File::open(path)?;
        let mut rdr = create_reader(&file);
        let header = rdr
            .records()
            .skip(num_metadata_rows_to_skip(location_count_type))
            .take(1)
            .last()
            .ok_or(CountError::MissingHeader(path))?
            .map_err(CountError::HeadertoStringRecordError)?
            .iter()
            .map(|x| x.trim().to_string())
            .collect::<Vec<String>>()
            .join(",");

        match header.as_str() {
            v if v == INDIVIDUAL_VEHICLE_COUNT_HEADER => Ok(CountType::IndividualVehicle),
            v if v == FIFTEEN_MINUTE_VEHICLE_COUNT_HEADER1 => Ok(CountType::FifteenMinuteVehicle),
            v if v == FIFTEEN_MINUTE_VEHICLE_COUNT_HEADER2 => Ok(CountType::FifteenMinuteVehicle),
            _ => Err(CountError::BadHeader(path)),
        }
    }
}

// CountedVehicle - the raw, unbinned data
#[derive(Debug, Clone)]
struct CountedVehicle {
    date: Date,
    time: Time,
    channel: u8,
    class: VehicleClass,
    speed: f32,
}

impl CountedVehicle {
    fn new(
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

/// Extract CountedVehicle records from a file.
impl Extract for CountedVehicle {
    type Item = CountedVehicle;

    fn extract(path: &Path) -> Result<Vec<Self::Item>, CountError> {
        let data_file = File::open(path)?;
        let mut rdr = create_reader(&data_file);

        // Iterate through data rows (skipping metadata rows + 1 for header).
        let mut counts = vec![];
        for row in rdr
            .records()
            .skip(num_metadata_rows_to_skip(CountType::IndividualVehicle) + 1)
        {
            // Parse date.
            let date_format = format_description!("[month padding:none]/[day padding:none]/[year]");
            let date_col = &row.as_ref().unwrap()[1];
            let count_date = Date::parse(date_col, &date_format).unwrap();

            // Parse time.
            let time_format =
                format_description!("[hour padding:none repr:12]:[minute]:[second] [period]");
            let time_col = &row.as_ref().unwrap()[2];
            let count_time = Time::parse(time_col, &time_format).unwrap();

            let count = match CountedVehicle::new(
                count_date,
                count_time,
                row.as_ref().unwrap()[3].parse().unwrap(),
                row.as_ref().unwrap()[4].parse().unwrap(),
                row.as_ref().unwrap()[5].parse().unwrap(),
            ) {
                Ok(v) => v,
                Err(e) => {
                    error!("{e}");
                    continue;
                }
            };

            counts.push(count);
        }
        Ok(counts)
    }
}

///  FifteenMinuteVehicle - pre-binned, simple volume counts in 15-minute intervals.
#[derive(Debug, Clone)]
struct FifteenMinuteVehicle {
    date: Date,
    time: Time,
    count: u8,
    direction: Direction,
}

impl FifteenMinuteVehicle {
    fn new(
        date: Date,
        time: Time,
        count: u8,
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

/// Extract FifteenMinuteVehicle records from a file.
impl Extract for FifteenMinuteVehicle {
    type Item = FifteenMinuteVehicle;

    fn extract(path: &Path) -> Result<Vec<Self::Item>, CountError> {
        let data_file = File::open(path)?;
        let mut rdr = create_reader(&data_file);
        let directions = CountMetadata::new(path)?.directions;

        // Iterate through data rows (skipping metadata rows + 1 for header).
        let mut counts = vec![];
        for row in rdr
            .records()
            .skip(num_metadata_rows_to_skip(CountType::FifteenMinuteVehicle) + 1)
        {
            // Parse date.
            let date_format = format_description!("[month padding:none]/[day padding:none]/[year]");
            let date_col = &row.as_ref().unwrap()[1];
            let count_date = Date::parse(date_col, &date_format).unwrap();

            // Parse time.
            let time_format = format_description!("[hour padding:none repr:12]:[minute] [period]");
            let time_col = &row.as_ref().unwrap()[2];
            let count_time = Time::parse(time_col, &time_format).unwrap();

            // There will always be at least one count per row.
            // Extract the first (and perhaps only) direction.
            match FifteenMinuteVehicle::new(
                count_date,
                count_time,
                row.as_ref().unwrap()[3].parse().unwrap(),
                directions.direction1,
            ) {
                Ok(v) => counts.push(v),
                Err(e) => {
                    error!("{e}");
                    continue;
                }
            };

            // There may also be a second count within the row.
            if let Some(v) = directions.direction2 {
                match FifteenMinuteVehicle::new(
                    count_date,
                    count_time,
                    row.as_ref().unwrap()[4].parse().unwrap(),
                    v,
                ) {
                    Ok(v) => counts.push(v),
                    Err(e) => {
                        error!("{e}");
                        continue;
                    }
                };
            }
        }
        Ok(counts)
    }
}

/// A count's metadata is contained within its filename. Each part is separate by a dash (-).
/// technician-dvrpc_num-directions-counter_id-speed_limit.csv/txt
/// e.g. rc-166905-ew-40972-35.txt
#[derive(Debug, Clone, PartialEq)]
struct CountMetadata {
    technician: String, // initials
    dvrpc_num: i32,
    directions: Directions,
    counter_id: i32,
    speed_limit: Option<i32>,
    // start_datetime: PrimitiveDateTime,
    // site_code: usize,
    // station_id: Option<usize>,
}

impl CountMetadata {
    fn new(path: &Path) -> Result<Self, CountError> {
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

#[derive(Debug, Clone, Copy, PartialEq)]
enum Direction {
    North,
    East,
    South,
    West,
}

#[derive(Debug, Clone, PartialEq)]
struct Directions {
    direction1: Direction,
    direction2: Option<Direction>,
}

impl Directions {
    fn new(direction1: Direction, direction2: Option<Direction>) -> Self {
        Self {
            direction1,
            direction2,
        }
    }
}

/// Represents a row in TC_CLACOUNT table
type FifteenMinuteVehicleClassCount = HashMap<BinnedCountKey, VehicleClassCount>;
/// Represents a row in TC_SPECOUNT table
type FifteenMinuteSpeedRangeCount = HashMap<BinnedCountKey, SpeedRangeCount>;

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
struct BinnedCountKey {
    datetime: PrimitiveDateTime,
    channel: u8,
}

/// Count of vehicles by vehicle class in some non-specific time period.
#[derive(Debug, Clone, Copy)]
struct VehicleClassCount {
    dvrpc_num: i32,
    direction: Direction,
    c1: i32,
    c2: i32,
    c3: i32,
    c4: i32,
    c5: i32,
    c6: i32,
    c7: i32,
    c8: i32,
    c9: i32,
    c10: i32,
    c11: i32,
    c12: i32,
    c13: i32,
    c15: i32,
    total: i32,
}

impl VehicleClassCount {
    fn new(dvrpc_num: i32, direction: Direction) -> Self {
        Self {
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
            total: 0,
        }
    }
    fn insert(&mut self, class: VehicleClass) -> &Self {
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
        self
    }
}

/// Count of vehicles by speed range in some non-specific time period.
#[derive(Debug, Clone, Copy)]
struct SpeedRangeCount {
    dvrpc_num: i32,
    direction: Direction,
    s1: i32,
    s2: i32,
    s3: i32,
    s4: i32,
    s5: i32,
    s6: i32,
    s7: i32,
    s8: i32,
    s9: i32,
    s10: i32,
    s11: i32,
    s12: i32,
    s13: i32,
    s14: i32,
    total: i32,
}

impl SpeedRangeCount {
    // Create a SpeedRangeCount with 0 count for all speed ranges.
    fn new(dvrpc_num: i32, direction: Direction) -> Self {
        Self {
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
        }
    }
    fn insert(&mut self, speed: f32) -> Result<&Self, CountError> {
        if speed.is_sign_negative() {
            return Err(CountError::InvalidSpeed(speed));
        }

        // The end of the ranges are inclusive to the number's .0 decimal;
        // that is:
        // 0-15: 0.0 to 15.0
        // >15-20: 15.1 to 20.0, etc.

        // Unfortunately, using floats as tests in pattern matching will be an error in a future
        // Rust release, so need to do if/else rather than match.
        // <https://github.com/rust-lang/rust/issues/41620>

        if (0.0..=15.0).contains(&speed) {
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
        } else {
            return Err(CountError::InvalidSpeed(speed));
        }

        self.total += 1;
        Ok(self)
    }
}

fn main() {
    // Load file containing environment variables, panic if it doesn't exist.
    dotenvy::dotenv().expect("Unable to load .env file.");

    // Get env var for path where CSVs will be, panic if it doesn't exist.
    let data_dir =
        env::var("DATA_DIR").expect("Unable to load data directory path from .env file.");

    // Set up logging, panic if it fails.
    let config = ConfigBuilder::new().set_time_format_rfc3339().build();
    CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Debug,
            config.clone(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        ),
        WriteLogger::new(
            LevelFilter::Info,
            config,
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(format!("{data_dir}/{LOG}"))
                .expect("Could not open log file."),
        ),
    ])
    .expect("Could not configure logging.");

    // The database env vars aren't needed for a while, but if they aren't available, return
    // early before doing any work.
    match env::var("DB_USERNAME") {
        Ok(v) => v,
        Err(e) => {
            error!("Unable to load username from .env file: {e}.");
            return;
        }
    };
    match env::var("DB_PASSWORD") {
        Ok(v) => v,
        Err(e) => {
            error!("Unable to load password from .env file: {e}.");
            return;
        }
    };

    // Get all the paths of the files that need to be processed.
    let mut paths = vec![];
    let paths = match collect_paths(data_dir.into(), &mut paths) {
        Ok(v) => v,
        Err(e) => {
            error!("{e}");
            return;
        }
    };

    // Iterate through all paths, extacting the data from the files, transforming it into the
    // desired shape, and loading it into the database.
    // Exactly how the data is processed depends on what `CountType` it is.
    for path in paths {
        match process_count(path) {
            Ok(()) => (),
            // If there's an error, log it and continue to next file.
            Err(e) => {
                error!("{path:?} not processed: {e}");
                continue;
            }
        }
    }
}

/// Collect all the file paths to extract data from.
fn collect_paths(dir: PathBuf, paths: &mut Vec<PathBuf>) -> io::Result<&mut Vec<PathBuf>> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();

        if path.is_dir() {
            collect_paths(path, paths)?;
        } else if let Some(v) = path.file_name() {
            if v != LOG {
                paths.push(path)
            }
        }
    }
    Ok(paths)
}

fn determine_count_type(path: &Path) -> Result<CountType, CountError> {
    let count_type_from_location = CountType::from_location(path)?;
    let count_type_from_header = CountType::from_header(path, count_type_from_location)?;
    if count_type_from_location != count_type_from_header {
        return Err(CountError::LocationHeaderMisMatch(path));
    }
    Ok(count_type_from_location)
}

/// Process count - extract from file, transform, load into database.
fn process_count(path: &Path) -> Result<(), CountError> {
    // Get count type and metatadata from file; create CSV reader over it.
    let count_type = determine_count_type(path)?;
    // Process the file according to CountType
    info!("Extracting data from {path:?}, a {count_type:?} count.");
    match count_type {
        CountType::IndividualVehicle => {
            // Extract data from CSV/text file
            let counted_vehicles = CountedVehicle::extract(path)?;

            let metadata = CountMetadata::new(path)?;
            // Create two counts from this: 15-minute speed count and 15-minute class count
            let (speed_range_count, vehicle_class_count) =
                create_speed_and_class_count(metadata, counted_vehicles);

            // TODO: enter these into the database
        }
        CountType::FifteenMinuteVehicle => {
            let fifteen_min_volcount = FifteenMinuteVehicle::extract(path)?;

            // As they are already binned by 15-minute period, these need to further processing.
            // TODO: enter into database.
        }
        CountType::FifteenMinuteBicycle => (),
        CountType::FifteenMinutePedestrian => (),
    }
    Ok(())
}

fn create_speed_and_class_count(
    metadata: CountMetadata,
    counts: Vec<CountedVehicle>,
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

        // Add new entry to 15-min speed range count if necessary, then insert count's speed.
        let speed_range_count = fifteen_min_speed_range_count
            .entry(key)
            .or_insert(SpeedRangeCount::new(metadata.dvrpc_num, direction));
        *speed_range_count = match speed_range_count.insert(count.speed) {
            Ok(v) => *v,
            Err(e) => {
                error!("{e}");
                continue;
            }
        };

        // Add new entry to 15-min vehicle class count if necessary, then insert count's class.
        let vehicle_class_count = fifteen_min_vehicle_class_count
            .entry(key)
            .or_insert(VehicleClassCount::new(metadata.dvrpc_num, direction));
        *vehicle_class_count = *vehicle_class_count.insert(count.class);
    }

    (
        fifteen_min_speed_range_count,
        fifteen_min_vehicle_class_count,
    )
}

/// Put time into four bins per hour.
fn time_bin(time: Time) -> Result<Time, time::error::ComponentRange> {
    let time = time.replace_second(0)?;
    match time.minute() {
        0..=14 => Ok(time.replace_minute(0)?),
        15..=29 => Ok(time.replace_minute(15)?),
        30..=44 => Ok(time.replace_minute(30)?),
        _ => Ok(time.replace_minute(45)?), // minute is always 0-59, so this is 45-59
    }
}

/// Create CSV reader from file.
fn create_reader(file: &File) -> Reader<&File> {
    ReaderBuilder::new()
        .has_headers(false)
        .trim(csv::Trim::All)
        .flexible(true)
        .from_reader(file)
}

fn num_metadata_rows_to_skip(count_type: CountType) -> usize {
    match count_type {
        CountType::IndividualVehicle => 3,
        CountType::FifteenMinuteVehicle => 4,
        _ => 8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_type_vehicle_ok() {
        let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
        let ct_from_location = CountType::from_location(path).unwrap();
        let ct_from_header = CountType::from_header(path, ct_from_location).unwrap();
        assert_eq!(&ct_from_location, &ct_from_header);
        assert_eq!(ct_from_location, CountType::IndividualVehicle);
    }

    #[test]
    fn extract_counts_gets_correct_number_of_counts() {
        let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
        let counted_vehicles = CountedVehicle::extract(path).unwrap();
        assert_eq!(counted_vehicles.len(), 8706);
    }

    #[test]
    fn vehicle_class_from_bad_num_errs() {
        assert!(VehicleClass::from_num(15).is_err());
    }

    #[test]
    fn vehicle_class_from_0_14_ok() {
        for i in 0..=14 {
            assert!(VehicleClass::from_num(i).is_ok())
        }
    }

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
    fn speed_binning_is_correct() {
        let mut speed_count = SpeedRangeCount::new(123, Direction::West);

        assert!(speed_count.insert(-0.1).is_err());
        assert!(speed_count.insert(-0.0).is_err());

        // s1
        speed_count.insert(0.0).unwrap();
        speed_count.insert(0.1).unwrap();
        speed_count.insert(15.0).unwrap();

        // s2
        speed_count.insert(15.1).unwrap();
        speed_count.insert(20.0).unwrap();

        // s3
        speed_count.insert(20.1).unwrap();
        speed_count.insert(25.0).unwrap();

        // s4
        speed_count.insert(25.1).unwrap();
        speed_count.insert(30.0).unwrap();

        // s5
        speed_count.insert(30.1).unwrap();
        speed_count.insert(35.0).unwrap();

        // s6
        speed_count.insert(35.1).unwrap();
        speed_count.insert(40.0).unwrap();

        // s7
        speed_count.insert(40.1).unwrap();
        speed_count.insert(45.0).unwrap();

        // s8
        speed_count.insert(45.1).unwrap();
        speed_count.insert(50.0).unwrap();

        // s9
        speed_count.insert(50.1).unwrap();
        speed_count.insert(55.0).unwrap();

        // s10
        speed_count.insert(55.1).unwrap();
        speed_count.insert(60.0).unwrap();

        // s11
        speed_count.insert(60.1).unwrap();
        speed_count.insert(65.0).unwrap();

        // s12
        speed_count.insert(65.1).unwrap();
        speed_count.insert(70.0).unwrap();

        // s13
        speed_count.insert(70.1).unwrap();
        speed_count.insert(75.0).unwrap();

        // s14
        speed_count.insert(75.1).unwrap();
        speed_count.insert(100.0).unwrap();
        speed_count.insert(120.0).unwrap();

        assert_eq!(speed_count.s1, 3);
        assert_eq!(speed_count.s2, 2);
        assert_eq!(speed_count.s3, 2);
        assert_eq!(speed_count.s4, 2);
        assert_eq!(speed_count.s5, 2);
        assert_eq!(speed_count.s6, 2);
        assert_eq!(speed_count.s7, 2);
        assert_eq!(speed_count.s8, 2);
        assert_eq!(speed_count.s9, 2);
        assert_eq!(speed_count.s10, 2);
        assert_eq!(speed_count.s11, 2);
        assert_eq!(speed_count.s12, 2);
        assert_eq!(speed_count.s13, 2);
        assert_eq!(speed_count.s14, 3);
        assert_eq!(speed_count.total, 30);
    }

    #[test]
    fn metadata_parse_from_path_ok() {
        let path = Path::new("some/path/rc-166905-ew-40972-35.txt");
        let metadata = CountMetadata::new(path).unwrap();
        let expected_metadata = {
            CountMetadata {
                technician: "rc".to_string(),
                dvrpc_num: 166905,
                directions: Directions::new(Direction::East, Some(Direction::West)),
                counter_id: 40972,
                speed_limit: Some(35),
            }
        };
        assert_eq!(metadata, expected_metadata)
    }

    #[test]
    fn metadata_parse_from_path_ok_with_na_speed_limit() {
        let path = Path::new("some/path/rc-166905-ew-40972-na.txt");
        let metadata = CountMetadata::new(path).unwrap();
        let expected_metadata = {
            CountMetadata {
                technician: "rc".to_string(),
                dvrpc_num: 166905,
                directions: Directions::new(Direction::East, Some(Direction::West)),
                counter_id: 40972,
                speed_limit: None,
            }
        };
        assert_eq!(metadata, expected_metadata)
    }

    #[test]
    fn metadata_parse_from_path_errs_if_too_few_parts() {
        let path = Path::new("some/path/rc-166905-ew-40972.txt");
        assert!(matches!(
            CountMetadata::new(path),
            Err(CountError::InvalidFileName {
                problem: FileNameProblem::TooFewParts,
                ..
            })
        ))
    }

    #[test]
    fn metadata_parse_from_path_errs_if_too_many_parts() {
        let path = Path::new("some/path/rc-166905-ew-40972-35-extra.txt");
        assert!(matches!(
            CountMetadata::new(path),
            Err(CountError::InvalidFileName {
                problem: FileNameProblem::TooManyParts,
                ..
            })
        ))
    }

    #[test]
    fn metadata_parse_from_path_errs_if_technician_bad() {
        let path = Path::new("some/path/12-letters-ew-40972-35.txt");
        assert!(matches!(
            CountMetadata::new(path),
            Err(CountError::InvalidFileName {
                problem: FileNameProblem::InvalidTech,
                ..
            })
        ))
    }

    #[test]
    fn metadata_parse_from_path_errs_if_dvrpcnum_bad() {
        let path = Path::new("some/path/rc-letters-ew-40972-35.txt");
        assert!(matches!(
            CountMetadata::new(path),
            Err(CountError::InvalidFileName {
                problem: FileNameProblem::InvalidRecordNum,
                ..
            })
        ))
    }

    #[test]
    fn metadata_parse_from_path_errs_if_directions_bad() {
        let path = Path::new("some/path/rc-166905-eb-letters-35.txt");
        assert!(matches!(
            CountMetadata::new(path),
            Err(CountError::InvalidFileName {
                problem: FileNameProblem::InvalidDirections,
                ..
            })
        ));
        let path = Path::new("some/path/rc-166905-be-letters-35.txt");
        assert!(matches!(
            CountMetadata::new(path),
            Err(CountError::InvalidFileName {
                problem: FileNameProblem::InvalidDirections,
                ..
            })
        ));
        let path = Path::new("some/path/rc-166905-cc-letters-35.txt");
        assert!(matches!(
            CountMetadata::new(path),
            Err(CountError::InvalidFileName {
                problem: FileNameProblem::InvalidDirections,
                ..
            })
        ));
    }

    #[test]
    fn metadata_parse_from_path_errs_if_counter_id_bad() {
        let path = Path::new("some/path/rc-166905-ew-letters-35.txt");
        assert!(matches!(
            CountMetadata::new(path),
            Err(CountError::InvalidFileName {
                problem: FileNameProblem::InvalidCounterID,
                ..
            })
        ))
    }

    #[test]
    fn metadata_parse_from_path_errs_if_speedlimit_bad() {
        let path = Path::new("some/path/rc-166905-ew-40972-abc.txt");
        assert!(matches!(
            CountMetadata::new(path),
            Err(CountError::InvalidFileName {
                problem: FileNameProblem::InvalidSpeedLimit,
                ..
            })
        ))
    }
}

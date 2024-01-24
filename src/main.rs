//! See <https://www.dvrpc.org/traffic/> for additional information about traffic counting.
//! Naming convention in this program:
//!  - "CountedVehicle" is the individual vehicle that got counted
//!    "VehicleCount" is the overall count spanning multiple days

use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use csv::{Reader, ReaderBuilder};
use log::{error, info, LevelFilter};
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};
use time::{macros::format_description, Date, PrimitiveDateTime, Time};

const VEHICLE_COUNT_HEADER: &str = "Veh. No.,Date,Time,Channel,Class,Speed";

const LOG: &str = "log.txt";

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
    fn from_num(num: u8) -> Result<Self, String> {
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
            other => Err(format!("no such vehicle class '{other}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum CountType {
    FifteenMinuteBicycle,    // Eco-Counter
    FifteenMinutePedestrian, // Eco-Counter
    Vehicle, // this is the raw data that all of the other StarNext types get built from
}

impl CountType {
    fn from_location(path: &Path) -> Result<CountType, String> {
        // Get the directory immediately above the file.
        let parent = path
            .parent()
            .unwrap()
            .components()
            .last()
            .unwrap()
            .as_os_str()
            .to_str()
            .unwrap();

        match parent.to_lowercase().as_str() {
            "15minutebicycle" => Ok(CountType::FifteenMinuteBicycle),
            "15minutepedestrian" => Ok(CountType::FifteenMinutePedestrian),
            "vehicles" => Ok(CountType::Vehicle),
            _ => Err(format!("No matching count type for directory {path:?}")),
        }
    }

    fn from_header(path: &Path, location_count_type: CountType) -> Result<CountType, String> {
        // `location_count_type` is what we expect this file to be, based off its location. We use
        // this because different types of counts can have a variable number of metadata rows.

        let file = File::open(path).unwrap();
        let mut rdr = create_reader(&file);
        let header = rdr
            .records()
            .skip(num_metadata_rows_to_skip(location_count_type))
            .take(1)
            .last()
            .unwrap()
            .unwrap()
            .iter()
            .map(|x| x.trim().to_string())
            .collect::<Vec<String>>()
            .join(",");

        match header.as_str() {
            VEHICLE_COUNT_HEADER => Ok(CountType::Vehicle),
            _ => Err(format!("No matching count type for header in {path:?}.")),
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
    fn new(date: Date, time: Time, channel: u8, class: u8, speed: f32) -> Self {
        let class = VehicleClass::from_num(class).unwrap();
        Self {
            date,
            time,
            channel,
            class,
            speed,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CountMetadata<'a> {
    technician: &'a str,
    dvrpc_num: i32,
    directions: &'a str,
    counter_id: i32,
    speed_limit: i32,
    // start_datetime: PrimitiveDateTime,
    // site_code: usize,
    // station_id: Option<usize>,
}

impl<'a> CountMetadata<'a> {
    fn new(path: &'a Path) -> Result<Self, String> {
        let parts: Vec<&str> = path
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .split('-')
            .collect();

        if parts.len() != 5 {
            return Err("filename is not to specification".to_string());
        }

        let metadata = Self {
            technician: parts[0],
            dvrpc_num: parts[1].parse().unwrap(),
            directions: parts[2],
            counter_id: parts[3].parse().unwrap(),
            speed_limit: parts[4].parse().unwrap(),
        };

        Ok(metadata)
    }
}

/// FifteenMinuteSpeedCount represents a row in the TC_SPECOUNT table, with the key of the HashMap
/// containing the date and time fields as a datetime.
type FifteenMinuteSpeedRangeCount = HashMap<BinnedCountKey, SpeedRangeCount>;
type FifteenMinuteVehicleClassCount = HashMap<BinnedCountKey, VehicleClassCount>;

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
struct BinnedCountKey {
    datetime: PrimitiveDateTime,
    channel: u8,
}

#[derive(Debug, Clone, Copy)]
enum Direction {
    North,
    East,
    South,
    West,
}

#[derive(Debug)]
struct Directions {
    direction1: Direction,
    direction2: Option<Direction>,
}

impl Directions {
    fn from_metadata(directions: &str) -> Result<Self, String> {
        let (direction1, direction2) = match directions {
            "ns" => (Direction::North, Some(Direction::South)),
            "sn" => (Direction::South, Some(Direction::North)),
            "ew" => (Direction::East, Some(Direction::West)),
            "we" => (Direction::West, Some(Direction::East)),
            "nn" => (Direction::North, Some(Direction::North)),
            "ss" => (Direction::South, Some(Direction::South)),
            "ee" => (Direction::East, Some(Direction::East)),
            "ww" => (Direction::West, Some(Direction::West)),
            "n" => (Direction::North, None),
            "s" => (Direction::South, None),
            "e" => (Direction::East, None),
            "w" => (Direction::West, None),
            other => return Err(format!("invalid direction {other}")),
        };
        Ok(Self {
            direction1,
            direction2,
        })
    }
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
            total: 0,
        }
    }
    fn insert(&mut self, class: VehicleClass) -> Result<&Self, String> {
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
            _ => (),
        }

        // TODO: determine how to handle unclassed vehicles, and also how they should be
        // reflected in total. Most accurate representation would be to not reclass unclassed,
        // while still including in total.

        self.total += 1;
        Ok(self)
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
    fn insert(&mut self, speed: f32) -> Result<&Self, String> {
        if speed.is_sign_negative() {
            return Err(format!("invalid speed '{speed}'"));
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
            return Err(format!("invalid speed '{speed}'"));
        }

        self.total += 1;
        Ok(self)
    }
}

fn main() {
    // Load file containing environment variables, panic if it doesn't exist.
    dotenvy::dotenv().expect("Unable to load .env file.");

    // Get env var for path where CSVs will be, panic if it doesn't exist.
    let data_dir = env::var("DATA_DIR").expect("Unable to data directory path from .env file.");

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

    // Oracle env vars
    let username = match env::var("USERNAME") {
        Ok(v) => v,
        Err(e) => {
            error!("Unable to load username from .env file: {e}.");
            return;
        }
    };
    let password = match env::var("PASSWORD") {
        Ok(v) => v,
        Err(e) => {
            error!("Unable to load password from .env file: {e}.");
            return;
        }
    };

    let paths = collect_paths(&data_dir.into(), vec![]);

    // TODO: this will probably need to be wrapped in other type in order to accept both StarNext
    // and EcoCounter counts

    for path in paths {
        let metadata = match CountMetadata::new(&path) {
            Ok(v) => v,
            Err(e) => {
                error!("{path:?} not processed: {e}");
                continue;
            }
        };
        let directions = Directions::from_metadata(metadata.directions).unwrap();

        let data_file = match File::open(&path) {
            Ok(v) => v,
            Err(_) => {
                error!("Unable to open {path:?}. File not processed.");
                continue;
            }
        };

        let count_type_from_location = CountType::from_location(&path).unwrap();
        let count_type_from_header =
            CountType::from_header(&path, count_type_from_location).unwrap();

        if count_type_from_location != count_type_from_header {
            error!("{path:?}: Mismatch in count types between the file location and the header in that file. File not processed.");
            continue;
        }

        let counts = extract_counts(data_file, &path, count_type_from_location);
        // Created variously binned counts (by time period, speed range, vehicle class) from
        // counts of individual vehicles.
        let mut fifteen_min_speed_range_count: FifteenMinuteSpeedRangeCount = HashMap::new();
        let mut fifteen_min_vehicle_class_count: FifteenMinuteVehicleClassCount = HashMap::new();

        for count in counts {
            // Get the direction from the channel of count/metadata of filename.
            // Channel 1 is first direction, Channel 2 is the second (if any)
            let direction = match count.channel {
                1 => directions.direction1,
                2 => directions.direction2.unwrap(),
                _ => {
                    error!("Unable to determine channel/direction.");
                    continue;
                }
            };

            // create a key for the Hashmap for 15-minute periods
            let key = BinnedCountKey {
                datetime: PrimitiveDateTime::new(count.date, time_bin(count.time).unwrap()),
                channel: count.channel,
            };

            // Add new entry to 15-min speed range count if necessary, then insert count's speed.
            let speed_range_count = fifteen_min_speed_range_count
                .entry(key)
                .or_insert(SpeedRangeCount::new(metadata.dvrpc_num, direction));
            *speed_range_count = *speed_range_count.insert(count.speed).unwrap();

            // Add new entry to 15-min vehicle class count if necessary, then insert count's class.
            let vehicle_class_count = fifteen_min_vehicle_class_count
                .entry(key)
                .or_insert(VehicleClassCount::new(metadata.dvrpc_num, direction));
            *vehicle_class_count = *vehicle_class_count.insert(count.class).unwrap();
        }

        // some manual validating
        let specific_dt = PrimitiveDateTime::new(
            time::macros::date!(2023 - 11 - 06),
            time::macros::time!(11:00:00),
        );
        let channel1_key = BinnedCountKey {
            datetime: specific_dt,
            channel: 1,
        };
        let channel2_key = BinnedCountKey {
            datetime: specific_dt,
            channel: 2,
        };
        dbg!(&fifteen_min_speed_range_count.get(&channel1_key));
        dbg!(&fifteen_min_speed_range_count.get(&channel2_key));

        dbg!(&fifteen_min_vehicle_class_count.get(&channel1_key));
        dbg!(&fifteen_min_vehicle_class_count.get(&channel2_key));

        // dbg!(&fifteen_min_speed_range_count);
        // dbg!(fifteen_min_speed_range_count.len());
    }

    // mostly just debugging
    // for count in counts {
    //     dbg!(&count);
    // }
}

/// Collect all the file paths to extract data from.
fn collect_paths(dir: &PathBuf, mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();

        // Ignore the log file.
        if path.file_name().unwrap().to_str() == Some(LOG) {
            continue;
        }

        if path.is_dir() {
            paths = collect_paths(&path, paths);
        } else {
            paths.push(path);
        }
    }
    paths
}

/// Extract counts from a file.
fn extract_counts(data_file: File, path: &Path, count_type: CountType) -> Vec<CountedVehicle> {
    // Create CSV reader over file
    let mut rdr = create_reader(&data_file);

    info!("Extracting data from {path:?}, a {count_type:?} count.");

    // Iterate through data rows (skipping metadata rows + 1 for header).
    let mut counts = vec![];
    for row in rdr
        .records()
        .skip(num_metadata_rows_to_skip(count_type) + 1)
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

        if count_type == CountType::Vehicle {
            let count = CountedVehicle::new(
                count_date,
                count_time,
                row.as_ref().unwrap()[3].parse().unwrap(),
                row.as_ref().unwrap()[4].parse().unwrap(),
                row.as_ref().unwrap()[5].parse().unwrap(),
            );

            counts.push(count);
        }
    }
    counts
}

/// Put time into four bins per hour.
fn time_bin(time: Time) -> Result<Time, String> {
    let time = time.replace_second(0).unwrap();
    match time.minute() {
        0..=14 => Ok(time.replace_minute(0).unwrap()),
        15..=29 => Ok(time.replace_minute(15).unwrap()),
        30..=44 => Ok(time.replace_minute(30).unwrap()),
        45..=59 => Ok(time.replace_minute(45).unwrap()),
        _ => Err("Invalid minute".to_string()),
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
        CountType::Vehicle => 3,
        _ => 8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_type_vehicle_ok() {
        let path = Path::new("test_files/vehicles/rc-166905-ew-40972-35.txt");
        let ct_from_location = CountType::from_location(path).unwrap();
        let ct_from_header = CountType::from_header(path, ct_from_location).unwrap();
        assert_eq!(&ct_from_location, &ct_from_header);
        assert_eq!(ct_from_location, CountType::Vehicle);
    }

    #[test]
    fn extract_counts_gets_correct_number_of_counts() {
        let path = Path::new("test_files/vehicles/rc-166905-ew-40972-35.txt");
        let ct_from_location = CountType::from_location(path).unwrap();
        let data_file = File::open(path).unwrap();
        let counts = extract_counts(data_file, path, ct_from_location);
        assert_eq!(counts.len(), 8706);
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
        let path = Path::new("test_files/vehicles/rc-166905-ew-40972-35.txt");
        let metadata = CountMetadata::new(path).unwrap();
        let expected_metadata = {
            CountMetadata {
                technician: "rc",
                dvrpc_num: 166905,
                directions: "ew",
                counter_id: 40972,
                speed_limit: 35,
            }
        };
        assert_eq!(metadata, expected_metadata)
    }

    #[test]
    fn metadata_parse_from_path_errs_if_too_few_parts() {
        let path = Path::new("test_files/vehicles/rc-166905-ew-40972.txt");
        assert!(CountMetadata::new(path).is_err())
    }

    #[test]
    fn metadata_parse_from_path_errs_if_too_many_parts() {
        let path = Path::new("test_files/vehicles/rc-166905-ew-40972-35-extra.txt");
        assert!(CountMetadata::new(path).is_err())
    }
}

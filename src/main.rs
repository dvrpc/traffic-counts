//! See <https://www.dvrpc.org/traffic/> for additional information about traffic counting.
use std::env;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use chrono::{NaiveDate, NaiveTime};
use csv::{Reader, ReaderBuilder};
use log::{error, info, LevelFilter};
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};
use time::macros::format_description;

const SPEED_COUNT_HEADER: &str = "Number,Date,Time,>0 to 15,>15 to 20,>20 to 25,>25 to 30,>30 to 35,>35 to 40,>40 to 45,>45 to 50,>50 to 55,>55 to 60,>60 to 65,>65 to 70,>70 to 75,>75";
const CLASSED_VOLUME_COUNT_HEADER: &str = "Number,Date,Time,Motorcycles,Cars & Trailers,2 Axle Long,Buses,2 Axle 6 Tire,3 Axle Single,4 Axle Single,<5 Axl Double,5 Axle Double,>6 Axl Double,<6 Axl Multi,6 Axle Multi,>6 Axl Multi,Not Classed";
const VOLUME_COUNT_HEADER1: &str = "Number,Date,Time,Channel 1";
const VOLUME_COUNT_HEADER2: &str = "Number,Date,Time,Channel 2";
const VEHICLE_COUNT_HEADER: &str = "Veh. No.,Date,Time,Channel,Class,Speed";

const LOG: &str = "log.txt";

/* Not sure if this will be needed, but these are the names of the 15 classifications from the FWA.
   See:
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

// 0 is unclassed from starnext "class" field

#[derive(Debug, Clone, Copy, PartialEq)]
enum CountType {
    FifteenMinuteVolume,
    FifteenMinuteClassedVolume,
    FifteenMinuteSpeed,
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
            "15minutevolume" => Ok(CountType::FifteenMinuteVolume),
            "15minutespeed" => Ok(CountType::FifteenMinuteSpeed),
            "15minuteclassedvolume" => Ok(CountType::FifteenMinuteClassedVolume),
            "15minutebicycle" => Ok(CountType::FifteenMinuteBicycle),
            "15minutepedestrian" => Ok(CountType::FifteenMinutePedestrian),
            "vehicles" => Ok(CountType::Vehicle),
            _ => Err(format!("No matching count type for directory {path:?}")),
        }
    }
    fn from_header(path: &Path, expected_count_type: CountType) -> Result<CountType, String> {
        // location_count_type is what we expect this file to be. We use this because the various
        // counts have a variable number of metadata rows.

        let file = File::open(path).unwrap();
        let mut rdr = create_reader(&file);
        let header = rdr
            .records()
            .skip(num_metadata_rows_to_skip(expected_count_type))
            .take(1)
            .last()
            .unwrap()
            .unwrap()
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .join(",");

        match header.as_str() {
            SPEED_COUNT_HEADER => Ok(CountType::FifteenMinuteSpeed),
            CLASSED_VOLUME_COUNT_HEADER => Ok(CountType::FifteenMinuteClassedVolume),
            VOLUME_COUNT_HEADER1 | VOLUME_COUNT_HEADER2 => Ok(CountType::FifteenMinuteVolume),
            VEHICLE_COUNT_HEADER => Ok(CountType::Vehicle),
            _ => Err(format!("No matching count type for header in {path:?}.")),
        }
    }
}

// Vehicle Counts - the raw, unbinned data
#[derive(Debug, Clone)]
struct VehicleCount {
    date: NaiveDate,
    time: NaiveTime,
    channel: u8,
    class: VehicleClass,
    speed: f32,
}

// Volume counts - without modifiers - are simple totals of all vehicles counted in given interval
#[derive(Debug, Clone)]
struct FifteenMinuteVolumeCount {
    date: NaiveDate,
    time: NaiveTime,
    count: usize,
}

// Speed counts are volume counts by speed range
// The CSV for this includes fields:
// id,date,time,<counts for 14 speed ranges: 0-15, 5-mph increments to 75, more than 75>jj
// A file contains speed counts if it ends in a "-1" (North and East) or "-2" (South and West).
#[derive(Debug, Clone)]
struct FifteenMinuteSpeedCount {
    date: NaiveDate,
    time: NaiveTime,
    counts: [usize; 14],
}

impl FifteenMinuteSpeedCount {
    fn new(date: NaiveDate, time: NaiveTime, counts: [usize; 14]) -> Self {
        FifteenMinuteSpeedCount { date, time, counts }
    }
}

// The CSV for this includes fields:
// id,date,time,<counts for each the 14 used classifications (see above)>
#[derive(Debug, Clone)]
struct FifteenMinuteClassedVolumeCount {
    date: NaiveDate,
    time: NaiveTime,
    counts: [usize; 14],
}

// The first 8 lines of CSVs contain metadata.
// (They are followed by a blankline, the header, and then data.)
#[derive(Debug, Clone)]
struct CountMetadata {
    filename: String,
    start_date: NaiveDate,
    start_time: NaiveTime,
    site_code: usize,
    station_id: Option<usize>,
    location_2: Option<usize>,
    latitude: Option<f32>,
    longitude: Option<f32>,
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

    walk_dirs(&data_dir.into())
}

fn walk_dirs(dir: &PathBuf) {
    // For now, just walk directory and print out data
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();

        // Ignore the log file.
        if path.file_name().unwrap().to_str() == Some(LOG) {
            continue;
        }

        if path.is_dir() {
            walk_dirs(&path);
        } else {
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
            } else {
                extract_data(data_file, &path, count_type_from_location)
            }
        }
    }
}

fn extract_data(data_file: File, path: &Path, count_type: CountType) {
    // Create CSV reader over file
    let mut rdr = create_reader(&data_file);

    info!("Extracting data from {path:?}, a {count_type:?} count.");

    // Iterate through data rows (skipping metadata rows + 1 for header)
    for row in rdr
        .records()
        .skip(num_metadata_rows_to_skip(count_type) + 1)
    {
        // Parse date.
        let date_format = format_description!("[month padding:none]/[day padding:none]/[year]");
        let date_col = &row.as_ref().unwrap()[1];
        let count_date = time::Date::parse(date_col, &date_format);

        // Parse time.
        let time_format =
            format_description!("[hour padding:none repr:12]:[minute]:[second] [period]");
        let time_col = &row.as_ref().unwrap()[2];
        let count_time = time::Time::parse(time_col, &time_format);

        println!("{:?} {:?}", count_time.unwrap(), count_date.unwrap());

        // put data into structs
        // match count_type {
        //     CountType::FifteenMinuteVolumeCount => {}
        //     CountType::FifteenMinuteClassedVolumeCount => {}
        //     CountType::FifteenMinuteSpeedCount => {}
        // }
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
    fn count_type_15minspeed_mismatch_errs() {
        let path = Path::new("test_files/15minutespeed/class_count.txt");
        let ct_from_location = CountType::from_location(path).unwrap();
        let ct_from_header = CountType::from_header(path, ct_from_location).unwrap();
        assert_ne!(ct_from_header, ct_from_location);
    }

    #[test]
    fn count_type_15minspeed_ok() {
        let path = Path::new("test_files/15minutespeed/166905-2.txt");
        let ct_from_location = CountType::from_location(path).unwrap();
        let ct_from_header = CountType::from_header(path, ct_from_location).unwrap();
        assert_eq!(ct_from_header, ct_from_location);
        assert!(matches!(ct_from_location, CountType::FifteenMinuteSpeed))
    }

    #[test]
    fn count_type_15minclassedvolume_mismatch_errs() {
        let path = Path::new("test_files/15minuteclassedvolume/speed_count.txt");
        let ct_from_location = CountType::from_location(path).unwrap();
        let ct_from_header = CountType::from_header(path, ct_from_location).unwrap();
        assert_ne!(ct_from_location, ct_from_header);
    }

    #[test]
    fn count_type_15minclassedvolume_ok() {
        let path = Path::new("test_files/15minuteclassedvolume/rc166905w.txt");
        let ct_from_location = CountType::from_location(path).unwrap();
        let ct_from_header = CountType::from_header(path, ct_from_location).unwrap();
        assert_eq!(ct_from_location, ct_from_header);
        assert!(matches!(
            ct_from_location,
            CountType::FifteenMinuteClassedVolume
        ))
    }

    #[test]
    fn count_type_vehicle_ok() {
        let path = Path::new("test_files/vehicles/rc-166905-ew-40972-35.txt");
        let ct_from_location = CountType::from_location(path).unwrap();
        let ct_from_header = CountType::from_header(path, ct_from_location).unwrap();
        assert_eq!(&ct_from_location, &ct_from_header);
        assert_eq!(ct_from_location, CountType::Vehicle);
    }
}

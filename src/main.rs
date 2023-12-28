//! See <https://www.dvrpc.org/traffic/> for additional information about traffic counting.
use std::env;
use std::fs::OpenOptions;

use chrono::{NaiveDate, NaiveTime};
use log::{error, LevelFilter};
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};

/* Not sure if this will be needed, but these are the names of the 15 classifications from the FWA.
   See:
    * <https://www.fhwa.dot.gov/policyinformation/vehclass.cfm>
    * <https://www.fhwa.dot.gov/policyinformation/tmguide/tmg_2013/vehicle-types.cfm>
    * <https://www.fhwa.dot.gov/publications/research/infrastructure/pavements/ltpp/13091/002.cfm>
*/
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

// Volume counts - without modifiers - are simple totals of all vehicles counted in given interval
#[derive(Debug, Clone)]
struct FifteenMinuteVolumeCount {
    date: NaiveDate,
    time: NaiveTime,
    count: usize,
}

// Speed counts are volume counts by speed range
// The CSV for this includes fields:
// id,date,time,<counts for 14 speed ranges: 0-15, 5-mph increments to 75, more than 75>
#[derive(Debug, Clone)]
struct FifteenMinuteSpeedCount {
    date: NaiveDate,
    time: NaiveTime,
    counts: [usize; 14],
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
                .open(format!("{data_dir}/log.txt"))
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
}

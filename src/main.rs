//! See <https://www.dvrpc.org/traffic/> for additional information about traffic counting.
use std::env;
use std::fs::{self, File, OpenOptions};

use chrono::{NaiveDate, NaiveTime};
use log::{debug, error, LevelFilter};
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};
use time::macros::format_description;

const SPEED_COUNT_HEADER: &str = "Number,Date,Time,>0 to 15,>15 to 20,>20 to 25,>25 to 30,>30 to 35,>35 to 40,>40 to 45,>45 to 50,>50 to 55,>55 to 60,>60 to 65,>65 to 70,>70 to 75,>75";

const CLASS_COUNT_HEADER: &str = "Number,Date,Time,Motorcycles,Cars & Trailers,2 Axle Long,Buses,2 Axle 6 Tire,3 Axle Single,4 Axle Single,<5 Axl Double,5 Axle Double,>6 Axl Double,<6 Axl Multi,6 Axle Multi,>6 Axl Multi,Not Classed";

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

    // For now, just walk directory and print out data
    for entry in fs::read_dir(data_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if !path.is_file() || path.file_name().unwrap().to_str() == Some("log.txt") {
            continue;
        }
        println!("{:?}", path);
        let data_file = match File::open(&path) {
            Ok(v) => v,
            Err(_) => {
                debug!("Unable to open {:?}.", path);
                continue;
            }
        };

        // Create CSV reader over file,
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(data_file);

        // Determine what type of count this file contains, in two ways:
        //   1. file directory location
        //   2. the header of the file matches that type of count for that directory

        // Get location.

        // Get header.
        let header: String = rdr
            .records()
            .skip(8)
            .take(1)
            .last()
            .unwrap()
            .unwrap()
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .join(",");
        println!("header: {header}");

        // Confirm header is correct for count type we expect in that directory

        // the remaining rows are individual counts
        for row in rdr.records() {
            // Classed counts and speed counts have same fields for date/time

            // Parse date.
            // TODO: unsure if StarNext uses DD or D for month - waiting for sample. Until then, using DD>
            let date_format = format_description!("[month]/[day padding:none]/[year]");
            let date_col = &row.as_ref().unwrap()[1];
            let count_date = time::Date::parse(date_col, &date_format);

            // Parse time.
            let time_format =
                format_description!("[hour padding:none repr:12]:[minute]:[second] [period]");
            let time_col = &row.as_ref().unwrap()[2];
            let count_time = time::Time::parse(time_col, &time_format);

            println!("{:?} {:?}", count_time.unwrap(), count_date.unwrap());

            // put data into structs
        }
    }
}

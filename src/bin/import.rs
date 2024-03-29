use std::env;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::PathBuf;

use log::{error, info, LevelFilter};
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};

use traffic_counts::intermediate::VehicleClassCount;
use traffic_counts::{
    annual_avg::determine_date,
    db::{create_pool, CountTable},
    extract_from_file::Extract,
    *,
};

const LOG: &str = "log.txt";

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
    let username = match env::var("DB_USERNAME") {
        Ok(v) => v,
        Err(e) => {
            error!("Unable to load username from .env file: {e}.");
            return;
        }
    };
    let password = match env::var("DB_PASSWORD") {
        Ok(v) => v,
        Err(e) => {
            error!("Unable to load password from .env file: {e}.");
            return;
        }
    };
    let pool = create_pool(username, password).unwrap();
    let conn = pool.get().unwrap();

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
    // desired shape, and inserting it into the database.
    // Exactly how the data is processed depends on what `InputCount` it is.
    for path in paths {
        let count_type = match InputCount::from_parent_dir_and_header(path) {
            Ok(v) => v,
            Err(e) => {
                error!("{path:?} not processed: {e}");
                continue;
            }
        };

        info!("Extracting data from {path:?}, a {count_type:?} count.");
        let metadata = match CountMetadata::from_path(path) {
            Ok(v) => v,
            Err(e) => {
                error!("{path:?} not processed: {e}");
                continue;
            }
        };
        let record_num = metadata.clone().dvrpc_num;
        // Process the file according to InputCount.
        match count_type {
            InputCount::IndividualVehicle => {
                // Extract data from CSV/text file
                let individual_vehicles = match IndividualVehicle::extract(path) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("{path:?} not processed: {e}");
                        continue;
                    }
                };

                // Create two counts from this: 15-minute speed count and 15-minute class count
                // TODO: this could also be for other intervals - the function is probably too
                // specific as is and should take desired interval as parameter
                let (speed_range_count, vehicle_class_count) =
                    create_speed_and_class_count(metadata.clone(), individual_vehicles.clone());
                let date = determine_date(individual_vehicles.clone());
                // dbg!(date);
                // dbg!(vehicle_class_count);

                // Create records for the non-normalized TC_VOLCOUNT table.
                // (the one with specific hourly fields - AM12, AM1, etc. - rather than a single
                // hour field and count)
                let non_normal_vol_count =
                    create_non_normal_vol_count(metadata.clone(), individual_vehicles.clone());

                // dbg!(&non_normal_vol_count);

                // Create records for the non-normalized TC_SPESUM table
                // (another one with specific hourly fields, this time for average speed/hour)
                let non_normal_speedavg_count =
                    create_non_normal_speedavg_count(metadata.clone(), individual_vehicles);

                // dbg!(&non_normal_speedavg_count);
                // TODO: enter these into the database
                FifteenMinuteVehicleClassCount::delete(&conn, record_num).unwrap();
                FifteenMinuteSpeedRangeCount::delete(&conn, record_num).unwrap();
                NonNormalVolCount::delete(&conn, record_num).unwrap();
                NonNormalAvgSpeedCount::delete(&conn, record_num).unwrap();
            }
            InputCount::FifteenMinuteVehicle => {
                let fifteen_min_volcount = match FifteenMinuteVehicle::extract(path) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("{path:?} not processed: {e}");
                        continue;
                    }
                };

                // As they are already binned by 15-minute period, these need no further processing.
                // TODO: enter into database.
                FifteenMinuteVehicle::delete(&conn, record_num).unwrap();
            }
            InputCount::FifteenMinuteBicycle => (),
            InputCount::FifteenMinutePedestrian => (),
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

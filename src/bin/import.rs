use std::env;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use log::{error, info, LevelFilter};
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};

use traffic_counts::{
    extract_from_file::{get_count_type, Extract},
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

/// Process count - extract from file, transform, load into database.
fn process_count(path: &Path) -> Result<(), CountError> {
    // Get count type and metatadata from file; create CSV reader over it.
    let count_type = get_count_type(path)?;
    // Process the file according to CountType
    info!("Extracting data from {path:?}, a {count_type:?} count.");
    match count_type {
        CountType::IndividualVehicle => {
            // Extract data from CSV/text file
            let counted_vehicles = CountedVehicle::extract(path)?;

            let metadata = CountMetadata::from_path(path)?;

            // Create two counts from this: 15-minute speed count and 15-minute class count
            // TODO: this could also be for other intervals - the function is probably too
            // specific as is and should take desired interval as parameter
            let (speed_range_count, vehicle_class_count) =
                create_speed_and_class_count(metadata.clone(), counted_vehicles.clone());

            dbg!(vehicle_class_count);

            // Create records for the non-normalized TC_VOLCOUNT table.
            // (the one with specific hourly fields - AM12, AM1, etc. - rather than a single
            // hour field and count)
            let non_normal_volcount = create_non_normal_volcount(metadata, counted_vehicles);

            dbg!(non_normal_volcount);

            // TODO: enter these into the database
        }
        CountType::FifteenMinuteVehicle => {
            let fifteen_min_volcount = FifteenMinuteVehicle::extract(path)?;

            // As they are already binned by 15-minute period, these need no further processing.
            // TODO: enter into database.
        }
        CountType::FifteenMinuteBicycle => (),
        CountType::FifteenMinutePedestrian => (),
    }
    Ok(())
}

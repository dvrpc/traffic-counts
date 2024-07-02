//! Import traffic counts to our database from files.
//! This program watches a directory for files to be uploaded to one of the following subdirectories:
//!   - vehicle/  - for raw, unbinned records of [individual vehicles][IndividualVehicle] containing vehicle class and speed, from STARneXt/JAMAR
//!   - 15minutevehicle/ - for [pre-binned, 15-minute volume counts][FifteenMinuteVehicle] from STARneXt/JAMAR
//!   - 15minutebicycle/ - for [pre-binned, 15-minute bicycle counts][FifteenMinuteBicycle] from
//! Eco-Counter
//!   - 15minutepedestrian/ - for [pre-binned, 15-minute pedestrian counts][FifteenMinutePedestrian]
//! from Eco-Counter
//!
//! When a file is found, the program verifies that it contains the correct/expected kind of data,
//! derives the appropriate counts from it, and then inserts these into our database and removes
//! the file. (NOTE: removal of files is not yet implemented.)
//!
//! A [log][`LOG`] of the program's work is kept in the main directory.
//! The program is able to log most errors and continue its execution,
//! so that an error in one file will not prevent it from successfully processing another.
//! The program itself should only fail if it is misconfigured, meaning that,
//! once started successfully, it should run indefinitely.
//!
//! ## Filename specification
//!
//! The names of all exported files (see below for export process) should be in the form
//! [tech initials]-[record num]-[direction(s)]-[physical counter id]-[speed limit].csv.
//!
//! (.txt can also be used for the file extension rater than .csv.)
//!
//! All components must be present, separated by a dash (-).
//! Here are several examples:
//!   - rc-166905-ew-40972-35.csv
//!     - "rc" is the technician's initials.
//!     - "166905" is the recordnum of the count.
//!     - "ew" is the direction. In this case, two lanes going opposite directions.
//!     - "40972" is the physical machine the count was taken on.
//!     - "35" is the speed limit.
//!   - kh-165367-ee-40972-35.csv
//!     - "kh" is the technician's initials.
//!     - "165367" is the recordnum of the count.
//!     - "ee" is the direction. In this case, two lanes going the same direction.
//!     - "40972" is the physical machine the count was taken on.
//!     - "35" is the speed limit.
//!   - kw-123456-s-101-na.csv
//!     - "kw" is the technician's initials.
//!     - "123456" is the recordnum of the count.
//!     - "s" is the direction. In this case, only one lane, going south.
//!     - "101" is the physical machine the count was taken on.
//!     - "na" for unknown/not available speed limit.
//!
//! All possible sets of directions:
//!   - ew
//!   - we
//!   - ns
//!   - sn
//!   - ee
//!   - ww
//!   - nn
//!   - ss
//!   - e
//!   - w
//!   - n
//!   - s
//!
//! Note that for bicycle and pedestrian counts that are unidirectional, the program will use
//! the total for each period, capturing both in/out directions and thus any wrong-way travel.
//! In terms of the filename, this would mean using a single direction in that position,
//! e.g. like the last example above.
//!
//! ## Exporting from STARneXt
//!
//! The process is the same whether doing class/speed or simple volume counts.
//!
//!   - open STARneXt, open a .snj file.
//!   - click the **Process** button from the top menu to transform tube pulses to
//! vehicle counts. This will take you to a "Per Vehicle Records" tab.
//!   - click the **Export** button from the top menu, selecting *ASCII (CSV)*
//! as the format. Then:
//!     - leave the radio button checked for *Export all vehicles*
//!     - click **Next**
//!     - click the checkboxes for all channels available
//!     - click the **Output Format** button, and then choose the following settings:
//!       - under the "Header Fields" column:
//!         - *Start Date and Time*
//!       - under "Included Data":
//!         - *Class*
//!         - *Speed*
//!       - under "Options", use the defaults:
//!         - *Date and Time Separate*
//!         - *Header Titles Separate*
//!         - *Include Vehicle No*
//!         - *Comma* for the *Delimiter* field
//!         - *Channel Number* rather than *Channel Name*
//!    - click **Done** to return from the Output Format menu
//!    - click **Export** to save the file locally.
//!
//! ## Exporting from Eco-Counter
//!
//! For both bicycle and pedestrian counts, in [Eco-Vizio](https://www.eco-visio.net):
//!   - go to the Analysis tab
//!   - set the (custom) time period covered
//!   - choose the counter the count was taken on
//!   - choose *15 min* from the **Traffic** dropdown menu.
//!
//! A visualization will then appear in the main area of the site. Do the following:
//!   - By default the visualization is set to *Curve*; change it to *Table*.
//!   - Select **Options** and ensure that *Total per site* and *Directions* are
//! both toggled on and that both directions (in/out) are included.
//!   - click on the **Download** (⤓) button, choosing *Spreadsheet (CSV)* as the format, comma as
//! the delimiter, and save locally.

use std::env;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::PathBuf;

use log::{error, info, LevelFilter};
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};

use traffic_counts::{
    aadv::Aadv,
    count_insert::CountInsert,
    db::create_pool,
    denormalize::{Denormalize, *},
    extract_from_file::Extract,
    *,
};

const LOG: &str = "import.log";

fn main() {
    // Load file containing environment variables, panic if it doesn't exist.
    dotenvy::dotenv().expect("Unable to load .env file.");

    // Get env var for path where CSVs will be, panic if it doesn't exist.
    let data_dir =
        env::var("DATA_DIR").expect("Unable to load data directory path from .env file.");

    // Get env var for path where log will be, panic if it doesn't exist.
    let log_dir = env::var("LOG_DIR").expect("Unable to load log directory path from .env file.");

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
                .open(format!("{log_dir}/{LOG}"))
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
                // Extract data from CSV/text file.
                let individual_vehicles = match IndividualVehicle::extract(path) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("{path:?} not processed: {e}");
                        continue;
                    }
                };

                // Create two counts from this: 15-minute speed count and 15-minute class count
                let (speed_range_count, vehicle_class_count) = create_speed_and_class_count(
                    metadata.clone(),
                    individual_vehicles.clone(),
                    TimeInterval::FifteenMin,
                );

                // Create records for the non-normalized TC_SPESUM table (another one with
                // specific hourly fields, this time for average speed/hour).
                let non_normal_speedavg_count =
                    create_non_normal_speedavg_count(metadata.clone(), individual_vehicles);

                // Delete existing records from db.
                TimeBinnedVehicleClassCount::delete(&conn, record_num).unwrap();
                TimeBinnedSpeedRangeCount::delete(&conn, record_num).unwrap();
                NonNormalAvgSpeedCount::delete(&conn, record_num).unwrap();

                // Create prepared statments and use them to insert counts.
                let mut prepared = TimeBinnedVehicleClassCount::prepare_insert(&conn).unwrap();
                for count in vehicle_class_count {
                    match count.insert(&mut prepared) {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error inserting count {count:?}: {e}");
                        }
                    }
                }
                conn.commit().unwrap();

                let mut prepared = TimeBinnedSpeedRangeCount::prepare_insert(&conn).unwrap();
                for count in speed_range_count {
                    match count.insert(&mut prepared) {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error inserting count {count:?}: {e}");
                        }
                    }
                }
                conn.commit().unwrap();

                // Denormalize this data to insert into tc_volcount table.
                let denormalized_volcount =
                    TimeBinnedVehicleClassCount::denormalize_vol_count(record_num, &conn).unwrap();

                // Delete existing records from db.
                NonNormalVolCount::delete(&conn, record_num).unwrap();

                // Create prepared statments and use them to insert counts.
                let mut prepared = NonNormalVolCount::prepare_insert(&conn).unwrap();
                for count in denormalized_volcount {
                    match count.insert(&mut prepared) {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error inserting count {count:?}: {e}");
                        }
                    }
                }
                conn.commit().unwrap();

                let mut prepared = NonNormalAvgSpeedCount::prepare_insert(&conn).unwrap();
                for count in non_normal_speedavg_count {
                    match count.insert(&mut prepared) {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error inserting count {count:?}: {e}");
                        }
                    }
                }
                conn.commit().unwrap();

                // Calculate and insert the annual average daily volume.
                if let Err(e) = TimeBinnedVehicleClassCount::insert_aadv(record_num as u32, &conn) {
                    error!("failed to calculate/insert AADV for {path:?}: {e}")
                }
            }
            InputCount::FifteenMinuteVehicle => {
                // Extract data from CSV/text file.
                let fifteen_min_volcount = match FifteenMinuteVehicle::extract(path) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("{path:?} not processed: {e}");
                        continue;
                    }
                };

                // As they are already binned by 15-minute period, these need no further
                // processing; just insert into database.
                FifteenMinuteVehicle::delete(&conn, record_num).unwrap();
                let mut prepared = FifteenMinuteVehicle::prepare_insert(&conn).unwrap();
                for count in fifteen_min_volcount {
                    match count.insert(&mut prepared) {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error inserting count {count:?}: {e}");
                        }
                    }
                }
                conn.commit().unwrap();

                // Calculate and insert the annual average daily volume.
                if let Err(e) = FifteenMinuteVehicle::insert_aadv(record_num as u32, &conn) {
                    error!("failed to calculate/insert AADV for {path:?}: {e}")
                }

                // Denormalize this data to insert into tc_volcount table.
                let denormalized_volcount =
                    FifteenMinuteVehicle::denormalize_vol_count(record_num, &conn).unwrap();

                // Delete existing records from db.
                NonNormalVolCount::delete(&conn, record_num).unwrap();

                // Create prepared statments and use them to insert counts.
                let mut prepared = NonNormalVolCount::prepare_insert(&conn).unwrap();
                for count in denormalized_volcount {
                    match count.insert(&mut prepared) {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error inserting count {count:?}: {e}");
                        }
                    }
                }
                conn.commit().unwrap();
            }
            InputCount::FifteenMinuteBicycle => {
                // Extract data from CSV/text file.
                let fifteen_min_volcount = match FifteenMinuteBicycle::extract(path) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("{path:?} not processed: {e}");
                        continue;
                    }
                };

                // As they are already binned by 15-minute period, these need no further
                // processing; just insert into database.
                FifteenMinuteBicycle::delete(&conn, record_num).unwrap();
                let mut prepared = FifteenMinuteBicycle::prepare_insert(&conn).unwrap();
                for count in fifteen_min_volcount {
                    match count.insert(&mut prepared) {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error inserting count {count:?}: {e}");
                        }
                    }
                }
                conn.commit().unwrap();

                // Calculate and insert the annual average daily volume.
                if let Err(e) = FifteenMinuteBicycle::insert_aadv(record_num as u32, &conn) {
                    error!("failed to calculate/insert AADV for {path:?}: {e}")
                }
            }
            InputCount::FifteenMinutePedestrian => {
                // Extract data from CSV/text file.
                let fifteen_min_volcount = match FifteenMinutePedestrian::extract(path) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("{path:?} not processed: {e}");
                        continue;
                    }
                };

                // As they are already binned by 15-minute period, these need no further
                // processing; just insert into database.
                FifteenMinutePedestrian::delete(&conn, record_num).unwrap();
                let mut prepared = FifteenMinutePedestrian::prepare_insert(&conn).unwrap();
                for count in fifteen_min_volcount {
                    match count.insert(&mut prepared) {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error inserting count {count:?}: {e}");
                        }
                    }
                }
                conn.commit().unwrap();

                // Calculate and insert the annual average daily volume.
                if let Err(e) = FifteenMinutePedestrian::insert_aadv(record_num as u32, &conn) {
                    error!("failed to calculate/insert AADV for {path:?}: {e}")
                }
            }
            // Nothing to do here.
            InputCount::FifteenMinuteBicycleOrPedestrian => (),
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

//! Import traffic counts to our database from files.
//! This program watches a directory for files to be uploaded to one of the following subdirectories:
//!   - vehicle/  - for raw, unbinned records of [individual vehicles][IndividualVehicle] containing vehicle class and speed, from STARneXt/JAMAR
//!   - 15minutevehicle/ - for [pre-binned, 15-minute volume counts][FifteenMinuteVehicle] from STARneXt/JAMAR
//!   - 15minutebicycle/ - for [pre-binned, 15-minute bicycle counts][FifteenMinuteBicycle] from
//!     Eco-Counter
//!   - 15minutepedestrian/ - for [pre-binned, 15-minute pedestrian counts][FifteenMinutePedestrian]
//!     from Eco-Counter
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
//! [record num]-[direction(s)]-[physical counter id]-[speed limit].csv.
//!
//! (.txt can also be used for the file extension rater than .csv.)
//!
//! All components must be present, separated by a dash (-).
//! Here are several examples:
//!   - 166905-ew-40972-35.csv
//!     - "166905" is the recordnum of the count.
//!     - "ew" is the direction. In this case, two lanes going opposite directions.
//!     - "40972" is the physical machine the count was taken on.
//!     - "35" is the speed limit.
//!   - 165367-ee-40972-35.csv
//!     - "165367" is the recordnum of the count.
//!     - "ee" is the direction. In this case, two lanes going the same direction.
//!     - "40972" is the physical machine the count was taken on.
//!     - "35" is the speed limit.
//!   - 123456-s-101-na.csv
//!     - "123456" is the recordnum of the count.
//!     - "s" is the direction. In this case, only one lane, going south.
//!     - "101" is the physical machine the count was taken on.
//!     - "na" for unknown/not available speed limit.
//!
//! All possible sets of directions:
//!   - e
//!   - w
//!   - n
//!   - s
//!   - ew
//!   - we
//!   - ns
//!   - sn
//!   - ee
//!   - ww
//!   - nn
//!   - ss
//!   - eee
//!   - www
//!   - nnn
//!   - sss
//!
//! Note that for bicycle and pedestrian counts that are unidirectional, the program will use
//! the total for each period, capturing both in/out directions and thus any wrong-way travel.
//! In terms of the filename, this would mean using a single direction in that position.
//!
//! ## Exporting from STARneXt
//!
//! The process is the same whether doing class/speed or simple volume counts.
//!
//!   - open STARneXt, open a .snj file.
//!   - click the **Process** button from the top menu to transform tube pulses to
//!     vehicle counts. This will take you to a "Per Vehicle Records" tab.
//!   - click the **Export** button from the top menu, selecting *ASCII (CSV)*
//!     as the format. Then:
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
//!     both toggled on and that both directions (in/out) are included.
//!   - click on the **Download** (⤓) button, choosing *Spreadsheet (CSV)* as the format, comma   //!     as the delimiter, and save locally.

use std::env;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::PathBuf;
use std::thread;
use std::time;

use log::{error, info, Level, LevelFilter};
use oracle::Connection;
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};

use traffic_counts::{
    check_data::check,
    create_speed_and_class_count,
    db::{create_pool, crud::Crud, insert_aadv2, insert_import_log_entry, ImportLogEntry},
    denormalize::{Denormalize, *},
    extract_from_file::{Extract, InputCount},
    FieldMetadata, FifteenMinuteBicycle, FifteenMinutePedestrian, FifteenMinuteVehicle,
    IndividualVehicle, TimeBinnedSpeedRangeCount, TimeBinnedVehicleClassCount, TimeInterval,
};

const LOG: &str = "import.log";
const CHECK_DATA_LOG: &str = "data_check.log";
const TIME_BETWEEN_LOOPS: u64 = 20;

fn main() {
    // Load file containing environment variables, panic if it doesn't exist.
    dotenvy::dotenv().expect("Unable to load .env file.");

    // Get env var for path where CSVs will be, panic if it doesn't exist.
    let data_dir =
        env::var("DATA_DIR").expect("Unable to load data directory path from .env file.");

    // Get env var for path where log will be, panic if it doesn't exist.
    let log_dir = env::var("LOG_DIR").expect("Unable to load log directory path from .env file.");

    // Get env var for whether or not to clean up files.
    // (When run in production, we want to remove the data files after they've been processed.)
    let cleanup_files = match env::var("IMPORT_CLEANUP_FILES") {
        Ok(v) if v == "true" => true,
        Ok(_) => false,
        Err(_) => false,
    };
    // Set up logging, panic if it fails.
    // (This is being done instead the loop in case the logs accidentally get deleted, so
    // the files can be recreated.)
    // Log messages related to actual import.
    let import_config = ConfigBuilder::new()
        .set_time_format_rfc3339()
        .add_filter_allow("import".to_string())
        .build();

    // Log messages related to data verification.
    let check_config = ConfigBuilder::new()
        .set_time_format_rfc3339()
        .add_filter_allow("check".to_string())
        .build();
    CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Debug,
            import_config.clone(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        ),
        WriteLogger::new(
            LevelFilter::Info,
            import_config,
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(format!("{log_dir}/{LOG}"))
                .expect("Could not open log file."),
        ),
        TermLogger::new(
            LevelFilter::Debug,
            check_config.clone(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        ),
        WriteLogger::new(
            LevelFilter::Info,
            check_config,
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(format!("{log_dir}/{CHECK_DATA_LOG}"))
                .expect("Could not open log file."),
        ),
    ])
    .expect("Could not configure logging.");

    // The database env vars aren't needed for a while, but if they aren't available, return
    // early before doing any work.
    let username = match env::var("DB_USERNAME") {
        Ok(v) => v,
        Err(e) => {
            error!(
                target: "import",
                "Unable to load username from .env file: {e}."
            );
            return;
        }
    };
    let password = match env::var("DB_PASSWORD") {
        Ok(v) => v,
        Err(e) => {
            error!(target: "import", "Unable to load password from .env file: {e}.");
            return;
        }
    };
    let pool = create_pool(username, password).unwrap();
    let conn = pool.get().unwrap();

    loop {
        // Recreate the logs in case they somehow get deleted.
        let _ = OpenOptions::new()
            .append(true)
            .create(true)
            .open(format!("{log_dir}/{LOG}"))
            .expect("Could not open log file.");

        let _ = OpenOptions::new()
            .append(true)
            .create(true)
            .open(format!("{log_dir}/{CHECK_DATA_LOG}"))
            .expect("Could not open log file.");

        // Get all the paths of the files that need to be processed.
        let mut paths = vec![];
        let paths = match collect_paths(data_dir.clone().into(), &mut paths) {
            Ok(v) => v,
            Err(e) => {
                error!(target: "import", "{e}");
                return;
            }
        };

        // Iterate through all paths, extacting the data from the files, transforming it into the
        // desired shape, and inserting it into the database.
        // Exactly how the data is processed depends on what `InputCount` it is.
        'paths_loop: for path in paths {
            // Don't try to process the log files.
            if path.extension().is_some_and(|x| x == "log") {
                continue;
            }
            let count_type = match InputCount::from_parent_dir(path) {
                Ok(v) => v,
                Err(e) => {
                    error!(target: "import", "{path:?} not processed: {e}");
                    cleanup(cleanup_files, path);
                    continue;
                }
            };

            let metadata = match FieldMetadata::from_path(path) {
                Ok(v) => v,
                Err(e) => {
                    error!(target: "import", "{path:?} not processed: {e}");
                    cleanup(cleanup_files, path);
                    continue;
                }
            };
            let recordnum = metadata.clone().recordnum;

            // Check that the count is already included in meta table in database - abort otherwise.
            if conn
                .query_row_as::<Option<String>>(
                    "select recordnum from tc_header where recordnum = :1",
                    &[&recordnum],
                )
                .is_err()
            {
                let msg = "Not processed: recordnum not found in TC_HEADER table";
                error!(
                    target: "import",
                    "{recordnum}: {msg}",
                );
                insert_import_log_entry(
                    &conn,
                    ImportLogEntry::new(recordnum, msg.to_string(), Level::Error),
                )
                .unwrap();
                cleanup(cleanup_files, path);
                continue;
            }

            // Process the file according to InputCount.
            let msg = format!("Extracting data from {path:?}, a {count_type:?} count");
            info!( target: "import", "{msg}" );
            insert_import_log_entry(&conn, ImportLogEntry::new(recordnum, msg, Level::Info))
                .unwrap();
            match count_type {
                InputCount::IndividualVehicle => {
                    // Extract data from CSV/text file.
                    let individual_vehicles = match IndividualVehicle::extract(path) {
                        Ok(v) => v,
                        Err(e) => {
                            let msg = format!("Not processed: {e}");
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                            cleanup(cleanup_files, path);
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
                    TimeBinnedVehicleClassCount::delete(&conn, recordnum).unwrap();
                    TimeBinnedSpeedRangeCount::delete(&conn, recordnum).unwrap();
                    NonNormalAvgSpeedCount::delete(&conn, recordnum).unwrap();
                    NonNormalVolCount::delete(&conn, recordnum).unwrap();

                    // Create prepared statements and use them to insert counts.
                    let mut prepared = TimeBinnedVehicleClassCount::prepare_insert(&conn).unwrap();
                    for count in vehicle_class_count {
                        match count.insert(&mut prepared) {
                            Ok(_) => (),
                            Err(e) => {
                                let msg = format!("Error inserting count {count:?}: {e}; further processing has been abandoned");
                                error!(target: "import", "{recordnum}: {msg}");
                                insert_import_log_entry(
                                    &conn,
                                    ImportLogEntry::new(recordnum, msg, Level::Error),
                                )
                                .unwrap();
                                continue 'paths_loop;
                            }
                        }
                    }
                    let table = <TimeBinnedVehicleClassCount as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            let msg = format!("Successfully committed class data insert to database ({table} table)");
                            info!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Info),
                            )
                            .unwrap();
                        }
                        Err(e) => {
                            let msg = format!("Error committing class data insert to database ({table} table): {e}");
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                        }
                    }

                    let mut prepared = TimeBinnedSpeedRangeCount::prepare_insert(&conn).unwrap();
                    for count in speed_range_count {
                        match count.insert(&mut prepared) {
                            Ok(_) => (),
                            Err(e) => {
                                let msg = format!("Error inserting count {count:?}: {e}; further processing has been abandoned");
                                error!(target: "import", "{recordnum}: {msg}");
                                insert_import_log_entry(
                                    &conn,
                                    ImportLogEntry::new(recordnum, msg, Level::Error),
                                )
                                .unwrap();
                                continue 'paths_loop;
                            }
                        }
                    }
                    let table = <TimeBinnedSpeedRangeCount as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            let msg = format!("Successfully committed speed range data insert to database ({table} table)");
                            info!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Info),
                            )
                            .unwrap();
                        }
                        Err(e) => {
                            let msg = format!("Error committing speed range data insert to database ({table} table): {e}");
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                        }
                    }

                    // Denormalize this data to insert into tc_volcount table.
                    let denormalized_volcount =
                        TimeBinnedVehicleClassCount::denormalize_vol_count(recordnum, &conn)
                            .unwrap();

                    // Create prepared statements and use them to insert counts.
                    let mut prepared = NonNormalVolCount::prepare_insert(&conn).unwrap();
                    for count in denormalized_volcount {
                        match count.insert(&mut prepared) {
                            Ok(_) => (),
                            Err(e) => {
                                let msg = format!("Error inserting count {count:?}: {e}; further processing has been abandoned");
                                error!(target: "import", "{recordnum}: {msg}");
                                insert_import_log_entry(
                                    &conn,
                                    ImportLogEntry::new(recordnum, msg, Level::Error),
                                )
                                .unwrap();
                                continue 'paths_loop;
                            }
                        }
                    }
                    let table = <NonNormalVolCount as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            let msg = format!("Successfully committed denormalized class data insert to database ({table} table)");
                            info!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Info),
                            )
                            .unwrap();
                        }
                        Err(e) => {
                            let msg = format!("Error committing denormalized class data insert to database ({table} table): {e}");
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                        }
                    }

                    let mut prepared = NonNormalAvgSpeedCount::prepare_insert(&conn).unwrap();
                    for count in non_normal_speedavg_count {
                        match count.insert(&mut prepared) {
                            Ok(_) => (),
                            Err(e) => {
                                let msg = format!("Error inserting count {count:?}: {e}; further processing has been abandoned");
                                error!(target: "import", "{recordnum}: {msg}");
                                insert_import_log_entry(
                                    &conn,
                                    ImportLogEntry::new(recordnum, msg, Level::Error),
                                )
                                .unwrap();
                                continue 'paths_loop;
                            }
                        }
                    }
                    let table = <NonNormalAvgSpeedCount as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            let msg = format!("Successfully committed denormalized speed data insert to database ({table} table)");
                            info!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Info),
                            )
                            .unwrap();
                        }
                        Err(e) => {
                            let msg = format!("Error committing denormalized speed data insert to database ({table} table): {e}");
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                        }
                    }

                    match update_metadata(recordnum, metadata, &conn) {
                        Ok(()) => {
                            let msg = "Metadata updated (tc_header table)";
                            info!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg.to_string(), Level::Info),
                            )
                            .unwrap();
                        }
                        Err(e) => {
                            let msg = format!("Error updating metadata (tc_header table): {e}");
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                        }
                    }
                }
                InputCount::FifteenMinuteVehicle => {
                    // Extract data from CSV/text file.
                    let fifteen_min_volcount = match FifteenMinuteVehicle::extract(path) {
                        Ok(v) => v,
                        Err(e) => {
                            let msg = format!("Not processed: {e}");
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                            cleanup(cleanup_files, path);
                            continue;
                        }
                    };

                    // As they are already binned by 15-minute period, these need no further
                    // processing; just insert into database.
                    FifteenMinuteVehicle::delete(&conn, recordnum).unwrap();
                    let mut prepared = FifteenMinuteVehicle::prepare_insert(&conn).unwrap();
                    for count in fifteen_min_volcount {
                        match count.insert(&mut prepared) {
                            Ok(_) => (),
                            Err(e) => {
                                let msg = format!("Error inserting count {count:?}: {e}; further processing has been abandoned");
                                error!(target: "import", "{recordnum}: {msg}");
                                insert_import_log_entry(
                                    &conn,
                                    ImportLogEntry::new(recordnum, msg, Level::Error),
                                )
                                .unwrap();
                                continue 'paths_loop;
                            }
                        }
                    }
                    let table = <FifteenMinuteVehicle as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => (),
                        Err(e) => {
                            let msg = format!(
                                "Error committing data insert to database ({table} table): {e}"
                            );
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                        }
                    }

                    // Denormalize this data to insert into tc_volcount table.
                    let denormalized_volcount =
                        FifteenMinuteVehicle::denormalize_vol_count(recordnum, &conn).unwrap();

                    // Delete existing records from db.
                    NonNormalVolCount::delete(&conn, recordnum).unwrap();

                    // Create prepared statements and use them to insert counts.
                    let mut prepared = NonNormalVolCount::prepare_insert(&conn).unwrap();
                    for count in denormalized_volcount {
                        match count.insert(&mut prepared) {
                            Ok(_) => (),
                            Err(e) => {
                                let msg = format!("Error inserting count {count:?}: {e}; further processing has been abandoned");
                                error!(target: "import", "{recordnum}: {msg}");
                                insert_import_log_entry(
                                    &conn,
                                    ImportLogEntry::new(recordnum, msg, Level::Error),
                                )
                                .unwrap();
                                continue 'paths_loop;
                            }
                        }
                    }
                    let table = <NonNormalVolCount as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            let msg = format!("Successfully committed denormalized data insert to database ({table} table)");
                            info!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Info),
                            )
                            .unwrap();
                        }
                        Err(e) => {
                            let msg = format!("Error committing denormalized data insert to database ({table} table): {e}");
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                        }
                    }

                    match update_metadata(recordnum, metadata, &conn) {
                        Ok(()) => {
                            let msg = "Metadata updated (tc_header table)";
                            info!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg.to_string(), Level::Info),
                            )
                            .unwrap();
                        }
                        Err(e) => {
                            let msg = format!("Error updating metadata (tc_header table): {e}");
                            error!(target: "import", "{recordnum}: {msg}"
                            );
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                        }
                    }
                }
                InputCount::FifteenMinuteBicycle => {
                    // Extract data from CSV/text file.
                    let fifteen_min_volcount = match FifteenMinuteBicycle::extract(path) {
                        Ok(v) => v,
                        Err(e) => {
                            let msg = format!("Not processed: {e}");
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                            cleanup(cleanup_files, path);
                            continue;
                        }
                    };

                    // As they are already binned by 15-minute period, these need no further
                    // processing; just insert into database.
                    FifteenMinuteBicycle::delete(&conn, recordnum).unwrap();
                    let mut prepared = FifteenMinuteBicycle::prepare_insert(&conn).unwrap();
                    for count in fifteen_min_volcount {
                        match count.insert(&mut prepared) {
                            Ok(_) => (),
                            Err(e) => {
                                let msg = format!("Error inserting count {count:?}: {e}; further processing has been abandoned");
                                error!(target: "import", "{recordnum}: {msg}");
                                insert_import_log_entry(
                                    &conn,
                                    ImportLogEntry::new(recordnum, msg, Level::Error),
                                )
                                .unwrap();
                                continue 'paths_loop;
                            }
                        }
                    }
                    let table = <FifteenMinuteBicycle as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            let msg = format!(
                                "Successfully committed data insert to database ({table} table)"
                            );
                            info!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Info),
                            )
                            .unwrap();
                        }
                        Err(e) => {
                            let msg = format!(
                                "Error committing data insert to database ({table} table): {e}"
                            );
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                        }
                    }

                    match update_metadata(recordnum, metadata, &conn) {
                        Ok(()) => {
                            let msg = "Metadata updated (tc_header table)";
                            info!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg.to_string(), Level::Info),
                            )
                            .unwrap();
                        }
                        Err(e) => {
                            let msg = format!("Error updating metadata (tc_header table): {e}");
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                        }
                    }
                }

                InputCount::FifteenMinutePedestrian => {
                    // Extract data from CSV/text file.
                    let fifteen_min_volcount = match FifteenMinutePedestrian::extract(path) {
                        Ok(v) => v,
                        Err(e) => {
                            let msg = format!("Not processed: {e}");
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                            cleanup(cleanup_files, path);
                            continue;
                        }
                    };

                    // As they are already binned by 15-minute period, these need no further
                    // processing; just insert into database.
                    FifteenMinutePedestrian::delete(&conn, recordnum).unwrap();
                    let mut prepared = FifteenMinutePedestrian::prepare_insert(&conn).unwrap();
                    for count in fifteen_min_volcount {
                        match count.insert(&mut prepared) {
                            Ok(_) => (),
                            Err(e) => {
                                let msg = format!("Error inserting count {count:?}: {e}");
                                error!(target: "import", "{recordnum}: {msg}");
                                insert_import_log_entry(
                                    &conn,
                                    ImportLogEntry::new(recordnum, msg, Level::Error),
                                )
                                .unwrap();
                                continue 'paths_loop;
                            }
                        }
                    }
                    let table = <FifteenMinutePedestrian as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            let msg = format!(
                                "Successfully committed data insert to database ({table} table)"
                            );
                            info!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Info),
                            )
                            .unwrap();
                        }
                        Err(e) => {
                            let msg = format!(
                                "Error committing data insert to database ({table} table): {e}"
                            );
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                        }
                    }

                    match update_metadata(recordnum, metadata, &conn) {
                        Ok(()) => {
                            let msg = "Metadata updated (tc_header table)";
                            info!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg.to_string(), Level::Info),
                            )
                            .unwrap();
                        }
                        Err(e) => {
                            let msg = format!("Error updating metadata (tc_header header): {e}");
                            error!(target: "import", "{recordnum}: {msg}");
                            insert_import_log_entry(
                                &conn,
                                ImportLogEntry::new(recordnum, msg, Level::Error),
                            )
                            .unwrap();
                        }
                    }
                }
            }

            // Calculate and insert the annual average daily volume, except for bicycle counts,
            // which first require an additional field in the database to be set after the import.
            if count_type != InputCount::FifteenMinuteBicycle
                && count_type != InputCount::IndividualBicycle
            {
                match insert_aadv2(recordnum as u32, &conn) {
                    Ok(()) => {
                        let msg = "AADV calculated and inserted";
                        info!(target: "import", "{recordnum}: {msg}");
                        insert_import_log_entry(
                            &conn,
                            ImportLogEntry::new(recordnum, msg.to_string(), Level::Info),
                        )
                        .unwrap();
                    }
                    Err(e) => {
                        let msg = format!("Failed to calculate/insert AADV: {e}");
                        error!(target: "import", "{recordnum}: {msg}");
                        insert_import_log_entry(
                            &conn,
                            ImportLogEntry::new(recordnum, msg, Level::Error),
                        )
                        .unwrap();
                    }
                }
            }
            // Check for potential issues with data, after it has been inserted into the database,
            // and log them for review.
            let msg = "Checking data";
            info!(target: "import", "{recordnum}: {msg}");
            insert_import_log_entry(
                &conn,
                ImportLogEntry::new(recordnum, msg.to_string(), Level::Info),
            )
            .unwrap();
            if let Err(e) = check(recordnum, &conn) {
                let msg = format!("An error occurred while checking data: {e}; warnings likely to be incomplete or incorrect.");
                error!(target: "import", "{recordnum}: {msg}");
                insert_import_log_entry(&conn, ImportLogEntry::new(recordnum, msg, Level::Error))
                    .unwrap();
            }
            cleanup(cleanup_files, path);
        }
        // Wait to try again
        thread::sleep(time::Duration::from_secs(TIME_BETWEEN_LOOPS));
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

fn cleanup(cleanup_files: bool, path: &PathBuf) {
    if cleanup_files {
        if let Err(e) = fs::remove_file(path) {
            error!("Unable to delete file {path:?} {e}");
        }
    }
}

fn update_metadata(
    recordnum: u32,
    metadata: FieldMetadata,
    conn: &Connection,
) -> Result<(), oracle::Error> {
    conn.execute(
        "update tc_header SET
        importdatadate = (select current_date from dual),
        status = :1,
        counterid = :2,
        speedlimit = :3
        where recordnum = :4",
        &[
            &"imported",
            &metadata.counter_id,
            &metadata.speed_limit,
            &recordnum,
        ],
    )?;
    conn.commit()
}

//! Import traffic counts to our database from files.
//! This program watches a directory for files to be uploaded to one of the following subdirectories:
//!   - vehicle/ - for raw, unbinned records of [individual vehicles][IndividualVehicle] containing vehicle class and speed, from STARneXt/JAMAR
//!   - bicycle/ - for raw, unbinned records of [individual bicycles][IndividualBicycle] containing bicycle counts, from STARneXt/JAMAR
//!   - 15minutevehicle/ - for [pre-binned, 15-minute volume counts][FifteenMinuteVehicle] from STARneXt/JAMAR
//!   - 15minutebicycle/ - for [pre-binned, 15-minute bicycle counts][FifteenMinuteBicycle] from
//!     Eco-Counter
//!   - 15minutepedestrian/ - for [pre-binned, 15-minute pedestrian counts][FifteenMinutePedestrian]
//!     from Eco-Counter
//!
//! When a file is found, the program verifies that it contains the correct/expected kind of data,
//! derives the appropriate counts from it, and then inserts these into our database and removes
//! the file.
//!
//! A [log][`LOG`] of the program's work is kept in the main directory.
//! The program is able to log most errors and continue its execution,
//! so that an error in one file will not prevent it from successfully processing another.
//! The program itself should only fail if it is misconfigured, meaning that,
//! once started successfully, it should run indefinitely.
//!
//! ## Exported Filename
//!
//! Give the file the name of the recordnum, appended with .csv or .txt, e.g. 166905.csv.
//!
//! ## Exporting from STARneXt
//!
//! To begin, open the STARneXt app from JAMAR and then open a .snj or .tf2 file. From there, it
//! depends on what kind of count you are processing:
//!
//! ### 1. Class/speed counts (with or without bicycles)
//!
//!   - if bicycles are included in the count, use the "Modified Scheme F - with bikes" scheme
//!   - if bicycles are *not* included, use the "Modified Scheme F" scheme
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
//!   **NOTE**: The steps above will need to be followed twice for those counts with *both*
//!   motorized vehicles and bicycles -- once with the recordnum (and scheme) for the Bicycle 5
//!   type and once with the recordnum (and scheme) for the Class type.
//!
//! ### 2. 15-minute volume counts
//!
//!   - click the **Export** button from the top menu, selecting *ASCII (CSV)*
//!     as the format.
//!   - use the following settings on the dialog that pops up (should be default):
//!     - *Include Start Date*
//!     - *Include Start Time*
//!     - *Include Interval Number*
//!     - *Include Interval Time*
//!     - *Include unclassed first*
//!     - *Export Separate*
//!     - Comma as delimiter
//!   - click **Save** to save the file locally.
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
use std::path::{Path, PathBuf};
use std::thread;
use std::time;

use log::{error, Level, LevelFilter};
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};

use traffic_counts::{
    check_data::check,
    create_binned_bicycle_vol_count, create_speed_and_class_count,
    db::{self, crud::Crud},
    denormalize::{Denormalize, *},
    extract_from_file::{Extract, InputCount},
    log_msg, CountError, Directions, FifteenMinuteBicycle, FifteenMinutePedestrian,
    FifteenMinuteVehicle, FileNameProblem, IndividualBicycle, IndividualVehicle,
    TimeBinnedSpeedRangeCount, TimeBinnedVehicleClassCount, TimeInterval,
};

const LOG: &str = "import.log";
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
    let import_config = ConfigBuilder::new().set_time_format_rfc3339().build();
    let import_log = CombinedLogger::new(vec![
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
    ]);

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
    let pool = db::create_pool(username, password).unwrap();
    let conn = pool.get().unwrap();

    loop {
        // Recreate the logs in case they somehow get deleted.
        let _ = OpenOptions::new()
            .append(true)
            .create(true)
            .open(format!("{log_dir}/{LOG}"))
            .expect("Could not open log file.");

        // Get all the paths of the files that need to be processed.
        let mut paths = vec![];
        let paths = match collect_paths(data_dir.clone().into(), &mut paths) {
            Ok(v) => v,
            Err(e) => {
                error!("{e}");
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
                    error!("{path:?} not processed: {e}");
                    cleanup(cleanup_files, path);
                    continue;
                }
            };

            let recordnum = match get_recordnum(path) {
                Ok(v) => v,
                Err(e) => {
                    error!("{path:?} not processed: {e}");
                    cleanup(cleanup_files, path);
                    continue;
                }
            };

            // Check that the count is already included in meta table in database - abort otherwise.
            if conn
                .query_row_as::<Option<String>>(
                    "select recordnum from tc_header where recordnum = :1",
                    &[&recordnum],
                )
                .is_err()
            {
                log_msg(
                    recordnum,
                    &import_log,
                    Level::Error,
                    "Not processed: recordnum not found in TC_HEADER table",
                    &conn,
                );
                cleanup(cleanup_files, path);
                continue;
            }

            // Get all the lane directions of a count.
            let directions = match Directions::from_db(recordnum, &conn) {
                Ok(v) => v,
                Err(e) => {
                    error!("{path:?} not processed: {e}");
                    cleanup(cleanup_files, path);
                    continue;
                }
            };

            // Process the file according to InputCount.
            log_msg(
                recordnum,
                &import_log,
                Level::Info,
                &format!("Extracting data from {path:?}, a {count_type:?} count"),
                &conn,
            );
            match count_type {
                InputCount::IndividualVehicle => {
                    // Extract data from CSV/text file.
                    let individual_vehicles =
                        match IndividualVehicle::extract(path, recordnum, &directions) {
                            Ok(v) => v,
                            Err(e) => {
                                log_msg(
                                    recordnum,
                                    &import_log,
                                    Level::Error,
                                    &format!("Not processed: {e}"),
                                    &conn,
                                );
                                cleanup(cleanup_files, path);
                                continue;
                            }
                        };

                    // Create two counts from this: 15-minute speed count and 15-minute class count
                    let (speed_range_count, vehicle_class_count) = create_speed_and_class_count(
                        TimeInterval::FifteenMin,
                        recordnum,
                        &directions,
                        individual_vehicles.clone(),
                    );

                    // Create records for the non-normalized TC_SPESUM table (another one with
                    // specific hourly fields, this time for average speed/hour).
                    let non_normal_speedavg_count = create_non_normal_speedavg_count(
                        recordnum,
                        directions,
                        individual_vehicles,
                    );

                    // Delete existing records from db.
                    TimeBinnedVehicleClassCount::delete(&conn, recordnum).unwrap();
                    TimeBinnedSpeedRangeCount::delete(&conn, recordnum).unwrap();
                    NonNormalAvgSpeedCount::delete(&conn, recordnum).unwrap();
                    NonNormalVolCount::delete(&conn, recordnum).unwrap();

                    // Create prepared statements and use them to insert counts.
                    let mut prepared = TimeBinnedVehicleClassCount::prepare_insert(&conn).unwrap();
                    for count in vehicle_class_count {
                        if let Err(e) = count.insert(&mut prepared) {
                            log_msg(
                                recordnum,
                                &import_log,
                                Level::Error,
                                &format!("Error inserting count {count:?}: {e}; further processing has been abandoned"),
                                &conn,
                            );
                            cleanup(cleanup_files, path);
                            continue 'paths_loop;
                        }
                    }
                    let table = <TimeBinnedVehicleClassCount as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            log_msg(
                                recordnum, &import_log, Level::Info, &format!("Successfully committed class data insert to database ({table} table)"), &conn);
                        }
                        Err(e) => {
                            log_msg(recordnum, &import_log, Level::Error, &format!("Error committing class data insert to database ({table} table): {e}"), &conn);
                            cleanup(cleanup_files, path);
                            continue;
                        }
                    }

                    let mut prepared = TimeBinnedSpeedRangeCount::prepare_insert(&conn).unwrap();
                    for count in speed_range_count {
                        if let Err(e) = count.insert(&mut prepared) {
                            log_msg(recordnum, &import_log, Level::Error, &format!("Error inserting count {count:?}: {e}; further processing has been abandoned"), &conn);
                            cleanup(cleanup_files, path);
                            continue 'paths_loop;
                        }
                    }
                    let table = <TimeBinnedSpeedRangeCount as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            log_msg(recordnum, &import_log, Level::Info, &format!("Successfully committed speed range data insert to database ({table} table)"), &conn);
                        }
                        Err(e) => {
                            log_msg(recordnum, &import_log, Level::Error, &format!("Error committing speed range data insert to database ({table} table): {e}"), &conn);
                            cleanup(cleanup_files, path);
                            continue;
                        }
                    }

                    // Denormalize this data to insert into tc_volcount table.
                    let denormalized_volcount =
                        TimeBinnedVehicleClassCount::denormalize_vol_count(recordnum, &conn)
                            .unwrap();

                    // Create prepared statements and use them to insert counts.
                    let mut prepared = NonNormalVolCount::prepare_insert(&conn).unwrap();
                    for count in denormalized_volcount {
                        if let Err(e) = count.insert(&mut prepared) {
                            log_msg(recordnum, &import_log, Level::Error, &format!("Error inserting count {count:?}: {e}; further processing has been abandoned"), &conn);
                            cleanup(cleanup_files, path);
                            continue 'paths_loop;
                        }
                    }
                    let table = <NonNormalVolCount as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            log_msg(recordnum, &import_log, Level::Info, &format!("Successfully committed denormalized class data insert to database ({table} table)"), &conn);
                        }
                        Err(e) => {
                            log_msg(recordnum, &import_log, Level::Error, &format!("Error committing denormalized class data insert to database ({table} table): {e}"), &conn);

                            cleanup(cleanup_files, path);
                            continue;
                        }
                    }

                    let mut prepared = NonNormalAvgSpeedCount::prepare_insert(&conn).unwrap();
                    for count in non_normal_speedavg_count {
                        if let Err(e) = count.insert(&mut prepared) {
                            log_msg(recordnum, &import_log, Level::Error, &format!("Error inserting count {count:?}: {e}; further processing has been abandoned"), &conn);
                            cleanup(cleanup_files, path);
                            continue 'paths_loop;
                        }
                    }
                    let table = <NonNormalAvgSpeedCount as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            log_msg(recordnum, &import_log, Level::Info, &format!("Successfully committed denormalized speed data insert to database ({table} table)"), &conn);
                        }
                        Err(e) => {
                            log_msg(recordnum, &import_log, Level::Error, &format!("Error committing denormalized speed data insert to database ({table} table): {e}"), &conn);
                            cleanup(cleanup_files, path);
                            continue;
                        }
                    }
                }
                InputCount::IndividualBicycle => {
                    // Extract data from CSV/text file.
                    let counts = match IndividualBicycle::extract(path, recordnum, &directions) {
                        Ok(v) => v,
                        Err(e) => {
                            log_msg(
                                recordnum,
                                &import_log,
                                Level::Error,
                                &format!("Not processed: {e}"),
                                &conn,
                            );
                            cleanup(cleanup_files, path);
                            continue;
                        }
                    };

                    // Create aggregated 15-minute bicycle count from this.
                    let fifteen_min_volcount = create_binned_bicycle_vol_count(
                        TimeInterval::FifteenMin,
                        recordnum,
                        counts,
                    );

                    // Delete existing records from db.
                    FifteenMinuteBicycle::delete(&conn, recordnum).unwrap();

                    // Create prepared statements and use them to insert counts.
                    let mut prepared = FifteenMinuteBicycle::prepare_insert(&conn).unwrap();
                    for count in fifteen_min_volcount {
                        if let Err(e) = count.insert(&mut prepared) {
                            log_msg(recordnum,  &import_log, Level::Error, &format!("Error inserting count {count:?}: {e}; further processing has been abandoned"), &conn);
                            cleanup(cleanup_files, path);
                            continue 'paths_loop;
                        }
                    }
                    let table = <FifteenMinuteBicycle as Crud>::COUNT_TABLE;

                    match conn.commit() {
                        Ok(()) => {
                            log_msg(
                                recordnum,
                                &import_log,
                                Level::Info,
                                &format!(
                                "Successfully committed data insert to database ({table} table)"
                            ),
                                &conn,
                            );
                        }
                        Err(e) => {
                            log_msg(
                                recordnum,
                                &import_log,
                                Level::Error,
                                &format!(
                                    "Error committing data insert to database ({table} table): {e}"
                                ),
                                &conn,
                            );
                            cleanup(cleanup_files, path);
                            continue;
                        }
                    }
                }
                InputCount::FifteenMinuteVehicle => {
                    // Extract data from CSV/text file.
                    let fifteen_min_volcount =
                        match FifteenMinuteVehicle::extract(path, recordnum, &directions) {
                            Ok(v) => v,
                            Err(e) => {
                                log_msg(
                                    recordnum,
                                    &import_log,
                                    Level::Error,
                                    &format!("Not processed: {e}"),
                                    &conn,
                                );
                                cleanup(cleanup_files, path);
                                continue;
                            }
                        };

                    // As they are already binned by 15-minute period, these need no further
                    // processing; just insert into database.
                    FifteenMinuteVehicle::delete(&conn, recordnum).unwrap();
                    let mut prepared = FifteenMinuteVehicle::prepare_insert(&conn).unwrap();
                    for count in fifteen_min_volcount {
                        if let Err(e) = count.insert(&mut prepared) {
                            log_msg(recordnum,  &import_log, Level::Error, &format!("Error inserting count {count:?}: {e}; further processing has been abandoned"), &conn);
                            cleanup(cleanup_files, path);
                            continue 'paths_loop;
                        }
                    }
                    let table = <FifteenMinuteVehicle as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            log_msg(
                                recordnum,
                                &import_log,
                                Level::Info,
                                &format!(
                                "Successfully committed data insert to database ({table} table)"
                            ),
                                &conn,
                            );
                        }
                        Err(e) => {
                            log_msg(
                                recordnum,
                                &import_log,
                                Level::Error,
                                &format!(
                                    "Error committing data insert to database ({table} table): {e}"
                                ),
                                &conn,
                            );
                            cleanup(cleanup_files, path);
                            continue;
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
                        if let Err(e) = count.insert(&mut prepared) {
                            log_msg(recordnum, &import_log, Level::Error, &format!("Error inserting count {count:?}: {e}; further processing has been abandoned"), &conn);
                            cleanup(cleanup_files, path);
                            continue 'paths_loop;
                        }
                    }
                    let table = <NonNormalVolCount as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            log_msg(recordnum, &import_log, Level::Info, &format!("Successfully committed denormalized data insert to database ({table} table)"), &conn);
                        }
                        Err(e) => {
                            log_msg(recordnum, &import_log, Level::Error,&format!("Error committing denormalized data insert to database ({table} table): {e}"), &conn);
                            cleanup(cleanup_files, path);
                            continue;
                        }
                    }
                }
                InputCount::FifteenMinuteBicycle => {
                    // Extract data from CSV/text file.
                    let fifteen_min_volcount =
                        match FifteenMinuteBicycle::extract(path, recordnum, &directions) {
                            Ok(v) => v,
                            Err(e) => {
                                log_msg(
                                    recordnum,
                                    &import_log,
                                    Level::Error,
                                    &format!("Not processed: {e}"),
                                    &conn,
                                );
                                cleanup(cleanup_files, path);
                                continue;
                            }
                        };

                    // As they are already binned by 15-minute period, these need no further
                    // processing; just insert into database.
                    FifteenMinuteBicycle::delete(&conn, recordnum).unwrap();
                    let mut prepared = FifteenMinuteBicycle::prepare_insert(&conn).unwrap();
                    for count in fifteen_min_volcount {
                        if let Err(e) = count.insert(&mut prepared) {
                            log_msg(recordnum, &import_log, Level::Error, &format!("Error inserting count {count:?}: {e}; further processing has been abandoned"), &conn);
                            cleanup(cleanup_files, path);
                            continue 'paths_loop;
                        }
                    }
                    let table = <FifteenMinuteBicycle as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            log_msg(
                                recordnum,
                                &import_log,
                                Level::Info,
                                &format!(
                                "Successfully committed data insert to database ({table} table)"
                            ),
                                &conn,
                            );
                        }
                        Err(e) => {
                            log_msg(
                                recordnum,
                                &import_log,
                                Level::Error,
                                &format!(
                                    "Error committing data insert to database ({table} table): {e}"
                                ),
                                &conn,
                            );
                            cleanup(cleanup_files, path);
                            continue;
                        }
                    }
                }
                InputCount::FifteenMinutePedestrian => {
                    // Extract data from CSV/text file.
                    let fifteen_min_volcount =
                        match FifteenMinutePedestrian::extract(path, recordnum, &directions) {
                            Ok(v) => v,
                            Err(e) => {
                                log_msg(
                                    recordnum,
                                    &import_log,
                                    Level::Error,
                                    &format!("Not processed: {e}"),
                                    &conn,
                                );
                                cleanup(cleanup_files, path);
                                continue;
                            }
                        };

                    // As they are already binned by 15-minute period, these need no further
                    // processing; just insert into database.
                    FifteenMinutePedestrian::delete(&conn, recordnum).unwrap();
                    let mut prepared = FifteenMinutePedestrian::prepare_insert(&conn).unwrap();
                    for count in fifteen_min_volcount {
                        if let Err(e) = count.insert(&mut prepared) {
                            log_msg(
                                recordnum,
                                &import_log,
                                Level::Error,
                                &format!("Error inserting count {count:?}: {e}"),
                                &conn,
                            );
                            cleanup(cleanup_files, path);
                            continue 'paths_loop;
                        }
                    }
                    let table = <FifteenMinutePedestrian as Crud>::COUNT_TABLE;
                    match conn.commit() {
                        Ok(()) => {
                            log_msg(
                                recordnum,
                                &import_log,
                                Level::Info,
                                &format!(
                                "Successfully committed data insert to database ({table} table)"
                            ),
                                &conn,
                            );
                        }
                        Err(e) => {
                            log_msg(
                                recordnum,
                                &import_log,
                                Level::Error,
                                &format!(
                                    "Error committing data insert to database ({table} table): {e}"
                                ),
                                &conn,
                            );
                            cleanup(cleanup_files, path);
                            continue;
                        }
                    }
                }
            }

            // Update metadata table in db.
            if let Err(e) = conn.execute(
                "update tc_header SET
                importdatadate = (select current_date from dual),
                status = :1,
                where recordnum = :2",
                &[&"imported", &recordnum],
            ) {
                log_msg(
                    recordnum,
                    &import_log,
                    Level::Error,
                    &format!("Error updating metadata (tc_header table): {e}"),
                    &conn,
                );
            };

            match conn.commit() {
                Ok(()) => log_msg(
                    recordnum,
                    &import_log,
                    Level::Info,
                    "Metadata updated (tc_header table)",
                    &conn,
                ),
                Err(e) => {
                    log_msg(
                        recordnum,
                        &import_log,
                        Level::Error,
                        &format!("Error updating metadata (tc_header table): {e}"),
                        &conn,
                    );
                }
            };

            // Update the intermediate table used for calculating AADV in all cases.
            match db::update_intermediate_aadv(recordnum as u32, &conn) {
                Ok(_) => {
                    log_msg(
                        recordnum,
                        &import_log,
                        Level::Info,
                        "Intermediate table TC_COUNTDATE updated",
                        &conn,
                    );
                }
                Err(e) => {
                    log_msg(
                        recordnum,
                        &import_log,
                        Level::Error,
                        &format!("Failed to update intermediate table TC_COUNTDATE: {e}"),
                        &conn,
                    );
                }
            }

            // Update setdate.
            match db::update_setdate(recordnum as u32, &conn) {
                Ok(_) => {
                    log_msg(
                        recordnum,
                        &import_log,
                        Level::Info,
                        "Field SETDATE updated",
                        &conn,
                    );
                }
                Err(e) => {
                    log_msg(
                        recordnum,
                        &import_log,
                        Level::Error,
                        &format!("Failed to update field SETDATE: {e}"),
                        &conn,
                    );
                }
            }

            // Calculate and insert the annual average daily volume, except for bicycle counts,
            // which first require an additional field in the database to be set after the import.
            if count_type != InputCount::FifteenMinuteBicycle
                && count_type != InputCount::IndividualBicycle
            {
                match db::calc_aadv(recordnum as u32, &conn) {
                    Ok(_) => {
                        log_msg(
                            recordnum,
                            &import_log,
                            Level::Info,
                            "AADV calculated and inserted",
                            &conn,
                        );
                    }
                    Err(e) => {
                        log_msg(
                            recordnum,
                            &import_log,
                            Level::Error,
                            &format!("Failed to calculate/insert AADV: {e}"),
                            &conn,
                        );
                    }
                }
            }

            // Check for potential issues with data, after it has been inserted into the database,
            // and log them for review.
            log_msg(recordnum, &import_log, Level::Info, "Checking data", &conn);

            if let Err(e) = check(recordnum, &conn) {
                log_msg(recordnum,  &import_log, Level::Error, &format!("An error occurred while checking data: {e}; warnings likely to be incomplete or incorrect."), &conn);
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

fn get_recordnum(path: &Path) -> Result<u32, CountError> {
    let recordnum = path
        .file_stem()
        .ok_or(CountError::BadPath(path.to_owned()))?
        .to_str()
        .ok_or(CountError::BadPath(path.to_owned()))?;

    match recordnum.parse() {
        Ok(v) => Ok(v),
        Err(_) => Err(CountError::InvalidFileName {
            problem: FileNameProblem::InvalidRecordNum,
            path: path.to_owned(),
        }),
    }
}

//! Insert or update factors in TC_FACTOR table in DVRPC's traffic count database.
//!
//! NOTE: this can be run with `cargo run --bin upsert_factors [filename]`

use std::collections::BTreeMap;
use std::env;
use std::fs::{File, OpenOptions};

use log::{error, LevelFilter};
use oracle::{self, sql_type::ToSql};
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};

use traffic_counts::{db::create_pool, extract_from_file::create_reader};

const LOG: &str = "upsert_factor.log";

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

    let args: Vec<String> = env::args().collect();
    let data_file = if args.is_empty() {
        error!("Filename of CSV not supplied - aborting.");
        return;
    } else {
        args[1].clone()
    };

    let data_file = match File::open(data_file) {
        Ok(v) => v,
        Err(e) => {
            error!("{e}");
            return;
        }
    };
    let mut rdr = create_reader(&data_file);

    let mut iter = rdr.records();

    // Get header to determine which fields we're dealing with.
    let header = if let Some(header) = iter.next() {
        match header {
            Ok(v) => v,
            Err(e) => {
                error!("{e}");
                return;
            }
        }
    } else {
        error!("No header found in file.");
        return;
    };

    let header: Vec<&str> = match header.deserialize(None) {
        Ok(v) => v,
        Err(e) => {
            error!("{e}");
            return;
        }
    };

    let header = header
        .iter()
        .map(|each| each.to_lowercase())
        .collect::<Vec<String>>();

    // Check that header has all required fields.
    if !header.contains(&"year".to_string())
        && !header.contains(&"month".to_string())
        && !header.contains(&"fc".to_string())
        && !header.contains(&"dayofweek".to_string())
    {
        error!(
            "Missing header fields: header must contain 'year', 'month', 'fc', and 'dayofweek'."
        );
        return;
    }

    let year = header.iter().position(|x| x == "year").unwrap();
    let month = header.iter().position(|x| x == "month").unwrap();
    let fc = header.iter().position(|x| x == "fc").unwrap();
    let dayofweek = header.iter().position(|x| x == "dayofweek").unwrap();

    // Set up data structure for parameters we'll eventually use in the insert.
    // The fieldname and its column number can be determined from the header; when we
    // iterate through the rows, we'll get the value.
    // Then we'll use this to create a new structure that matches the expected signature of
    // `execute`.
    let mut params_plus_col_no: BTreeMap<&str, (usize, Option<f32>)> = BTreeMap::new();

    if let Some(v) = header.iter().position(|x| x == "njfactor") {
        params_plus_col_no.insert("njfactor", (v, None));
    }
    if let Some(v) = header.iter().position(|x| x == "njaxle") {
        params_plus_col_no.insert("njaxle", (v, None));
    }
    if let Some(v) = header.iter().position(|x| x == "pafactor") {
        params_plus_col_no.insert("pafactor", (v, None));
    }
    if let Some(v) = header.iter().position(|x| x == "paaxle") {
        params_plus_col_no.insert("paaxle", (v, None));
    }
    if let Some(v) = header.iter().position(|x| x == "nj_region4_factor") {
        params_plus_col_no.insert("nj_region4_factor", (v, None));
    }
    if let Some(v) = header.iter().position(|x| x == "nj_region4_axle") {
        params_plus_col_no.insert("nj_region4_axle", (v, None));
    }

    // Build set clause
    let mut set_clause = String::from("SET ");
    for (i, (col_name, _)) in params_plus_col_no.iter().enumerate() {
        set_clause.push_str(&format!("{col_name} = :{col_name}"));
        // separate each column/value with comma if not last one
        if i != params_plus_col_no.len() - 1 {
            set_clause.push_str(", ");
        }
    }

    // dbg!(&set_clause);

    // TODO:
    // Created prepared statement.

    // TODO: upsert instead of update
    // Iterate through data rows and insert into table.
    for row in rdr.records().take(5) {
        let row = row.unwrap();

        dbg!(&row[3]);

        let mut params: Vec<(&str, &dyn ToSql)> = vec![];

        // TODO: handle possible errors
        // Populate the value from each row into params
        for (fieldname, (col_no, value)) in params_plus_col_no.iter_mut() {
            *value = Some(row.get(*col_no).unwrap().parse::<f32>().unwrap());
            params.push((*fieldname, value));
        }

        let year = row[year].to_string();
        let month = row[month].to_string();
        let fc = row[fc].to_string();
        let dayofweek = row[dayofweek].to_string();
        params.push(("year", &year));
        params.push(("month", &month));
        params.push(("fc", &fc));
        params.push(("dayofweek", &dayofweek));

        let sql = format!("update TC_FACTOR {set_clause} where year = :year and month = :month and fc = :fc and dayofweek = :dayofweek");
        dbg!(&sql);
        conn.execute_named(&sql, params.as_slice()).unwrap();
    }
    conn.commit().unwrap();
}

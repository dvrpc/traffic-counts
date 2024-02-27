//! Extract various kinds of counts from files.
use std::fs::{self, File};
use std::path::Path;

use csv::{Reader, ReaderBuilder};
use log::error;
use time::{macros::format_description, Date, Time};

use crate::{
    CountError, CountMetadata, CountedVehicle, FifteenMinuteVehicle, FIFTEEN_MINUTE_VEHICLE_HEADER,
    INDIVIDUAL_VEHICLE_HEADER,
};

pub trait Extract {
    type Item;
    fn extract(path: &Path) -> Result<Vec<Self::Item>, CountError>;
}

/// Extract FifteenMinuteVehicle records from a file.
impl Extract for FifteenMinuteVehicle {
    type Item = FifteenMinuteVehicle;

    fn extract(path: &Path) -> Result<Vec<Self::Item>, CountError> {
        let data_file = File::open(path)?;
        let mut rdr = create_reader(&data_file);
        let directions = CountMetadata::from_path(path)?.directions;

        // Iterate through data rows (skipping metadata rows + 1 for header).
        let mut counts = vec![];
        for row in rdr.records().skip(num_metadata_rows_in_file(path)?) {
            // Parse date.
            let date_format = format_description!("[month padding:none]/[day padding:none]/[year]");
            let date_col = &row.as_ref().unwrap()[1];
            let count_date = Date::parse(date_col, &date_format).unwrap();

            // Parse time.
            let time_format = format_description!("[hour padding:none repr:12]:[minute] [period]");
            let time_col = &row.as_ref().unwrap()[2];
            let count_time = Time::parse(time_col, &time_format).unwrap();

            // There will always be at least one count per row.
            // Extract the first (and perhaps only) direction.
            match FifteenMinuteVehicle::new(
                count_date,
                count_time,
                row.as_ref().unwrap()[3].parse().unwrap(),
                directions.direction1,
            ) {
                Ok(v) => counts.push(v),
                Err(e) => {
                    error!("{e}");
                    continue;
                }
            };

            // There may also be a second count within the row.
            if let Some(v) = directions.direction2 {
                match FifteenMinuteVehicle::new(
                    count_date,
                    count_time,
                    row.as_ref().unwrap()[4].parse().unwrap(),
                    v,
                ) {
                    Ok(v) => counts.push(v),
                    Err(e) => {
                        error!("{e}");
                        continue;
                    }
                };
            }
        }
        Ok(counts)
    }
}

/// Extract CountedVehicle records from a file.
impl Extract for CountedVehicle {
    type Item = CountedVehicle;

    fn extract(path: &Path) -> Result<Vec<Self::Item>, CountError> {
        let data_file = File::open(path)?;
        let mut rdr = create_reader(&data_file);

        // Iterate through data rows (skipping metadata rows + 1 for header).
        let mut counts = vec![];
        for row in rdr.records().skip(num_metadata_rows_in_file(path)?) {
            // Parse date.
            let date_format = format_description!("[month padding:none]/[day padding:none]/[year]");
            let date_col = &row.as_ref().unwrap()[1];
            let count_date = Date::parse(date_col, &date_format).unwrap();

            // Parse time.
            let time_format =
                format_description!("[hour padding:none repr:12]:[minute]:[second] [period]");
            let time_col = &row.as_ref().unwrap()[2];
            let count_time = Time::parse(time_col, &time_format).unwrap();

            let count = match CountedVehicle::new(
                count_date,
                count_time,
                row.as_ref().unwrap()[3].parse().unwrap(),
                row.as_ref().unwrap()[4].parse().unwrap(),
                row.as_ref().unwrap()[5].parse().unwrap(),
            ) {
                Ok(v) => v,
                Err(e) => {
                    error!("{e}");
                    continue;
                }
            };

            counts.push(count);
        }
        Ok(counts)
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

/// Count metadata rows in file (those before one of the possible headers).
fn num_metadata_rows_in_file(path: &Path) -> Result<usize, CountError> {
    let contents = fs::read_to_string(path)?;
    let mut count = 0;
    for line in contents.lines().take(50) {
        count += 1;
        // Remove double quotes & spaces to avoid ambiguity in how headers are exported.
        let line = line.replace(['"', ' '], "");

        if line.contains(FIFTEEN_MINUTE_VEHICLE_HEADER) || line.contains(INDIVIDUAL_VEHICLE_HEADER)
        {
            return Ok(count);
        }
    }
    Err(CountError::BadHeader(path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_counted_vehicle_gets_correct_number_of_counts() {
        let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
        let counted_vehicles = CountedVehicle::extract(path).unwrap();
        assert_eq!(counted_vehicles.len(), 8706);
    }

    #[test]
    fn extract_fifteen_min_vehicle_gets_correct_number_of_counts() {
        let path = Path::new("test_files/15minutevehicle/rc-168193-ew-39352-na.txt");
        let fifteen_min_volcount = FifteenMinuteVehicle::extract(path).unwrap();
        assert_eq!(fifteen_min_volcount.len(), 384)
    }
}

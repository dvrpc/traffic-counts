//! Extract various kinds of counts from files.
use std::{fs::File, path::Path};

use csv::{Reader, ReaderBuilder};
use log::error;
use time::{macros::format_description, Date, Time};

use crate::{CountError, CountMetadata, CountType, CountedVehicle, FifteenMinuteVehicle};

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
        for row in rdr
            .records()
            .skip(num_metadata_rows_to_skip(CountType::FifteenMinuteVehicle) + 1)
        {
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
        for row in rdr
            .records()
            .skip(num_metadata_rows_to_skip(CountType::IndividualVehicle) + 1)
        {
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

fn count_type_from_location(path: &Path) -> Result<CountType, CountError> {
    // Get the directory immediately above the file.
    let parent = path
        .parent()
        .ok_or(CountError::BadPath(path))?
        .components()
        .last()
        .ok_or(CountError::BadPath(path))?
        .as_os_str()
        .to_str()
        .ok_or(CountError::BadPath(path))?;

    match parent.to_lowercase().as_str() {
        "15minutebicycle" => Ok(CountType::FifteenMinuteBicycle),
        "15minutepedestrian" => Ok(CountType::FifteenMinutePedestrian),
        "15minutevehicle" => Ok(CountType::FifteenMinuteVehicle),
        "vehicle" => Ok(CountType::IndividualVehicle),
        _ => Err(CountError::BadLocation(format!("{path:?}"))),
    }
}

fn count_type_from_header(
    path: &Path,
    location_count_type: CountType,
) -> Result<CountType, CountError> {
    // `location_count_type` is what we expect this file to be, based off its location. We use
    // this because different types of counts can have a variable number of metadata rows.

    const INDIVIDUAL_VEHICLE_COUNT_HEADER: &str = "Veh. No.,Date,Time,Channel,Class,Speed";
    const FIFTEEN_MINUTE_VEHICLE_COUNT_HEADER1: &str = "Number,Date,Time,Channel 1";
    const FIFTEEN_MINUTE_VEHICLE_COUNT_HEADER2: &str = "Number,Date,Time,Channel 1,Channel 2";
    const FIFTEEN_MINUTE_BICYCLE_COUNT_HEADER: &str = "";
    const FIFTEEN_MINUTE_PEDESTRIAN_COUNT_HEADER: &str = "";

    let file = File::open(path)?;
    let mut rdr = create_reader(&file);
    let header = rdr
        .records()
        .skip(num_metadata_rows_to_skip(location_count_type))
        .take(1)
        .last()
        .ok_or(CountError::MissingHeader(path))?
        .map_err(CountError::HeadertoStringRecordError)?
        .iter()
        .map(|x| x.trim().to_string())
        .collect::<Vec<String>>()
        .join(",");

    match header.as_str() {
        v if v == INDIVIDUAL_VEHICLE_COUNT_HEADER => Ok(CountType::IndividualVehicle),
        v if v == FIFTEEN_MINUTE_VEHICLE_COUNT_HEADER1 => Ok(CountType::FifteenMinuteVehicle),
        v if v == FIFTEEN_MINUTE_VEHICLE_COUNT_HEADER2 => Ok(CountType::FifteenMinuteVehicle),
        _ => Err(CountError::BadHeader(path)),
    }
}

pub fn get_count_type(path: &Path) -> Result<CountType, CountError> {
    let count_type_from_location = count_type_from_location(path)?;
    let count_type_from_header = count_type_from_header(path, count_type_from_location)?;
    if count_type_from_location != count_type_from_header {
        return Err(CountError::LocationHeaderMisMatch(path));
    }
    Ok(count_type_from_location)
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
        CountType::IndividualVehicle => 3,
        CountType::FifteenMinuteVehicle => 4,
        _ => 8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_type_vehicle_ok() {
        let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
        let ct_from_location = count_type_from_location(path).unwrap();
        let ct_from_header = count_type_from_header(path, ct_from_location).unwrap();
        assert_eq!(&ct_from_location, &ct_from_header);
        assert_eq!(ct_from_location, CountType::IndividualVehicle);
    }

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

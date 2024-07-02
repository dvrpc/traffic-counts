//! Extract various kinds of counts from files.
use std::fs::File;
use std::path::Path;

use csv::{Reader, ReaderBuilder};
use log::error;
use time::{macros::format_description, Date, PrimitiveDateTime, Time};

use crate::{
    num_nondata_rows, CountError, CountMetadata, FifteenMinuteBicycle, FifteenMinutePedestrian,
    FifteenMinuteVehicle, IndividualVehicle,
};

/// A trait for extracting count data from a file.
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
        let metadata = CountMetadata::from_path(path)?;

        // Iterate through data rows.
        let mut counts = vec![];
        for row in rdr.records().skip(num_nondata_rows(path)?) {
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
                metadata.dvrpc_num,
                count_date,
                count_time,
                row.as_ref().unwrap()[3].parse().unwrap(),
                metadata.directions.direction1,
                1,
            ) {
                Ok(v) => counts.push(v),
                Err(e) => {
                    error!("{e}");
                    continue;
                }
            };

            // There may also be a second count within the row.
            if let Some(v) = metadata.directions.direction2 {
                match FifteenMinuteVehicle::new(
                    metadata.dvrpc_num,
                    count_date,
                    count_time,
                    row.as_ref().unwrap()[4].parse().unwrap(),
                    v,
                    2,
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

/// Extract IndividualVehicle records from a file.
impl Extract for IndividualVehicle {
    type Item = IndividualVehicle;

    fn extract(path: &Path) -> Result<Vec<Self::Item>, CountError> {
        let data_file = File::open(path)?;
        let mut rdr = create_reader(&data_file);

        // Iterate through data rows.
        let mut counts = vec![];
        for row in rdr.records().skip(num_nondata_rows(path)?) {
            // Parse date.
            let date_format = format_description!("[month padding:none]/[day padding:none]/[year]");
            let date_col = &row.as_ref().unwrap()[1];
            let count_date = Date::parse(date_col, &date_format).unwrap();

            // Parse time.
            let time_format =
                format_description!("[hour padding:none repr:12]:[minute]:[second] [period]");
            let time_col = &row.as_ref().unwrap()[2];
            let count_time = Time::parse(time_col, &time_format).unwrap();

            let count = match IndividualVehicle::new(
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

/// Extract FifteenMinuteBicycle records from a file.
impl Extract for FifteenMinuteBicycle {
    type Item = FifteenMinuteBicycle;

    fn extract(path: &Path) -> Result<Vec<Self::Item>, CountError> {
        let data_file = File::open(path)?;
        let mut rdr = create_reader(&data_file);
        let metadata = CountMetadata::from_path(path)?;

        // Iterate through data rows.
        let mut counts = vec![];
        for row in rdr.records().skip(num_nondata_rows(path)?) {
            // Parse datetime.
            let datetime_format =
                format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
            let datetime_col = &row.as_ref().unwrap()[0];
            let count_dt = PrimitiveDateTime::parse(datetime_col, &datetime_format).unwrap();

            // Determine which fields to collect depending on direction(s) of count.
            match metadata.directions.direction2 {
                // If there's only one direction for this count, we only need the total.
                None => {
                    match FifteenMinuteBicycle::new(
                        metadata.dvrpc_num,
                        count_dt.date(),
                        count_dt.time(),
                        row.as_ref().unwrap()[1].parse().unwrap(),
                        None,
                        None,
                    ) {
                        Ok(v) => counts.push(v),
                        Err(e) => {
                            error!("{e}");
                            continue;
                        }
                    };
                }
                // If there are two directions, we need total, indir, and outdir.
                Some(_) => {
                    match FifteenMinuteBicycle::new(
                        metadata.dvrpc_num,
                        count_dt.date(),
                        count_dt.time(),
                        row.as_ref().unwrap()[1].parse().unwrap(),
                        Some(row.as_ref().unwrap()[2].parse().unwrap()),
                        Some(row.as_ref().unwrap()[3].parse().unwrap()),
                    ) {
                        Ok(v) => counts.push(v),
                        Err(e) => {
                            error!("{e}");
                            continue;
                        }
                    };
                }
            }
        }
        Ok(counts)
    }
}

/// Extract FifteenMinutePedestrian records from a file.
impl Extract for FifteenMinutePedestrian {
    type Item = FifteenMinutePedestrian;

    fn extract(path: &Path) -> Result<Vec<Self::Item>, CountError> {
        let data_file = File::open(path)?;
        let mut rdr = create_reader(&data_file);
        let metadata = CountMetadata::from_path(path)?;

        // Iterate through data rows.
        let mut counts = vec![];
        for row in rdr.records().skip(num_nondata_rows(path)?) {
            // Parse datetime.
            let datetime_format =
                format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
            let datetime_col = &row.as_ref().unwrap()[0];
            let count_dt = PrimitiveDateTime::parse(datetime_col, &datetime_format).unwrap();

            // Determine which fields to collect depending on direction(s) of count.
            match metadata.directions.direction2 {
                // If there's only one direction for this count, we only need the total.
                None => {
                    match FifteenMinutePedestrian::new(
                        metadata.dvrpc_num,
                        count_dt.date(),
                        count_dt.time(),
                        row.as_ref().unwrap()[1].parse().unwrap(),
                        None,
                        None,
                    ) {
                        Ok(v) => counts.push(v),
                        Err(e) => {
                            error!("{e}");
                            continue;
                        }
                    };
                }
                // If there are two directions, we need total, indir, and outdir.
                Some(_) => {
                    match FifteenMinutePedestrian::new(
                        metadata.dvrpc_num,
                        count_dt.date(),
                        count_dt.time(),
                        row.as_ref().unwrap()[1].parse().unwrap(),
                        Some(row.as_ref().unwrap()[2].parse().unwrap()),
                        Some(row.as_ref().unwrap()[3].parse().unwrap()),
                    ) {
                        Ok(v) => counts.push(v),
                        Err(e) => {
                            error!("{e}");
                            continue;
                        }
                    };
                }
            }
        }
        Ok(counts)
    }
}

/// Create CSV reader from file.
pub fn create_reader(file: &File) -> Reader<&File> {
    ReaderBuilder::new()
        .has_headers(false)
        .trim(csv::Trim::All)
        .flexible(true)
        .from_reader(file)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_ind_vehicle_gets_correct_number_of_counts() {
        let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
        let counted_vehicles = IndividualVehicle::extract(path).unwrap();
        assert_eq!(counted_vehicles.len(), 8706);
    }

    #[test]
    fn extract_fifteen_min_vehicle_gets_correct_number_of_counts() {
        let path = Path::new("test_files/15minutevehicle/rc-168193-ew-39352-na.txt");
        let fifteen_min_volcount = FifteenMinuteVehicle::extract(path).unwrap();
        assert_eq!(fifteen_min_volcount.len(), 384)
    }

    #[test]
    fn extract_fifteen_min_bicycle_gets_correct_number_of_counts() {
        let path = Path::new("test_files/15minutebicycle/vg-167607-ns-4175-na.csv");
        let fifteen_min_volcount = FifteenMinuteBicycle::extract(path).unwrap();
        assert_eq!(fifteen_min_volcount.len(), 480);

        let in_sum = fifteen_min_volcount
            .iter()
            .map(|count| count.indir.unwrap())
            .sum::<u16>();
        let out_sum = fifteen_min_volcount
            .iter()
            .map(|count| count.outdir.unwrap())
            .sum::<u16>();
        let sum = fifteen_min_volcount
            .iter()
            .map(|count| count.total)
            .sum::<u16>();
        assert_eq!(in_sum, 491);
        assert_eq!(out_sum, 20);
        assert_eq!(sum, 511);
    }

    #[test]
    fn extract_fifteen_min_pedestrian_gets_correct_number_of_counts() {
        let path = Path::new("test_files/15minutepedestrian/vg-167297-ns-4874-na.csv");
        let fifteen_min_volcount = FifteenMinutePedestrian::extract(path).unwrap();
        assert_eq!(fifteen_min_volcount.len(), 768);

        let in_sum = fifteen_min_volcount
            .iter()
            .map(|count| count.indir.unwrap())
            .sum::<u16>();
        let out_sum = fifteen_min_volcount
            .iter()
            .map(|count| count.outdir.unwrap())
            .sum::<u16>();
        let sum = fifteen_min_volcount
            .iter()
            .map(|count| count.total)
            .sum::<u16>();
        assert_eq!(in_sum, 1281);
        assert_eq!(out_sum, 1201);
        assert_eq!(sum, 2482);
    }
}

//! Extract count data from files.
//!
//! See the [Extract trait implementors](Extract#implementors) for kinds of counts.
use std::fs::{self, File};
use std::path::Path;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use csv::{Reader, ReaderBuilder};
use log::error;

use crate::{
    CountError, FieldMetadata, FifteenMinuteBicycle, FifteenMinutePedestrian, FifteenMinuteVehicle,
    IndividualVehicle,
};

// headers stripped of double quotes and spaces
const FIFTEEN_MINUTE_VEHICLE_HEADER: &str = "Number,Date,Time,Channel1";
const INDIVIDUAL_VEHICLE_HEADER: &str = "Veh.No.,Date,Time,Channel,Class,Speed";
const FIFTEEN_MINUTE_BIKE_OR_PED_HEADER: &str = "Time,";

/// The kinds of counts this module can handle as inputs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InputCount {
    /// Eco-Counter
    ///
    /// There's no difference in the headers for the exported CSV, so this is used to partially
    /// identify the type.
    FifteenMinuteBicycleOrPedestrian,
    /// Pre-binned, 15-minute volume counts from Eco-Counter
    ///
    /// See [`FifteenMinutePedestrian`], the corresponding type.
    FifteenMinuteBicycle,
    /// Pre-binned, 15-minute volume counts from Eco-Counter
    ///
    /// See [`FifteenMinuteBicycle`], the corresponding type.
    FifteenMinutePedestrian,
    /// Pre-binned, 15-minute volume counts from StarNext/Jamar.
    ///
    /// See [`FifteenMinuteVehicle`], the corresponding type.
    FifteenMinuteVehicle,
    /// Individual vehicles from StarNext/Jamar prior to any binning.
    ///
    /// See [`IndividualVehicle`], the corresponding type.
    IndividualVehicle,
}

impl InputCount {
    /// Get the `InputCount` variant from the parent directory where a file is located.
    pub fn from_parent_dir(path: &Path) -> Result<Self, CountError> {
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

        match parent {
            "15minutebicycle" => Ok(InputCount::FifteenMinuteBicycle),
            "15minutepedestrian" => Ok(InputCount::FifteenMinutePedestrian),
            "15minutevehicle" => Ok(InputCount::FifteenMinuteVehicle),
            "vehicle" => Ok(InputCount::IndividualVehicle),
            _ => Err(CountError::BadLocation(parent.to_string())),
        }
    }

    /// Get `InputCount` variant based on the header of a file.
    pub fn from_header(path: &Path) -> Result<InputCount, CountError> {
        let (count_type, _) = count_type_and_num_nondata_rows(path)?;
        Ok(count_type)
    }

    /// Get `InputCount` variant from both parent directory and the header of the file.
    ///
    /// If the variant differs between the two methods, we can't be sure which is correct,
    /// so return an error.
    pub fn from_parent_dir_and_header(path: &Path) -> Result<InputCount, CountError> {
        let count_type_from_location = InputCount::from_parent_dir(path)?;
        let count_type_from_header = InputCount::from_header(path)?;

        // For bicycle and pedestrian counts from Eco-Counter, there is no difference between
        // the header, so the `from_header` method can only partially determine the type.
        if count_type_from_header == InputCount::FifteenMinuteBicycleOrPedestrian {
            if count_type_from_location != InputCount::FifteenMinuteBicycle
                && count_type_from_location != InputCount::FifteenMinutePedestrian
            {
                return Err(CountError::LocationHeaderMisMatch(path));
            }
        } else if count_type_from_location != count_type_from_header {
            return Err(CountError::LocationHeaderMisMatch(path));
        }
        Ok(count_type_from_location)
    }
}

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
        let metadata = FieldMetadata::from_path(path)?;

        // Iterate through data rows.
        let mut counts = vec![];
        for row in rdr.records().skip(num_nondata_rows(path)?) {
            // Parse date.
            let date_format = "%-m/%-d/%Y";
            let date_col = &row.as_ref().unwrap()[1];
            let count_date = NaiveDate::parse_from_str(date_col, date_format).unwrap();

            // Parse time.
            let time_format = "%-I:%M %P";
            let time_col = &row.as_ref().unwrap()[2];
            let count_time = NaiveTime::parse_from_str(time_col, time_format).unwrap();

            // There will always be at least one count per row.
            // Extract the first (and perhaps only) direction.
            match row.as_ref().unwrap().get(3) {
                Some(count) => match count.parse() {
                    Ok(count) => match FifteenMinuteVehicle::new(
                        metadata.record_num,
                        count_date,
                        count_time,
                        count,
                        metadata.directions.direction1,
                        1,
                    ) {
                        Ok(v) => counts.push(v),
                        Err(e) => {
                            error!("{e}");
                            continue;
                        }
                    },
                    Err(e) => return Err(CountError::ParseError(e)),
                },
                None => return Err(CountError::DirectionLenMisMatch(path)),
            }

            // There may also be a second count within the row.
            if let Some(direction) = metadata.directions.direction2 {
                match row.as_ref().unwrap().get(4) {
                    Some(count) => match count.parse() {
                        Ok(count) => match FifteenMinuteVehicle::new(
                            metadata.record_num,
                            count_date,
                            count_time,
                            count,
                            direction,
                            2,
                        ) {
                            Ok(v) => counts.push(v),
                            Err(e) => {
                                error!("{e}");
                                continue;
                            }
                        },
                        Err(e) => return Err(CountError::ParseError(e)),
                    },
                    None => return Err(CountError::DirectionLenMisMatch(path)),
                }
            }
            // There may also be a third count within the row.
            if let Some(direction) = metadata.directions.direction3 {
                match row.as_ref().unwrap().get(5) {
                    Some(count) => match count.parse() {
                        Ok(count) => match FifteenMinuteVehicle::new(
                            metadata.record_num,
                            count_date,
                            count_time,
                            count,
                            direction,
                            3,
                        ) {
                            Ok(v) => counts.push(v),
                            Err(e) => {
                                error!("{e}");
                                continue;
                            }
                        },
                        Err(e) => return Err(CountError::ParseError(e)),
                    },
                    None => return Err(CountError::DirectionLenMisMatch(path)),
                }
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
            let date_format = "%-m/%-d/%Y";
            let date_col = &row.as_ref().unwrap()[1];
            let count_date = NaiveDate::parse_from_str(date_col, date_format).unwrap();

            // Parse time.
            let time_format = "%-I:%M:%S %P";
            let time_col = &row.as_ref().unwrap()[2];
            let count_time = NaiveTime::parse_from_str(time_col, time_format).unwrap();

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
        let metadata = FieldMetadata::from_path(path)?;

        // Iterate through data rows.
        let mut counts = vec![];
        for row in rdr.records().skip(num_nondata_rows(path)?) {
            // Parse datetime.
            let datetime_format = "%Y-%m-%d %H:%M:%S";
            let datetime_col = &row.as_ref().unwrap()[0];
            let count_dt = NaiveDateTime::parse_from_str(datetime_col, datetime_format).unwrap();

            // Determine which fields to collect depending on direction(s) of count.
            match metadata.directions.direction2 {
                // If there's only one direction for this count, we only need the total.
                None => {
                    match FifteenMinuteBicycle::new(
                        metadata.record_num,
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
                        metadata.record_num,
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
        let metadata = FieldMetadata::from_path(path)?;

        // Iterate through data rows.
        let mut counts = vec![];
        for row in rdr.records().skip(num_nondata_rows(path)?) {
            // Parse datetime.
            let datetime_format = "%Y-%m-%d %H:%M:%S";
            let datetime_col = &row.as_ref().unwrap()[0];
            let count_dt = NaiveDateTime::parse_from_str(datetime_col, datetime_format).unwrap();

            // Determine which fields to collect depending on direction(s) of count.
            match metadata.directions.direction2 {
                // If there's only one direction for this count, we only need the total.
                None => {
                    match FifteenMinutePedestrian::new(
                        metadata.record_num,
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
                        metadata.record_num,
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

/// Get `InputCount` and number of rows in file before data starts (metadata rows + header).
/*
  This is a rather naive solution - it simply checks that the exact string (
  stripped of double quotes and spaces) of one of the potential headers (and thus `InputCount`)
  is in the file. To make it somewhat performant, it limits the search to the first 50 lines, which
  is an egregiously large number to ensure that we will never miss the header and prevents the
  search going through tens of thousands of lines, which is the typical number in files.

  Two values are returned because the exact same procedure is needed to determine
  either of them. Convenience functions using it are also available to get only one value.
*/
fn count_type_and_num_nondata_rows(path: &Path) -> Result<(InputCount, usize), CountError> {
    let mut num_rows = 0;
    let contents = fs::read_to_string(path)?;
    for line in contents.lines().take(50) {
        num_rows += 1;
        let line = line.replace(['"', ' '], "");
        if line.starts_with(FIFTEEN_MINUTE_BIKE_OR_PED_HEADER) {
            return Ok((InputCount::FifteenMinuteBicycleOrPedestrian, num_rows));
        } else if line.contains(FIFTEEN_MINUTE_VEHICLE_HEADER) {
            return Ok((InputCount::FifteenMinuteVehicle, num_rows));
        } else if line.contains(INDIVIDUAL_VEHICLE_HEADER) {
            return Ok((InputCount::IndividualVehicle, num_rows));
        }
    }
    Err(CountError::BadHeader(path))
}

/// Get the number of nondata rows in a file.
pub fn num_nondata_rows(path: &Path) -> Result<usize, CountError> {
    let (_, num_rows) = count_type_and_num_nondata_rows(path)?;
    Ok(num_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LaneDirection;

    #[test]
    fn extract_ind_vehicle_gets_correct_number_of_counts() {
        let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
        let counted_vehicles = IndividualVehicle::extract(path).unwrap();
        assert_eq!(counted_vehicles.len(), 8706);
    }

    #[test]
    fn extract_ind_vehicle_gets_correct_number_of_counts_by_lane() {
        let path = Path::new("test_files/vehicle/kw-101-eee-21-35.csv");
        let counted_vehicles = IndividualVehicle::extract(path).unwrap();
        assert_eq!(counted_vehicles.len(), 227);

        let lane1 = counted_vehicles
            .iter()
            .filter(|veh| veh.lane == 1)
            .collect::<Vec<_>>();
        let lane2 = counted_vehicles
            .iter()
            .filter(|veh| veh.lane == 2)
            .collect::<Vec<_>>();
        let lane3 = counted_vehicles
            .iter()
            .filter(|veh| veh.lane == 3)
            .collect::<Vec<_>>();

        assert_eq!(lane1.len(), 96);
        assert_eq!(lane2.len(), 104);
        assert_eq!(lane3.len(), 27);
    }

    #[test]
    fn extract_fifteen_min_vehicle_gets_correct_number_of_counts_168193() {
        let path = Path::new("test_files/15minutevehicle/rc-168193-ew-39352-na.txt");
        let fifteen_min_volcount = FifteenMinuteVehicle::extract(path).unwrap();
        assert_eq!(fifteen_min_volcount.len(), 384)
    }

    #[test]
    fn extract_fifteen_min_vehicle_gets_correct_number_of_counts_102() {
        let path = Path::new("test_files/15minutevehicle/kw-102-www-21-35.csv");
        let mut fifteen_min_volcount = FifteenMinuteVehicle::extract(path).unwrap();
        fifteen_min_volcount.sort_unstable_by_key(|count| (count.date, count.time, count.lane));
        assert_eq!(fifteen_min_volcount.len(), 57);

        let count0 = fifteen_min_volcount.first().unwrap();
        dbg!(count0);
        assert_eq!(count0.lane, 1);
        assert_eq!(count0.direction, LaneDirection::West);
        assert_eq!(count0.count, 49);
        let count1 = fifteen_min_volcount.get(1).unwrap();
        assert_eq!(count1.lane, 2);
        assert_eq!(count1.direction, LaneDirection::West);
        assert_eq!(count1.count, 68);
        let count2 = fifteen_min_volcount.get(2).unwrap();
        assert_eq!(count2.lane, 3);
        assert_eq!(count2.direction, LaneDirection::West);
        assert_eq!(count2.count, 10);
    }

    #[test]
    fn extract_fifteen_min_vehicle_errs_when_dirs_mismatch_in_filename_and_data_103() {
        let path = Path::new("test_files/15minutevehicle/kw-103-sss-21-35.csv");

        assert!(matches!(
            FifteenMinuteVehicle::extract(path),
            Err(CountError::DirectionLenMisMatch(_))
        ))
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

    #[test]
    fn count_type_from_location_correct_ind_veh() {
        let count_type = InputCount::from_parent_dir(Path::new("/vehicle/count_data.csv")).unwrap();
        assert_eq!(count_type, InputCount::IndividualVehicle)
    }

    #[test]
    fn count_type_from_location_correct_15min_veh() {
        let count_type =
            InputCount::from_parent_dir(Path::new("/15minutevehicle/count_data.csv")).unwrap();
        assert_eq!(count_type, InputCount::FifteenMinuteVehicle)
    }

    #[test]
    fn count_type_from_location_correct_15min_bicycle() {
        let count_type =
            InputCount::from_parent_dir(Path::new("/15minutebicycle/count_data.csv")).unwrap();
        assert_eq!(count_type, InputCount::FifteenMinuteBicycle)
    }

    #[test]
    fn count_type_from_location_correct_15min_ped() {
        let count_type =
            InputCount::from_parent_dir(Path::new("/15minutepedestrian/count_data.csv")).unwrap();
        assert_eq!(count_type, InputCount::FifteenMinutePedestrian)
    }

    #[test]
    fn count_type_from_location_errs_if_invalid_dir() {
        let count_type = InputCount::from_parent_dir(Path::new("/not_count_dir/count_data.csv"));
        assert!(matches!(count_type, Err(CountError::BadLocation(_))))
    }

    #[test]
    fn count_type_and_num_nondata_rows_correct_15min_veh_sample() {
        let path = Path::new("test_files/15minutevehicle/rc-168193-ew-39352-na.txt");
        let (count_type, num_rows) = count_type_and_num_nondata_rows(path).unwrap();
        assert_eq!(count_type, InputCount::FifteenMinuteVehicle);
        assert_eq!(num_rows, 5);
    }

    #[test]
    fn count_type_and_num_nondata_rows_correct_ind_veh_sample() {
        let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
        let (count_type, num_rows) = count_type_and_num_nondata_rows(path).unwrap();
        assert_eq!(count_type, InputCount::IndividualVehicle);
        assert_eq!(num_rows, 4);
    }

    #[test]
    fn count_type_and_num_nondata_rows_correct_15min_bicycle_sample() {
        let path = Path::new("test_files/15minutebicycle/vg-167607-ns-4175-na.csv");
        let (count_type, num_rows) = count_type_and_num_nondata_rows(path).unwrap();
        assert_eq!(count_type, InputCount::FifteenMinuteBicycleOrPedestrian);
        assert_eq!(num_rows, 3);
    }

    #[test]
    fn count_type_and_num_nondata_rows_correct_15min_pedestrian_sample() {
        let path = Path::new("test_files/15minutepedestrian/vg-167297-ns-4874-na.csv");
        let (count_type, num_rows) = count_type_and_num_nondata_rows(path).unwrap();
        assert_eq!(count_type, InputCount::FifteenMinuteBicycleOrPedestrian);
        assert_eq!(num_rows, 3);
    }

    #[test]
    fn count_type_and_num_nondata_rows_errs_if_no_matching_header() {
        let path = Path::new("test_files/bad_header.txt");
        assert!(matches!(
            count_type_and_num_nondata_rows(path),
            Err(CountError::BadHeader(_))
        ))
    }

    #[test]
    fn num_nondata_rows_correct() {
        let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
        let num_rows = num_nondata_rows(path).unwrap();
        assert_eq!(num_rows, 4);
    }

    #[test]
    fn count_type_ind_veh_from_header_correct() {
        let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
        let ct_from_header = InputCount::from_header(path).unwrap();
        assert_eq!(ct_from_header, InputCount::IndividualVehicle);
    }

    #[test]
    fn count_type_15min_veh_from_header_correct() {
        let path = Path::new("test_files/15minutevehicle/rc-168193-ew-39352-na.txt");
        let ct_from_header = InputCount::from_header(path).unwrap();
        assert_eq!(ct_from_header, InputCount::FifteenMinuteVehicle);
    }

    #[test]
    fn count_type_15min_bicycle_from_header_correct() {
        let path = Path::new("test_files/15minutebicycle/vg-167607-ns-4175-na.csv");
        let ct_from_header = InputCount::from_header(path).unwrap();
        assert_eq!(ct_from_header, InputCount::FifteenMinuteBicycleOrPedestrian);
    }

    #[test]
    fn count_type_15min_pedestrian_from_header_correct() {
        let path = Path::new("test_files/15minutepedestrian/vg-167297-ns-4874-na.csv");
        let ct_from_header = InputCount::from_header(path).unwrap();
        assert_eq!(ct_from_header, InputCount::FifteenMinuteBicycleOrPedestrian);
    }

    #[test]
    fn count_type_from_header_errs_if_non_extant_file() {
        let path = Path::new("test_files/not_a_file.csv");
        assert!(matches!(
            InputCount::from_header(path),
            Err(CountError::CannotOpenFile { .. })
        ))
    }

    #[test]
    fn count_type_from_header_errs_if_no_matching_header() {
        let path = Path::new("test_files/bad_header.txt");
        assert!(matches!(
            InputCount::from_header(path),
            Err(CountError::BadHeader(_))
        ))
    }

    #[test]
    fn count_type_from_parent_dir_and_header_15min_veh_correct() {
        let path = Path::new("test_files/15minutevehicle/rc-168193-ew-39352-na.txt");
        let count_type = InputCount::from_parent_dir_and_header(path).unwrap();
        assert_eq!(count_type, InputCount::FifteenMinuteVehicle);
    }

    #[test]
    fn count_type_from_parent_dir_and_header_ind_veh_correct() {
        let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
        let count_type = InputCount::from_parent_dir_and_header(path).unwrap();
        assert_eq!(count_type, InputCount::IndividualVehicle);
    }

    #[test]
    fn count_type_from_parent_dir_and_header_15min_bicycle_correct() {
        let path = Path::new("test_files/15minutebicycle/vg-167607-ns-4175-na.csv");
        let count_type = InputCount::from_parent_dir_and_header(path).unwrap();
        assert_eq!(count_type, InputCount::FifteenMinuteBicycle);
    }

    #[test]
    fn count_type_from_parent_dir_and_header_15min_pedestrian_correct() {
        let path = Path::new("test_files/15minutepedestrian/vg-167297-ns-4874-na.csv");
        let count_type = InputCount::from_parent_dir_and_header(path).unwrap();
        assert_eq!(count_type, InputCount::FifteenMinutePedestrian);
    }

    #[test]
    fn count_type_from_parent_dir_and_errs_if_mismatch1() {
        let path = Path::new("test_files/15minutevehicle/ind_veh_count.txt");
        let count_type = InputCount::from_parent_dir_and_header(path);
        assert!(matches!(
            count_type,
            Err(CountError::LocationHeaderMisMatch(_))
        ))
    }

    #[test]
    fn count_type_from_parent_dir_and_errs_if_mismatch2() {
        let path = Path::new("test_files/vehicle/15min_veh_count.txt");
        let count_type = InputCount::from_parent_dir_and_header(path);
        assert!(matches!(
            count_type,
            Err(CountError::LocationHeaderMisMatch(_))
        ))
    }

    #[test]
    fn count_type_from_parent_dir_and_errs_if_mismatch3() {
        let path = Path::new("test_files/15minutebicycle/15min_veh_count.txt");
        let count_type = InputCount::from_parent_dir_and_header(path);
        assert!(matches!(
            count_type,
            Err(CountError::LocationHeaderMisMatch(_))
        ))
    }

    #[test]
    fn count_type_from_parent_dir_and_errs_if_mismatch4() {
        let path = Path::new("test_files/15minutevehicle/15min_bicycle_count.txt");
        let count_type = InputCount::from_parent_dir_and_header(path);
        assert!(matches!(
            count_type,
            Err(CountError::LocationHeaderMisMatch(_))
        ))
    }

    #[test]
    fn count_type_from_parent_dir_and_errs_if_mismatch5() {
        let path = Path::new("test_files/15minutevehicle/15min_pedestrian_count.csv");
        let count_type = InputCount::from_parent_dir_and_header(path);
        assert!(matches!(
            count_type,
            Err(CountError::LocationHeaderMisMatch(_))
        ))
    }

    #[test]
    fn count_type_from_parent_dir_and_errs_if_mismatch6() {
        let path = Path::new("test_files/15minutepedestrian/15min_veh_count.txt");
        let count_type = InputCount::from_parent_dir_and_header(path);
        assert!(matches!(
            count_type,
            Err(CountError::LocationHeaderMisMatch(_))
        ))
    }
}

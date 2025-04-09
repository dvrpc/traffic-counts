//! Extract count data from files.
//!
//! See the [Extract trait implementors](Extract#implementors) for kinds of counts.
use std::fs::{self, File};
use std::path::Path;

use chrono::{format::ParseErrorKind, NaiveDate, NaiveDateTime, NaiveTime};
use csv::{Reader, ReaderBuilder};
use log::error;

use crate::{
    CountError, Directions, FifteenMinuteBicycle, FifteenMinutePedestrian, FifteenMinuteVehicle,
    IndividualBicycle, IndividualVehicle,
};

// Headers stripped of double quotes and spaces.
const FIFTEEN_MINUTE_VEHICLE_HEADER1: &str = "Number,Date,Time,Channel1";
const FIFTEEN_MINUTE_VEHICLE_HEADER2: &str = "Number,Date,Time,Channel2";
const FIFTEEN_MINUTE_BIKE_OR_PED_HEADER: &str = "Time,";
const IND_VEH_OR_IND_BIKE: &str = "Veh.No.,Date,Time,Channel,Class,Speed";

/// The kinds of counts this module can handle as inputs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InputCount {
    /// Pre-binned, 15-minute volume counts from Eco-Counter
    /// See [`FifteenMinutePedestrian`], the corresponding type.
    FifteenMinuteBicycle,
    /// Pre-binned, 15-minute volume counts from Eco-Counter
    ///
    /// See [`FifteenMinuteBicycle`], the corresponding type.
    FifteenMinutePedestrian,
    /// Pre-binned, 15-minute volume counts from StarNext/JAMAR.
    ///
    /// See [`FifteenMinuteVehicle`], the corresponding type.
    FifteenMinuteVehicle,
    /// Individual vehicles from StarNext/JAMAR prior to any binning.
    ///
    /// See [`IndividualVehicle`], the corresponding type.
    IndividualVehicle,
    /// Individual vehicles and individual bicycles from StarNext/JAMAR prior to any binning.
    ///
    /// See [`IndividualVehicle`] and [`IndividualBicycle`], the corresponding types.
    IndividualVehicleAndIndividualBicycle,
}

impl InputCount {
    /// Get the `InputCount` variant from the parent directory where a file is located.
    pub fn from_parent_dir(path: &Path) -> Result<Self, CountError> {
        // Get the directory immediately above the file.
        let parent = path
            .parent()
            .ok_or(CountError::BadPath(path.to_owned()))?
            .components()
            .last()
            .ok_or(CountError::BadPath(path.to_owned()))?
            .as_os_str()
            .to_str()
            .ok_or(CountError::BadPath(path.to_owned()))?;

        match parent {
            "15minutebicycle" => Ok(InputCount::FifteenMinuteBicycle),
            "15minutepedestrian" => Ok(InputCount::FifteenMinutePedestrian),
            "15minutevehicle" => Ok(InputCount::FifteenMinuteVehicle),
            "vehicle_only" => Ok(InputCount::IndividualVehicle),
            "vehicle_and_bicycle" => Ok(InputCount::IndividualVehicleAndIndividualBicycle),
            _ => Err(CountError::BadLocation(parent.to_string())),
        }
    }
}

// Extract FifteenMinuteVehicle records from a file.
impl FifteenMinuteVehicle {
    pub fn extract(
        path: &Path,
        recordnum: u32,
        directions: &Directions,
    ) -> Result<Vec<Self>, CountError> {
        let data_file = File::open(path)?;
        let mut rdr = create_reader(&data_file);

        // Iterate through data rows.
        let mut counts = vec![];
        for row in rdr.records().skip(num_nondata_rows(path)?) {
            let row = row?;
            let datetime = NaiveDateTime::new(
                parse_date(row.get(1).ok_or(CountError::MissingDirection)?)?,
                parse_time(row.get(2).ok_or(CountError::MissingDataColumn)?)?,
            );

            // There will always be at least one count per row.
            // Extract the first (and perhaps only) direction.
            match row.get(3) {
                Some(count) => match count.parse() {
                    Ok(count) => match FifteenMinuteVehicle::new(
                        recordnum,
                        datetime.date(),
                        datetime,
                        count,
                        Some(directions.direction1),
                        Some(1),
                    ) {
                        Ok(v) => counts.push(v),
                        Err(e) => {
                            error!("{e}");
                            continue;
                        }
                    },
                    Err(e) => return Err(CountError::ParseIntError(e)),
                },
                None => return Err(CountError::DirectionLenMisMatch),
            }

            // There may also be a second count within the row.
            if let Some(direction) = directions.direction2 {
                match row.get(4) {
                    Some(count) => match count.parse() {
                        Ok(count) => match FifteenMinuteVehicle::new(
                            recordnum,
                            datetime.date(),
                            datetime,
                            count,
                            Some(direction),
                            Some(2),
                        ) {
                            Ok(v) => counts.push(v),
                            Err(e) => {
                                error!("{e}");
                                continue;
                            }
                        },
                        Err(e) => return Err(CountError::ParseIntError(e)),
                    },
                    None => return Err(CountError::DirectionLenMisMatch),
                }
            }
            // There may also be a third count within the row.
            if let Some(direction) = directions.direction3 {
                match row.get(5) {
                    Some(count) => match count.parse() {
                        Ok(count) => match FifteenMinuteVehicle::new(
                            recordnum,
                            datetime.date(),
                            datetime,
                            count,
                            Some(direction),
                            Some(3),
                        ) {
                            Ok(v) => counts.push(v),
                            Err(e) => {
                                error!("{e}");
                                continue;
                            }
                        },
                        Err(e) => return Err(CountError::ParseIntError(e)),
                    },
                    None => return Err(CountError::DirectionLenMisMatch),
                }
            }
        }
        Ok(counts)
    }
}

/// The number for unknown class depends on which schema is usededj
pub enum Bicycles {
    Without, // Modified Schema F
    With,    // Modified Schema F - With Bikes
}

impl IndividualVehicle {
    pub fn extract(path: &Path, bicycles: Bicycles) -> Result<Vec<Self>, CountError> {
        let data_file = File::open(path)?;
        let mut rdr = create_reader(&data_file);

        // Iterate through data rows.
        let mut counts = vec![];
        for row in rdr.records().skip(num_nondata_rows(path)?) {
            let row = row?;
            let datetime = NaiveDateTime::new(
                parse_date(row.get(1).ok_or(CountError::MissingDataColumn)?)?,
                parse_time(row.get(2).ok_or(CountError::MissingDataColumn)?)?,
            );

            // If bicycles are included, they are given class 14. They should not be included at
            // all - not as bicycles nor as unclassified, because they are included in separate
            // recordnum and count.
            let count = match bicycles {
                Bicycles::With => match row.get(4).ok_or(CountError::MissingDataColumn)?.parse() {
                    Ok(14) => continue,
                    Ok(v) => match IndividualVehicle::new(
                        datetime.date(),
                        datetime,
                        row.get(3).ok_or(CountError::MissingDataColumn)?.parse()?,
                        v,
                        row.get(5).ok_or(CountError::MissingDataColumn)?.parse()?,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("{e}");
                            continue;
                        }
                    },
                    Err(e) => {
                        error!("{e}");
                        continue;
                    }
                },
                Bicycles::Without => match IndividualVehicle::new(
                    datetime.date(),
                    datetime,
                    row.get(3).ok_or(CountError::MissingDataColumn)?.parse()?,
                    row.get(4).ok_or(CountError::MissingDataColumn)?.parse()?,
                    row.get(5).ok_or(CountError::MissingDataColumn)?.parse()?,
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("{e}");
                        continue;
                    }
                },
            };

            counts.push(count);
        }
        Ok(counts)
    }
}

/// Extract IndividualBicycle records from a file.
impl IndividualBicycle {
    pub fn extract(path: &Path) -> Result<Vec<Self>, CountError> {
        let data_file = File::open(path)?;
        let mut rdr = create_reader(&data_file);

        // Iterate through data rows.
        let mut counts = vec![];
        for row in rdr.records().skip(num_nondata_rows(path)?) {
            let row = row?;
            // Bicycles are given class 14. Skip if not 14.
            if row
                .get(4)
                .ok_or(CountError::MissingDataColumn)?
                .parse::<u16>()?
                != 14
            {
                continue;
            }
            let datetime = NaiveDateTime::new(
                parse_date(row.get(1).ok_or(CountError::MissingDataColumn)?)?,
                parse_time(row.get(2).ok_or(CountError::MissingDataColumn)?)?,
            );

            let count = match IndividualBicycle::new(
                datetime.date(),
                datetime,
                row.get(3).ok_or(CountError::MissingDataColumn)?.parse()?,
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
impl FifteenMinuteBicycle {
    pub fn extract(
        path: &Path,
        recordnum: u32,
        directions: &Directions,
    ) -> Result<Vec<Self>, CountError> {
        let data_file = File::open(path)?;
        let mut rdr = create_reader(&data_file);

        // Iterate through data rows.
        let mut counts = vec![];
        for row in rdr.records().skip(num_nondata_rows(path)?) {
            let row = row?;
            let datetime = parse_datetime(row.get(0).ok_or(CountError::MissingDataColumn)?)?;

            // Ignore row if no value in total count column.
            match row
                .get(1)
                .ok_or(CountError::MissingDataColumn)?
                .parse::<u32>()
            {
                Ok(_) => (),
                Err(_) => {
                    continue;
                }
            };

            // Direction1/indir
            match FifteenMinuteBicycle::new(
                recordnum,
                datetime,
                row.get(2).ok_or(CountError::MissingDataColumn)?.parse()?,
                directions.direction1,
            ) {
                Ok(v) => counts.push(v),
                Err(e) => {
                    error!("{e}");
                    continue;
                }
            }
            // Optionally direction2/outdir
            if let Some(v) = directions.direction2 {
                match FifteenMinuteBicycle::new(
                    recordnum,
                    datetime,
                    row.get(3).ok_or(CountError::MissingDataColumn)?.parse()?,
                    v,
                ) {
                    Ok(v) => counts.push(v),
                    Err(e) => {
                        error!("{e}");
                        continue;
                    }
                }
            }
        }
        Ok(counts)
    }
}

/// Extract FifteenMinutePedestrian records from a file.
impl FifteenMinutePedestrian {
    pub fn extract(
        path: &Path,
        recordnum: u32,
        directions: &Directions,
    ) -> Result<Vec<Self>, CountError> {
        let data_file = File::open(path)?;
        let mut rdr = create_reader(&data_file);

        // Iterate through data rows.
        let mut counts = vec![];
        for row in rdr.records().skip(num_nondata_rows(path)?) {
            let row = row?;
            let datetime = parse_datetime(row.get(0).ok_or(CountError::MissingDataColumn)?)?;

            // Ignore row if no value in total count column.
            match row
                .get(1)
                .ok_or(CountError::MissingDataColumn)?
                .parse::<u32>()
            {
                Ok(_) => (),
                Err(_) => {
                    continue;
                }
            };

            // Direction1/indir
            match FifteenMinutePedestrian::new(
                recordnum,
                datetime,
                row.get(2).ok_or(CountError::MissingDataColumn)?.parse()?,
                directions.direction1,
            ) {
                Ok(v) => counts.push(v),
                Err(e) => {
                    error!("{e}");
                    continue;
                }
            }
            // Optionally direction2/outdir
            if let Some(v) = directions.direction2 {
                match FifteenMinutePedestrian::new(
                    recordnum,
                    datetime,
                    row.get(3).ok_or(CountError::MissingDataColumn)?.parse()?,
                    v,
                ) {
                    Ok(v) => counts.push(v),
                    Err(e) => {
                        error!("{e}");
                        continue;
                    }
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

/// Get the number of nondata rows in a file based on header.
///
/// This is a rather naive solution - it simply checks that the exact string (stripped of
/// double quotes and spaces) of one of the potential headers (and thus `InputCount`) is in the
/// file. To make it somewhat performant, it limits the search to the first 50 lines, which
/// is an egregiously large number to ensure that we will never miss the header and prevents the
/// search going through tens of thousands of lines, which is the typical number in files.
pub fn num_nondata_rows(path: &Path) -> Result<usize, CountError> {
    let mut num_rows = 0;
    let contents = fs::read_to_string(path)?;
    for line in contents.lines().take(50) {
        num_rows += 1;
        let line = line.replace(['"', ' '], "");
        if line.starts_with(FIFTEEN_MINUTE_BIKE_OR_PED_HEADER)
            || line.contains(FIFTEEN_MINUTE_VEHICLE_HEADER1)
            || line.contains(FIFTEEN_MINUTE_VEHICLE_HEADER2)
            || line.contains(IND_VEH_OR_IND_BIKE)
        {
            return Ok(num_rows);
        }
    }
    Err(CountError::BadHeader(path.to_owned()))
}

/// Parse time from a str that can be in multiple formats.
fn parse_time(s: &str) -> Result<NaiveTime, CountError> {
    let mut err = ParseErrorKind::Invalid;

    for fmt in [
        "%-I:%M %P",
        "%-I:%M:%S %P",
        "%-I:%M%P",
        "%-I:%M:%S%P",
        "%H:%M:%S",
    ] {
        match NaiveTime::parse_from_str(s, fmt) {
            Ok(v) => return Ok(v),
            Err(e) => err = e.kind(),
        }
    }
    // If no format was successfully used in parsing, return error.
    Err(CountError::ChronoParseError(err))
}

/// Parse date from a str that can be in multiple formats.
fn parse_date(s: &str) -> Result<NaiveDate, CountError> {
    let mut err = ParseErrorKind::Invalid;

    for fmt in ["%-m/%-d/%Y", "%-m-%-d-%Y", "%Y-%m-%d"] {
        match NaiveDate::parse_from_str(s, fmt) {
            Ok(v) => return Ok(v),
            Err(e) => err = e.kind(),
        }
    }
    // If no format was successfully used in parsing, return error.
    Err(CountError::ChronoParseError(err))
}

/// Parse datetime from a str that can be in multiple formats.
fn parse_datetime(s: &str) -> Result<NaiveDateTime, CountError> {
    let mut err = ParseErrorKind::Invalid;

    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"] {
        match NaiveDateTime::parse_from_str(s, fmt) {
            Ok(v) => return Ok(v),
            Err(e) => err = e.kind(),
        }
    }
    // If no format was successfully used in parsing, return error.
    Err(CountError::ChronoParseError(err))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LaneDirection;

    #[test]
    fn extract_ind_vehicle_gets_correct_number_of_counts() {
        let path = Path::new("test_files/vehicle_only/166905.txt");
        let counted_vehicles = IndividualVehicle::extract(path, Bicycles::Without).unwrap();
        assert_eq!(counted_vehicles.len(), 8706);
    }

    #[test]
    fn extract_ind_vehicle_gets_correct_number_of_counts_by_lane() {
        let path = Path::new("test_files/vehicle_only/101.csv");
        let counted_vehicles = IndividualVehicle::extract(path, Bicycles::Without).unwrap();
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

    /// Note that 168193's contents doesn't match with db: this was one that had previously
    /// been separated into two different counts due to limitations of previous import program.
    #[test]
    fn extract_fifteen_min_vehicle_gets_correct_number_of_counts_168193() {
        let path = Path::new("test_files/15minutevehicle/168193.txt");
        let directions = Directions::new(LaneDirection::East, Some(LaneDirection::West), None);
        let fifteen_min_volcount =
            FifteenMinuteVehicle::extract(path, 168193, &directions).unwrap();
        assert_eq!(fifteen_min_volcount.len(), 384)
    }

    #[test]
    fn extract_fifteen_min_vehicle_gets_correct_number_of_counts_102() {
        let path = Path::new("test_files/15minutevehicle/102.csv");
        let directions = Directions::new(
            LaneDirection::West,
            Some(LaneDirection::West),
            Some(LaneDirection::West),
        );
        let mut fifteen_min_volcount =
            FifteenMinuteVehicle::extract(path, 102, &directions).unwrap();
        fifteen_min_volcount.sort_unstable_by_key(|count| (count.date, count.time, count.lane));
        assert_eq!(fifteen_min_volcount.len(), 57);

        let count0 = fifteen_min_volcount.first().unwrap();
        assert_eq!(count0.lane, Some(1));
        assert_eq!(count0.direction, Some(LaneDirection::West));
        assert_eq!(count0.count, 49);
        let count1 = fifteen_min_volcount.get(1).unwrap();
        assert_eq!(count1.lane, Some(2));
        assert_eq!(count1.direction, Some(LaneDirection::West));
        assert_eq!(count1.count, 68);
        let count2 = fifteen_min_volcount.get(2).unwrap();
        assert_eq!(count2.lane, Some(3));
        assert_eq!(count2.direction, Some(LaneDirection::West));
        assert_eq!(count2.count, 10);
    }

    #[test]
    fn extract_fifteen_min_vehicle_errs_when_dirs_mismatch_in_filename_and_data_103() {
        let path = Path::new("test_files/15minutevehicle/103.csv");
        let directions = Directions::new(
            LaneDirection::South,
            Some(LaneDirection::South),
            Some(LaneDirection::South),
        );

        assert!(matches!(
            FifteenMinuteVehicle::extract(path, 103, &directions),
            Err(CountError::DirectionLenMisMatch)
        ))
    }

    #[test]
    fn extract_fifteen_min_bicycle_gets_correct_number_of_counts_167607() {
        let path = Path::new("test_files/15minutebicycle/167607.csv");
        let directions = Directions {
            direction1: LaneDirection::North,
            direction2: Some(LaneDirection::South),
            direction3: None,
        };
        let fifteen_min_volcount =
            FifteenMinuteBicycle::extract(path, 167607, &directions).unwrap();
        assert_eq!(fifteen_min_volcount.len(), 960);

        let north_sum = fifteen_min_volcount
            .iter()
            .filter(|count| count.cntdir == LaneDirection::North)
            .map(|count| count.volume)
            .sum::<u16>();
        let south_sum = fifteen_min_volcount
            .iter()
            .filter(|count| count.cntdir == LaneDirection::South)
            .map(|count| count.volume)
            .sum::<u16>();
        assert_eq!(north_sum, 491);
        assert_eq!(south_sum, 20);
    }

    #[test]
    fn extract_fifteen_min_pedestrian_gets_correct_number_of_counts167297() {
        let path = Path::new("test_files/15minutepedestrian/167297.csv");
        let directions = Directions {
            direction1: LaneDirection::North,
            direction2: Some(LaneDirection::South),
            direction3: None,
        };
        let fifteen_min_volcount =
            FifteenMinutePedestrian::extract(path, 167297, &directions).unwrap();
        assert_eq!(fifteen_min_volcount.len(), 1536);

        let north_sum = fifteen_min_volcount
            .iter()
            .filter(|count| count.cntdir == LaneDirection::North)
            .map(|count| count.volume)
            .sum::<u16>();
        let south_sum = fifteen_min_volcount
            .iter()
            .filter(|count| count.cntdir == LaneDirection::South)
            .map(|count| count.volume)
            .sum::<u16>();
        assert_eq!(north_sum, 1281);
        assert_eq!(south_sum, 1201);
    }

    #[test]
    fn count_type_from_location_correct_ind_veh() {
        let count_type =
            InputCount::from_parent_dir(Path::new("/vehicle_only/count_data.csv")).unwrap();
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
    fn num_nondata_rows_correct_15min_veh_sample() {
        let path = Path::new("test_files/15minutevehicle/168193.txt");
        assert_eq!(num_nondata_rows(path).unwrap(), 5);
    }

    #[test]
    fn count_type_and_num_nondata_rows_correct_ind_veh_sample() {
        let path = Path::new("test_files/vehicle_only/166905.txt");
        assert_eq!(num_nondata_rows(path).unwrap(), 4);
    }

    #[test]
    fn count_type_and_num_nondata_rows_correct_15min_bicycle_sample() {
        let path = Path::new("test_files/15minutebicycle/167607.csv");
        assert_eq!(num_nondata_rows(path).unwrap(), 3);
    }

    #[test]
    fn count_type_and_num_nondata_rows_correct_15min_pedestrian_sample() {
        let path = Path::new("test_files/15minutepedestrian/167297.csv");
        assert_eq!(num_nondata_rows(path).unwrap(), 3);
    }

    #[test]
    fn count_type_and_num_nondata_rows_errs_if_no_matching_header() {
        let path = Path::new("test_files/bad_header.txt");
        assert!(matches!(
            num_nondata_rows(path),
            Err(CountError::BadHeader(_))
        ))
    }

    #[test]
    fn num_nondata_rows_correct() {
        let path = Path::new("test_files/vehicle_only/166905.txt");
        let num_rows = num_nondata_rows(path).unwrap();
        assert_eq!(num_rows, 4);
    }

    #[test]
    fn parse_time_hh_mm_ss_p() {
        assert!(parse_time("03:00:00 PM").is_ok());
        assert!(parse_time("3:00:00 PM").is_ok());
        assert!(parse_time("12:00:00 AM").is_ok());
    }
    #[test]
    fn parse_time_hh_mm_ssp() {
        assert!(parse_time("03:00:00PM").is_ok());
        assert!(parse_time("3:00:00PM").is_ok());
        assert!(parse_time("12:00:00AM").is_ok());
    }
    #[test]
    fn parse_time_hh_mm_p() {
        assert!(parse_time("03:00 PM").is_ok());
        assert!(parse_time("3:00 PM").is_ok());
        assert!(parse_time("12:00 AM").is_ok());
    }
    #[test]
    fn parse_time_hh_mmp() {
        assert!(parse_time("03:00PM").is_ok());
        assert!(parse_time("3:00PM").is_ok());
        assert!(parse_time("12:00AM").is_ok());
    }
    #[test]
    fn parse_time_24hh_mm_ss() {
        assert!(parse_time("03:00:00").is_ok());
        assert!(parse_time("12:00:00").is_ok());
        assert!(parse_time("23:00:00").is_ok());
    }
    #[test]
    fn parse_date_correct() {
        assert!(parse_date("3/15/2025").is_ok());
        assert!(parse_date("03/15/2025").is_ok());
        assert!(parse_date("03/05/2025").is_ok());
        assert!(parse_date("03/5/2025").is_ok());
        assert!(parse_date("3-15-2025").is_ok());
        assert!(parse_date("03-15-2025").is_ok());
        assert!(parse_date("03-05-2025").is_ok());
        assert!(parse_date("03-5-2025").is_ok());
        assert!(parse_date("2025-03-05").is_ok());
    }

    #[test]
    fn parse_datetime_correct() {
        assert!(parse_datetime("2025-03-07 23:15:00").is_ok());
        assert!(parse_datetime("2025-03-07 23:15").is_ok());
    }
}

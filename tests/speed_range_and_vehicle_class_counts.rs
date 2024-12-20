//! Since these two types of counts - `SpeedRangeCount` and `VehicleClassCount` share so much
//! code and are created together, they are also tested together.

use std::path::Path;

use chrono::NaiveDateTime;

use traffic_counts::{extract_from_file::Extract, intermediate::*, *};

#[test]
fn speed_binning_is_correct() {
    // Initialize count with the first speed of 0.0.
    let mut speed_count = SpeedRangeCount::first(123, LaneDirection::West, 0.0);

    // s1
    speed_count.insert(-0.0);
    speed_count.insert(0.1);
    speed_count.insert(15.0);

    // s2
    speed_count.insert(15.1);
    speed_count.insert(20.0);

    // s3
    speed_count.insert(20.1);
    speed_count.insert(25.0);

    // s4
    speed_count.insert(25.1);
    speed_count.insert(30.0);

    // s5
    speed_count.insert(30.1);
    speed_count.insert(35.0);

    // s6
    speed_count.insert(35.1);
    speed_count.insert(40.0);

    // s7
    speed_count.insert(40.1);
    speed_count.insert(45.0);

    // s8
    speed_count.insert(45.1);
    speed_count.insert(50.0);

    // s9
    speed_count.insert(50.1);
    speed_count.insert(55.0);

    // s10
    speed_count.insert(55.1);
    speed_count.insert(60.0);

    // s11
    speed_count.insert(60.1);
    speed_count.insert(65.0);

    // s12
    speed_count.insert(65.1);
    speed_count.insert(70.0);

    // s13
    speed_count.insert(70.1);
    speed_count.insert(75.0);

    // s14
    speed_count.insert(75.1);
    speed_count.insert(100.0);
    speed_count.insert(120.0);

    assert_eq!(speed_count.s1, 4);
    assert_eq!(speed_count.s2, 2);
    assert_eq!(speed_count.s3, 2);
    assert_eq!(speed_count.s4, 2);
    assert_eq!(speed_count.s5, 2);
    assert_eq!(speed_count.s6, 2);
    assert_eq!(speed_count.s7, 2);
    assert_eq!(speed_count.s8, 2);
    assert_eq!(speed_count.s9, 2);
    assert_eq!(speed_count.s10, 2);
    assert_eq!(speed_count.s11, 2);
    assert_eq!(speed_count.s12, 2);
    assert_eq!(speed_count.s13, 2);
    assert_eq!(speed_count.s14, 3);
    assert_eq!(speed_count.total, 31);
}

#[test]
fn empty_periods_created_correctly_166905() {
    let path = Path::new("test_files/vehicle/166905-ew-40972-35.txt");
    let individual_vehicles = IndividualVehicle::extract(path).unwrap();
    let field_metadata = FieldMetadata::from_path(path).unwrap();

    let (mut speed_range_count, mut vehicle_class_count) = create_speed_and_class_count(
        field_metadata,
        individual_vehicles,
        TimeInterval::FifteenMin,
    );

    speed_range_count.sort_unstable_by_key(|count| (count.date, count.time.time(), count.lane));
    vehicle_class_count.sort_unstable_by_key(|count| (count.date, count.time.time(), count.lane));

    // total number of periods
    assert_eq!(speed_range_count.len(), 386);

    // periods with 0 vehicles
    let empty_periods = speed_range_count.iter().filter(|c| c.total == 0).count();
    assert_eq!(empty_periods, 23);

    // total number of periods
    assert_eq!(vehicle_class_count.len(), 386);

    // periods with 0 vehicles
    let empty_periods = vehicle_class_count.iter().filter(|c| c.total == 0).count();
    assert_eq!(empty_periods, 23);

    // first and last periods
    let expected_first_dt =
        NaiveDateTime::parse_from_str("2023-11-06 10:45", "%Y-%m-%d %H:%M").unwrap();
    assert_eq!(
        NaiveDateTime::new(
            speed_range_count.first().unwrap().date,
            speed_range_count.first().unwrap().time.time()
        ),
        expected_first_dt
    );
    assert_eq!(
        NaiveDateTime::new(
            vehicle_class_count.first().unwrap().date,
            vehicle_class_count.first().unwrap().time.time()
        ),
        expected_first_dt
    );
    let expected_last_dt =
        NaiveDateTime::parse_from_str("2023-11-08 10:45", "%Y-%m-%d %H:%M").unwrap();
    assert_eq!(
        NaiveDateTime::new(
            speed_range_count.last().unwrap().date,
            speed_range_count.last().unwrap().time.time()
        ),
        expected_last_dt
    );
    assert_eq!(
        NaiveDateTime::new(
            vehicle_class_count.last().unwrap().date,
            vehicle_class_count.last().unwrap().time.time()
        ),
        expected_last_dt
    );

    // verify last period total (lane 2)
    assert_eq!(speed_range_count.last().unwrap().total, 17);
    assert_eq!(vehicle_class_count.last().unwrap().total, 17);
}

#[test]
fn counts_created_correctly_165367() {
    let path = Path::new("test_files/vehicle/165367-ee-38397-45.txt");
    let individual_vehicles = IndividualVehicle::extract(path).unwrap();
    let field_metadata = FieldMetadata::from_path(path).unwrap();

    let (mut speed_range_count, mut vehicle_class_count) = create_speed_and_class_count(
        field_metadata,
        individual_vehicles,
        TimeInterval::FifteenMin,
    );

    speed_range_count.sort_unstable_by_key(|count| (count.date, count.time.time(), count.lane));
    vehicle_class_count.sort_unstable_by_key(|count| (count.date, count.time.time(), count.lane));

    // total number of periods
    assert_eq!(speed_range_count.len(), 756);
    // periods with 0 vehicles
    let empty_periods = speed_range_count.iter().filter(|c| c.total == 0).count();
    assert_eq!(empty_periods, 15);

    // total number of periods
    assert_eq!(vehicle_class_count.len(), 756);

    // periods with 0 vehicles, including verifying the time of the first occurence
    let mut empty_periods = vehicle_class_count
        .iter()
        .filter(|c| c.total == 0)
        .collect::<Vec<_>>();
    empty_periods.sort_unstable_by_key(|count| (count.date, count.time.time(), count.lane));
    assert_eq!(empty_periods.len(), 15);
    assert_eq!(empty_periods[0].total, 0);
    assert_eq!(
        NaiveDateTime::new(empty_periods[0].date, empty_periods[0].time.time()),
        NaiveDateTime::parse_from_str("2023-11-07 1:00", "%Y-%m-%d %H:%M").unwrap()
    );
    assert_eq!(empty_periods[0].lane, Some(1));

    // first and last periods
    let expected_first_dt =
        NaiveDateTime::parse_from_str("2023-11-06 11:45", "%Y-%m-%d %H:%M").unwrap();
    assert_eq!(
        NaiveDateTime::new(
            speed_range_count.first().unwrap().date,
            speed_range_count.first().unwrap().time.time()
        ),
        expected_first_dt
    );
    assert_eq!(
        NaiveDateTime::new(
            vehicle_class_count.first().unwrap().date,
            vehicle_class_count.first().unwrap().time.time()
        ),
        expected_first_dt
    );
    let expected_last_dt =
        NaiveDateTime::parse_from_str("2023-11-10 10:00", "%Y-%m-%d %H:%M").unwrap();
    assert_eq!(
        NaiveDateTime::new(
            speed_range_count.last().unwrap().date,
            speed_range_count.last().unwrap().time.time()
        ),
        expected_last_dt
    );
    assert_eq!(
        NaiveDateTime::new(
            vehicle_class_count.last().unwrap().date,
            vehicle_class_count.last().unwrap().time.time()
        ),
        expected_last_dt
    );

    // verify last period total (lane 2)
    assert_eq!(speed_range_count.last().unwrap().total, 36);
    assert_eq!(vehicle_class_count.last().unwrap().total, 36);
}

#[test]
fn counts_created_correctly_101() {
    // This file was made up, based on another, but with just over an hour of counts.
    let path = Path::new("test_files/vehicle/101-eee-21-35.csv");
    let individual_vehicles = IndividualVehicle::extract(path).unwrap();
    let field_metadata = FieldMetadata::from_path(path).unwrap();

    let (mut speed_range_count, mut vehicle_class_count) = create_speed_and_class_count(
        field_metadata,
        individual_vehicles,
        TimeInterval::FifteenMin,
    );

    speed_range_count.sort_unstable_by_key(|count| (count.date, count.time.time(), count.lane));
    vehicle_class_count.sort_unstable_by_key(|count| (count.date, count.time.time(), count.lane));

    // total number of periods
    assert_eq!(speed_range_count.len(), 15); // 5 periods x 3 lanes
    assert_eq!(vehicle_class_count.len(), 15);

    // periods with 0 vehicles
    let empty_periods = speed_range_count.iter().filter(|c| c.total == 0).count();
    assert_eq!(empty_periods, 0);

    // first and last periods
    let expected_first_dt =
        NaiveDateTime::parse_from_str("2023-11-06 10:45", "%Y-%m-%d %H:%M").unwrap();
    assert_eq!(
        NaiveDateTime::new(
            speed_range_count.first().unwrap().date,
            speed_range_count.first().unwrap().time.time()
        ),
        expected_first_dt
    );
    assert_eq!(
        NaiveDateTime::new(
            vehicle_class_count.first().unwrap().date,
            vehicle_class_count.first().unwrap().time.time()
        ),
        expected_first_dt
    );
    let expected_last_dt =
        NaiveDateTime::parse_from_str("2023-11-06 11:45", "%Y-%m-%d %H:%M").unwrap();
    assert_eq!(
        NaiveDateTime::new(
            speed_range_count.last().unwrap().date,
            speed_range_count.last().unwrap().time.time()
        ),
        expected_last_dt
    );
    assert_eq!(
        NaiveDateTime::new(
            vehicle_class_count.last().unwrap().date,
            vehicle_class_count.last().unwrap().time.time()
        ),
        expected_last_dt
    );

    // Verify period in middle (11:15-11:30)
    assert_eq!(vehicle_class_count[6].total, 24);
    assert_eq!(vehicle_class_count[6].lane, Some(1));
    assert_eq!(vehicle_class_count[6].direction, Some(LaneDirection::East));
    assert_eq!(vehicle_class_count[7].total, 18);
    assert_eq!(vehicle_class_count[7].lane, Some(2));
    assert_eq!(vehicle_class_count[7].direction, Some(LaneDirection::East));
    assert_eq!(vehicle_class_count[8].total, 2);
    assert_eq!(vehicle_class_count[8].lane, Some(3));
    assert_eq!(vehicle_class_count[8].direction, Some(LaneDirection::East));

    // verify last period total (lane 3)
    assert_eq!(speed_range_count.last().unwrap().total, 5);
    assert_eq!(vehicle_class_count.last().unwrap().total, 5);
}

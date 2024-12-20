use std::path::Path;

use chrono::NaiveDate;

use traffic_counts::{
    denormalize::create_non_normal_speedavg_count, extract_from_file::Extract, LaneDirection, *,
};

#[test]
fn create_non_normal_avgspeed_count_166905_is_correct() {
    // two directions, two lanes
    let path = Path::new("test_files/vehicle/166905-ew-40972-35.txt");
    let counted_vehicles = IndividualVehicle::extract(path).unwrap();
    let field_metadata = FieldMetadata::from_path(path).unwrap();
    let mut non_normal_count = create_non_normal_speedavg_count(field_metadata, counted_vehicles);
    assert_eq!(non_normal_count.len(), 6);

    // Sort by date, and then lane, so elements of the vec are in an expected order to test.
    non_normal_count.sort_unstable_by_key(|count| (count.date, count.lane));

    // Ensure order is what we expect/count starts at correct times.
    assert_eq!(
        non_normal_count[0].date,
        NaiveDate::from_ymd_opt(2023, 11, 6).unwrap()
    );
    assert!(non_normal_count[0].am9.is_none());
    assert!(non_normal_count[0].am10.is_some());
    assert_eq!(non_normal_count[0].direction, Some(LaneDirection::East));
    assert_eq!(non_normal_count[0].lane, Some(1));

    assert_eq!(
        non_normal_count[1].date,
        NaiveDate::from_ymd_opt(2023, 11, 6).unwrap()
    );
    assert!(non_normal_count[1].am9.is_none());
    assert!(non_normal_count[1].am10.is_some());
    assert_eq!(non_normal_count[1].direction, Some(LaneDirection::West));
    assert_eq!(non_normal_count[1].lane, Some(2));

    assert_eq!(
        non_normal_count[5].date,
        NaiveDate::from_ymd_opt(2023, 11, 8).unwrap()
    );
    assert!(non_normal_count[5].am10.is_some());
    assert!(non_normal_count[5].am11.is_none());
    assert_eq!(non_normal_count[5].direction, Some(LaneDirection::West));
    assert_eq!(non_normal_count[5].lane, Some(2));

    // spotcheck averages
    assert_eq!(format!("{:.2}", non_normal_count[0].am11.unwrap()), "30.36");
    assert_eq!(format!("{:.2}", non_normal_count[1].am11.unwrap()), "32.71");
    assert_eq!(format!("{:.2}", non_normal_count[3].pm5.unwrap()), "31.94");
    assert_eq!(format!("{:.2}", non_normal_count[4].am9.unwrap()), "31.63");
}

#[test]
fn create_non_normal_avgspeed_count_165367_is_correct() {
    // one direction, two lanes
    let path = Path::new("test_files/vehicle/165367-ee-38397-45.txt");
    let counted_vehicles = IndividualVehicle::extract(path).unwrap();
    let field_metadata = FieldMetadata::from_path(path).unwrap();
    let mut non_normal_count = create_non_normal_speedavg_count(field_metadata, counted_vehicles);
    assert_eq!(non_normal_count.len(), 10);

    // Sort by date, and then lane, so elements of the vec are in an expected order to test.
    non_normal_count.sort_unstable_by_key(|count| (count.date, count.lane));

    // Ensure order is what we expect/count starts at correct times.
    assert_eq!(
        non_normal_count[0].date,
        NaiveDate::from_ymd_opt(2023, 11, 6).unwrap()
    );
    assert!(non_normal_count[0].am10.is_none());
    assert!(non_normal_count[0].am11.is_some());
    assert_eq!(non_normal_count[0].direction, Some(LaneDirection::East));
    assert_eq!(non_normal_count[0].lane, Some(1));

    assert_eq!(
        non_normal_count[1].date,
        NaiveDate::from_ymd_opt(2023, 11, 6).unwrap()
    );
    assert!(non_normal_count[1].am10.is_none());
    assert!(non_normal_count[1].am11.is_some());
    assert_eq!(non_normal_count[1].direction, Some(LaneDirection::East));
    assert_eq!(non_normal_count[1].lane, Some(2));

    assert_eq!(
        non_normal_count[8].date,
        NaiveDate::from_ymd_opt(2023, 11, 10).unwrap()
    );
    assert!(non_normal_count[8].am10.is_some());
    assert!(non_normal_count[8].am11.is_none());
    assert_eq!(non_normal_count[8].direction, Some(LaneDirection::East));
    assert_eq!(non_normal_count[8].lane, Some(1));

    // spotcheck averages
    assert_eq!(format!("{:.2}", non_normal_count[0].pm4.unwrap()), "38.34");
    assert_eq!(format!("{:.2}", non_normal_count[1].pm6.unwrap()), "37.68");
    assert_eq!(format!("{:.2}", non_normal_count[5].am9.unwrap()), "39.14");
    assert_eq!(format!("{:.2}", non_normal_count[6].pm12.unwrap()), "36.49");
    assert_eq!(format!("{:.2}", non_normal_count[8].am3.unwrap()), "43.14");
    assert_eq!(format!("{:.2}", non_normal_count[9].am3.unwrap()), "45.36");
}

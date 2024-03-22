use std::path::Path;

use time::macros::date;

use traffic_counts::{extract_from_file::Extract, Direction, *};

#[test]
fn create_non_normal_vol_count_correct_num_records_and_total_count_166905() {
    // two directions, two lanes
    let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
    let counted_vehicles = IndividualVehicle::extract(path).unwrap();
    let metadata = CountMetadata::from_path(path).unwrap();
    let mut non_normal_count = create_non_normal_vol_count(metadata, counted_vehicles);
    assert_eq!(non_normal_count.len(), 6);

    // Sort by date, and then channel, so elements of the vec are in an expected order to test.
    non_normal_count.sort_unstable_by_key(|count| (count.date, count.channel));

    // Ensure order is what we expect.
    assert_eq!(non_normal_count[0].date, date!(2023 - 11 - 06));
    assert_eq!(non_normal_count[0].direction, Direction::East);
    assert_eq!(non_normal_count[0].channel, 1);
    assert_eq!(non_normal_count[1].date, date!(2023 - 11 - 06));
    assert_eq!(non_normal_count[1].direction, Direction::West);
    assert_eq!(non_normal_count[1].channel, 2);
    assert_eq!(non_normal_count[5].date, date!(2023 - 11 - 08));
    assert_eq!(non_normal_count[5].direction, Direction::West);
    assert_eq!(non_normal_count[5].channel, 2);

    // the non-complete hours are not included in the resulting data structure
    assert!(non_normal_count[0].am10.is_none());
    assert!(non_normal_count[1].am10.is_none());
    assert!(non_normal_count[4].am10.is_none());
    assert!(non_normal_count[5].am10.is_none());

    // Test total counts.
    assert_eq!(
        non_normal_count[0].totalcount.unwrap() + non_normal_count[1].totalcount.unwrap(),
        2893
    );
    assert_eq!(
        non_normal_count[2].totalcount.unwrap() + non_normal_count[3].totalcount.unwrap(),
        4450
    );
    assert_eq!(
        non_normal_count[4].totalcount.unwrap() + non_normal_count[5].totalcount.unwrap(),
        1173
    );
}

#[test]
fn create_non_normal_vol_count_correct_num_records_and_total_count_165367() {
    // one direction, two lanes
    let path = Path::new("test_files/vehicle/kh-165367-ee-38397-45.txt");
    let counted_vehicles = IndividualVehicle::extract(path).unwrap();
    let metadata = CountMetadata::from_path(path).unwrap();
    let mut non_normal_count = create_non_normal_vol_count(metadata, counted_vehicles);
    assert_eq!(non_normal_count.len(), 10);

    // Sort by date, and then channel, so elements of the vec are in an expected order to test.
    non_normal_count.sort_unstable_by_key(|count| (count.date, count.channel));

    // Ensure order is what we expect.
    assert_eq!(non_normal_count[0].date, date!(2023 - 11 - 06));
    assert_eq!(non_normal_count[0].direction, Direction::East);
    assert_eq!(non_normal_count[0].channel, 1);
    assert_eq!(non_normal_count[1].date, date!(2023 - 11 - 06));
    assert_eq!(non_normal_count[1].direction, Direction::East);
    assert_eq!(non_normal_count[1].channel, 2);
    assert_eq!(non_normal_count[9].date, date!(2023 - 11 - 10));
    assert_eq!(non_normal_count[9].direction, Direction::East);
    assert_eq!(non_normal_count[9].channel, 2);

    // the non-complete hours are not included in the resulting data structure
    assert!(non_normal_count[0].am11.is_none());
    assert!(non_normal_count[1].am11.is_none());
    assert!(non_normal_count[8].am10.is_none());
    assert!(non_normal_count[9].am10.is_none());

    // Test total counts.
    assert_eq!(
        non_normal_count[0].totalcount.unwrap() + non_normal_count[1].totalcount.unwrap(),
        8636
    );
    assert_eq!(
        non_normal_count[2].totalcount.unwrap() + non_normal_count[3].totalcount.unwrap(),
        14751
    );
    assert_eq!(
        non_normal_count[4].totalcount.unwrap() + non_normal_count[5].totalcount.unwrap(),
        15298
    );
    assert_eq!(
        non_normal_count[6].totalcount.unwrap() + non_normal_count[7].totalcount.unwrap(),
        15379
    );
    assert_eq!(
        non_normal_count[8].totalcount.unwrap() + non_normal_count[9].totalcount.unwrap(),
        4220
    );
}

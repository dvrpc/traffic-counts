use std::path::Path;

use time::macros::date;

use traffic_counts::{extract_from_file::Extract, Direction, *};

#[test]
fn create_non_normal_speedavg_count_correct_num_records_and_keys() {
    // one direction, two lanes
    let path = Path::new("test_files/vehicle/kh-165367-ee-38397-45.txt");
    let counted_vehicles = CountedVehicle::extract(path).unwrap();
    let metadata = CountMetadata::from_path(path).unwrap();
    let non_normal_count = create_non_normal_speedavg_count(metadata, counted_vehicles);
    assert_eq!(non_normal_count.len(), 10);

    // two directions, two lanes
    let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
    let counted_vehicles = CountedVehicle::extract(path).unwrap();
    let metadata = CountMetadata::from_path(path).unwrap();
    let non_normal_count = create_non_normal_speedavg_count(metadata, counted_vehicles);
    assert_eq!(non_normal_count.len(), 6);

    let keys = vec![
        NonNormalCountKey {
            dvrpc_num: 166905,
            date: date!(2023 - 11 - 06),
            direction: Direction::East,
            channel: 1,
        },
        NonNormalCountKey {
            dvrpc_num: 166905,
            date: date!(2023 - 11 - 06),
            direction: Direction::West,
            channel: 2,
        },
        NonNormalCountKey {
            dvrpc_num: 166905,
            date: date!(2023 - 11 - 07),
            direction: Direction::West,
            channel: 2,
        },
        NonNormalCountKey {
            dvrpc_num: 166905,
            date: date!(2023 - 11 - 07),
            direction: Direction::East,
            channel: 1,
        },
        NonNormalCountKey {
            dvrpc_num: 166905,
            date: date!(2023 - 11 - 08),
            direction: Direction::West,
            channel: 2,
        },
        NonNormalCountKey {
            dvrpc_num: 166905,
            date: date!(2023 - 11 - 08),
            direction: Direction::East,
            channel: 1,
        },
    ];

    for key in keys {
        assert!(non_normal_count.contains_key(&key));
    }
}

#[test]
fn create_non_normal_speedavg_count_spot_check_averages() {
    // 165367
    // 1 direction, 2 lanes
    let path = Path::new("test_files/vehicle/kh-165367-ee-38397-45.txt");
    let counted_vehicles = CountedVehicle::extract(path).unwrap();
    let metadata = CountMetadata::from_path(path).unwrap();
    let non_normal_count = create_non_normal_speedavg_count(metadata, counted_vehicles);
    let day1_east1_key = NonNormalCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 06),
        direction: Direction::East,
        channel: 1,
    };
    let day1_east2_key = NonNormalCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 06),
        direction: Direction::East,
        channel: 2,
    };
    let day2_east1_key = NonNormalCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 07),
        direction: Direction::East,
        channel: 1,
    };
    let day2_east2_key = NonNormalCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 07),
        direction: Direction::East,
        channel: 2,
    };
    let day3_east1_key = NonNormalCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 08),
        direction: Direction::East,
        channel: 1,
    };
    let day3_east2_key = NonNormalCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 08),
        direction: Direction::East,
        channel: 2,
    };
    let day4_east1_key = NonNormalCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 09),
        direction: Direction::East,
        channel: 1,
    };
    let day4_east2_key = NonNormalCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 09),
        direction: Direction::East,
        channel: 2,
    };
    let day5_east1_key = NonNormalCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 10),
        direction: Direction::East,
        channel: 1,
    };
    let day5_east2_key = NonNormalCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 10),
        direction: Direction::East,
        channel: 2,
    };

    // the non-complete hours are not included in the resulting data structure
    assert!(non_normal_count[&day1_east1_key].am11.is_none());
    assert!(non_normal_count[&day1_east2_key].am11.is_none());
    assert!(non_normal_count[&day5_east1_key].am10.is_none());
    assert!(non_normal_count[&day5_east2_key].am10.is_none());

    // spotcheck averages
    assert_eq!(
        format!("{:.2}", non_normal_count[&day1_east1_key].pm4.unwrap()),
        "38.34"
    );
    assert_eq!(
        format!("{:.2}", non_normal_count[&day1_east2_key].pm6.unwrap()),
        "37.68"
    );
    assert_eq!(
        format!("{:.2}", non_normal_count[&day3_east2_key].am9.unwrap()),
        "39.14"
    );
    assert_eq!(
        format!("{:.2}", non_normal_count[&day4_east1_key].pm12.unwrap()),
        "36.49"
    );
    assert_eq!(
        format!("{:.2}", non_normal_count[&day5_east1_key].am3.unwrap()),
        "43.14"
    );
    assert_eq!(
        format!("{:.2}", non_normal_count[&day5_east2_key].am3.unwrap()),
        "45.36"
    );

    // 166905
    // two directions, 2 lanes
    let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
    let counted_vehicles = CountedVehicle::extract(path).unwrap();
    let metadata = CountMetadata::from_path(path).unwrap();
    let non_normal_count = create_non_normal_speedavg_count(metadata, counted_vehicles);

    let day1_east_key = NonNormalCountKey {
        dvrpc_num: 166905,
        date: date!(2023 - 11 - 06),
        direction: Direction::East,
        channel: 1,
    };
    let day1_west_key = NonNormalCountKey {
        dvrpc_num: 166905,
        date: date!(2023 - 11 - 06),
        direction: Direction::West,
        channel: 2,
    };
    let day2_east_key = NonNormalCountKey {
        dvrpc_num: 166905,
        date: date!(2023 - 11 - 07),
        direction: Direction::East,
        channel: 1,
    };
    let day2_west_key = NonNormalCountKey {
        dvrpc_num: 166905,
        date: date!(2023 - 11 - 07),
        direction: Direction::West,
        channel: 2,
    };
    let day3_east_key = NonNormalCountKey {
        dvrpc_num: 166905,
        date: date!(2023 - 11 - 08),
        direction: Direction::East,
        channel: 1,
    };
    let day3_west_key = NonNormalCountKey {
        dvrpc_num: 166905,
        date: date!(2023 - 11 - 08),
        direction: Direction::West,
        channel: 2,
    };

    // the non-complete hours are not included in the resulting data structure
    assert!(non_normal_count[&day1_east_key].am10.is_none());
    assert!(non_normal_count[&day1_west_key].am10.is_none());
    assert!(non_normal_count[&day3_east_key].am10.is_none());
    assert!(non_normal_count[&day3_west_key].am10.is_none());

    // spotcheck averages
    assert_eq!(
        format!("{:.2}", non_normal_count[&day1_east_key].am11.unwrap()),
        "30.36"
    );
    assert_eq!(
        format!("{:.2}", non_normal_count[&day1_west_key].am11.unwrap()),
        "32.71"
    );
    assert_eq!(
        format!("{:.2}", non_normal_count[&day2_west_key].pm5.unwrap()),
        "31.94"
    );
    assert_eq!(
        format!("{:.2}", non_normal_count[&day3_east_key].am9.unwrap()),
        "31.63"
    );
}

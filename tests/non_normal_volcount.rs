use std::path::Path;

use time::macros::date;

use traffic_counts::{extract_from_file::Extract, Direction, *};

#[test]
fn create_non_normal_vol_count_correct_num_records_and_keys() {
    // one direction, two lanes
    let path = Path::new("test_files/vehicle/kh-165367-ee-38397-45.txt");
    let counted_vehicles = IndividualVehicle::extract(path).unwrap();
    let metadata = CountMetadata::from_path(path).unwrap();
    let non_normal_count = create_non_normal_vol_count(metadata, counted_vehicles);
    assert_eq!(non_normal_count.len(), 10);

    // two directions, two lanes
    let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
    let counted_vehicles = IndividualVehicle::extract(path).unwrap();
    let metadata = CountMetadata::from_path(path).unwrap();
    let non_normal_count = create_non_normal_vol_count(metadata, counted_vehicles);
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
fn create_non_normal_volcount_correct_totals_by_day() {
    // 165367
    // 1 direction, 2 lanes
    let path = Path::new("test_files/vehicle/kh-165367-ee-38397-45.txt");
    let counted_vehicles = IndividualVehicle::extract(path).unwrap();
    let metadata = CountMetadata::from_path(path).unwrap();
    let non_normal_count = create_non_normal_vol_count(metadata, counted_vehicles);
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

    let day1_total = non_normal_count[&day1_east1_key].totalcount.unwrap()
        + non_normal_count[&day1_east2_key].totalcount.unwrap();
    let day2_total = non_normal_count[&day2_east1_key].totalcount.unwrap()
        + non_normal_count[&day2_east2_key].totalcount.unwrap();
    let day3_total = non_normal_count[&day3_east1_key].totalcount.unwrap()
        + non_normal_count[&day3_east2_key].totalcount.unwrap();
    let day4_total = non_normal_count[&day4_east1_key].totalcount.unwrap()
        + non_normal_count[&day4_east2_key].totalcount.unwrap();
    let day5_total = non_normal_count[&day5_east1_key].totalcount.unwrap()
        + non_normal_count[&day5_east2_key].totalcount.unwrap();
    assert_eq!(day1_total, 8636);
    assert_eq!(day2_total, 14751);
    assert_eq!(day3_total, 15298);
    assert_eq!(day4_total, 15379);
    assert_eq!(day5_total, 4220);

    // 166905
    // two directions, 2 lanes
    let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
    let counted_vehicles = IndividualVehicle::extract(path).unwrap();
    let metadata = CountMetadata::from_path(path).unwrap();
    let non_normal_volcount = create_non_normal_vol_count(metadata, counted_vehicles);

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
    let day1_total = non_normal_volcount[&day1_east_key].totalcount.unwrap()
        + non_normal_volcount[&day1_west_key].totalcount.unwrap();
    let day2_total = non_normal_volcount[&day2_east_key].totalcount.unwrap()
        + non_normal_volcount[&day2_west_key].totalcount.unwrap();
    let day3_total = non_normal_volcount[&day3_east_key].totalcount.unwrap()
        + non_normal_volcount[&day3_west_key].totalcount.unwrap();

    assert_eq!(day1_total, 2893);
    assert_eq!(day2_total, 4450);
    assert_eq!(day3_total, 1173);
}

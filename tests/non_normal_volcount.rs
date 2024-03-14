use std::path::Path;

use time::macros::date;

use traffic_counts::{extract_from_file::Extract, Direction, *};

#[test]
fn create_non_normal_volcount_correct_num_records() {
    // one direction
    let path = Path::new("test_files/vehicle/kh-165367-ee-38397-45.txt");
    let counted_vehicles = CountedVehicle::extract(path).unwrap();
    let metadata = CountMetadata::from_path(path).unwrap();
    let non_normal_volcount = create_non_normal_volcount(metadata, counted_vehicles);
    assert_eq!(non_normal_volcount.len(), 5);

    // two directions
    let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
    let counted_vehicles = CountedVehicle::extract(path).unwrap();
    let metadata = CountMetadata::from_path(path).unwrap();
    let non_normal_volcount = create_non_normal_volcount(metadata, counted_vehicles);
    assert_eq!(non_normal_volcount.len(), 6);

    let keys = vec![
        NonNormalVolCountKey {
            dvrpc_num: 166905,
            date: date!(2023 - 11 - 06),
            direction: Direction::East,
        },
        NonNormalVolCountKey {
            dvrpc_num: 166905,
            date: date!(2023 - 11 - 06),
            direction: Direction::West,
        },
        NonNormalVolCountKey {
            dvrpc_num: 166905,
            date: date!(2023 - 11 - 07),
            direction: Direction::West,
        },
        NonNormalVolCountKey {
            dvrpc_num: 166905,
            date: date!(2023 - 11 - 07),
            direction: Direction::East,
        },
        NonNormalVolCountKey {
            dvrpc_num: 166905,
            date: date!(2023 - 11 - 08),
            direction: Direction::West,
        },
        NonNormalVolCountKey {
            dvrpc_num: 166905,
            date: date!(2023 - 11 - 08),
            direction: Direction::East,
        },
    ];

    for key in keys {
        assert!(non_normal_volcount.contains_key(&key));
    }
}

#[test]
fn create_non_normal_volcount_correct_totals_by_day() {
    // one direction
    let path = Path::new("test_files/vehicle/kh-165367-ee-38397-45.txt");
    let counted_vehicles = CountedVehicle::extract(path).unwrap();
    let metadata = CountMetadata::from_path(path).unwrap();
    let non_normal_volcount = create_non_normal_volcount(metadata, counted_vehicles);
    let day1_key = NonNormalVolCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 06),
        direction: Direction::East,
    };
    let day2_key = NonNormalVolCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 07),
        direction: Direction::East,
    };
    let day3_key = NonNormalVolCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 08),
        direction: Direction::East,
    };
    let day4_key = NonNormalVolCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 09),
        direction: Direction::East,
    };
    let day5_key = NonNormalVolCountKey {
        dvrpc_num: 165367,
        date: date!(2023 - 11 - 10),
        direction: Direction::East,
    };
    assert_eq!(non_normal_volcount[&day1_key].totalcount, Some(8636));
    assert_eq!(non_normal_volcount[&day2_key].totalcount, Some(14751));
    assert_eq!(non_normal_volcount[&day3_key].totalcount, Some(15298));
    assert_eq!(non_normal_volcount[&day4_key].totalcount, Some(15379));
    assert_eq!(non_normal_volcount[&day5_key].totalcount, Some(4220));

    // two directions
    let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
    let counted_vehicles = CountedVehicle::extract(path).unwrap();
    let metadata = CountMetadata::from_path(path).unwrap();
    let non_normal_volcount = create_non_normal_volcount(metadata, counted_vehicles);

    let day1_east_key = NonNormalVolCountKey {
        dvrpc_num: 166905,
        date: date!(2023 - 11 - 06),
        direction: Direction::East,
    };
    let day1_west_key = NonNormalVolCountKey {
        dvrpc_num: 166905,
        date: date!(2023 - 11 - 06),
        direction: Direction::West,
    };
    let day2_east_key = NonNormalVolCountKey {
        dvrpc_num: 166905,
        date: date!(2023 - 11 - 07),
        direction: Direction::West,
    };
    let day2_west_key = NonNormalVolCountKey {
        dvrpc_num: 166905,
        date: date!(2023 - 11 - 07),
        direction: Direction::East,
    };
    let day3_east_key = NonNormalVolCountKey {
        dvrpc_num: 166905,
        date: date!(2023 - 11 - 08),
        direction: Direction::West,
    };
    let day3_west_key = NonNormalVolCountKey {
        dvrpc_num: 166905,
        date: date!(2023 - 11 - 08),
        direction: Direction::East,
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

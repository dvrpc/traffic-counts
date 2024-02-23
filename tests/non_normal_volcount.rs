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

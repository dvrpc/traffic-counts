use std::path::Path;

use time::{macros::date, Date, Weekday};

use traffic_counts::{aadv::determine_date, extract_from_file::Extract, GetDate, *};

// A type just for tests here.
struct TestCount {
    date: Date,
}

impl GetDate for TestCount {
    fn get_date(&self) -> Date {
        self.date.to_owned()
    }
}

#[test]
fn determine_date_skips_sunday() {
    let sunday = date!(2024 - 03 - 17);
    let monday = date!(2024 - 03 - 18);
    let tuesday = date!(2024 - 03 - 19);

    // Ensure we are using the days of the week we intend to.
    assert_eq!(sunday.weekday(), Weekday::Sunday);
    assert_eq!(monday.weekday(), Weekday::Monday);
    assert_eq!(tuesday.weekday(), Weekday::Tuesday);

    let test_counts = vec![
        TestCount { date: sunday },
        TestCount { date: monday },
        TestCount { date: tuesday },
    ];

    let determined_date = determine_date(test_counts).unwrap();
    assert_eq!(determined_date, monday)
}

#[test]
fn determine_date_skips_sat_and_sun() {
    let saturday = date!(2024 - 03 - 16);
    let sunday = date!(2024 - 03 - 17);
    let monday = date!(2024 - 03 - 18);
    let tuesday = date!(2024 - 03 - 19);

    // Ensure we are using the days of the week we intend to.
    assert_eq!(saturday.weekday(), Weekday::Saturday);
    assert_eq!(sunday.weekday(), Weekday::Sunday);
    assert_eq!(monday.weekday(), Weekday::Monday);
    assert_eq!(tuesday.weekday(), Weekday::Tuesday);

    let test_counts = vec![
        TestCount { date: saturday },
        TestCount { date: sunday },
        TestCount { date: monday },
        TestCount { date: tuesday },
    ];

    let determined_date = determine_date(test_counts).unwrap();
    assert_eq!(determined_date, monday)
}

#[test]
fn determine_date_skips_fri_sat_sun() {
    // Since Friday is the first day, and therefore not a full count, it should be skipped.
    // Weekend days skipped.
    let friday = date!(2024 - 03 - 15);
    let saturday = date!(2024 - 03 - 16);
    let sunday = date!(2024 - 03 - 17);
    let monday = date!(2024 - 03 - 18);
    let tuesday = date!(2024 - 03 - 19);

    // Ensure we are using the days of the week we intend to.
    assert_eq!(friday.weekday(), Weekday::Friday);
    assert_eq!(saturday.weekday(), Weekday::Saturday);
    assert_eq!(sunday.weekday(), Weekday::Sunday);
    assert_eq!(monday.weekday(), Weekday::Monday);
    assert_eq!(tuesday.weekday(), Weekday::Tuesday);

    let test_counts = vec![
        TestCount { date: friday },
        TestCount { date: saturday },
        TestCount { date: sunday },
        TestCount { date: monday },
        TestCount { date: tuesday },
    ];

    let determined_date = determine_date(test_counts).unwrap();
    assert_eq!(determined_date, monday)
}

#[test]
fn determine_date_skips_one_during_week_day() {
    let thursday = date!(2024 - 03 - 14);
    let friday = date!(2024 - 03 - 15);
    let saturday = date!(2024 - 03 - 16);
    let sunday = date!(2024 - 03 - 17);
    let monday = date!(2024 - 03 - 18);
    let tuesday = date!(2024 - 03 - 19);

    // Ensure we are using the days of the week we intend to.
    assert_eq!(thursday.weekday(), Weekday::Thursday);
    assert_eq!(friday.weekday(), Weekday::Friday);
    assert_eq!(saturday.weekday(), Weekday::Saturday);
    assert_eq!(sunday.weekday(), Weekday::Sunday);
    assert_eq!(monday.weekday(), Weekday::Monday);
    assert_eq!(tuesday.weekday(), Weekday::Tuesday);

    let test_counts = vec![
        TestCount { date: thursday },
        TestCount { date: friday },
        TestCount { date: saturday },
        TestCount { date: sunday },
        TestCount { date: monday },
        TestCount { date: tuesday },
    ];

    let determined_date = determine_date(test_counts).unwrap();
    assert_eq!(determined_date, friday)
}

#[test]
fn determine_date_skips_one_during_week_day_when_dates_out_of_order() {
    let thursday = date!(2024 - 03 - 14);
    let friday = date!(2024 - 03 - 15);
    let saturday = date!(2024 - 03 - 16);
    let sunday = date!(2024 - 03 - 17);
    let monday = date!(2024 - 03 - 18);
    let tuesday = date!(2024 - 03 - 19);

    // Ensure we are using the days of the week we intend to.
    assert_eq!(thursday.weekday(), Weekday::Thursday);
    assert_eq!(friday.weekday(), Weekday::Friday);
    assert_eq!(saturday.weekday(), Weekday::Saturday);
    assert_eq!(sunday.weekday(), Weekday::Sunday);
    assert_eq!(monday.weekday(), Weekday::Monday);
    assert_eq!(tuesday.weekday(), Weekday::Tuesday);

    let test_counts = vec![
        TestCount { date: monday },
        TestCount { date: tuesday },
        TestCount { date: thursday },
        TestCount { date: saturday },
        TestCount { date: friday },
        TestCount { date: sunday },
    ];

    let determined_date = determine_date(test_counts).unwrap();
    assert_eq!(determined_date, friday)
}

#[test]
fn determine_date_returns_none_when_count_empty() {
    let test_counts: Vec<TestCount> = vec![];
    assert!(determine_date(test_counts).is_none())
}

#[test]
fn determine_date_returns_none_when_only_one_count() {
    let monday = date!(2024 - 03 - 18);

    // Ensure we are using the days of the week we intend to.
    assert_eq!(monday.weekday(), Weekday::Monday);

    let test_counts = vec![TestCount { date: monday }];
    assert!(determine_date(test_counts).is_none())
}

#[test]
fn determine_date_correct_for_165367() {
    let path = Path::new("test_files/vehicle/kh-165367-ee-38397-45.txt");
    let counted_vehicles = IndividualVehicle::extract(path).unwrap();
    let correct_date = date!(2023 - 11 - 07);
    let determined_date = determine_date(counted_vehicles).unwrap();
    assert_eq!(correct_date, determined_date);
}

#[test]
fn determine_date_correct_for_166905() {
    let path = Path::new("test_files/vehicle/rc-166905-ew-40972-35.txt");
    let counted_vehicles = IndividualVehicle::extract(path).unwrap();
    let correct_date = date!(2023 - 11 - 07);
    let determined_date = determine_date(counted_vehicles).unwrap();
    assert_eq!(correct_date, determined_date);
}

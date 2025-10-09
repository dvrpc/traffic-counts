use std::path::Path;

use chrono::{NaiveDate, NaiveDateTime};

use traffic_counts::{
    create_binned_bicycle_vol_count, Directions, FifteenMinuteBicycle, IndividualBicycle,
    LaneDirection, TimeInterval,
};

#[test]
fn empty_periods_created_correctly_178955() {
    let path = Path::new("test_files/jamar_vehicle_and_bicycle/178955.csv");
    let directions = Directions {
        direction1: LaneDirection::South,
        direction2: None,
        direction3: None,
        one_way_bicycle: false,
    };

    let individual_bicycles = IndividualBicycle::extract(path).unwrap();

    let mut bikes15min = create_binned_bicycle_vol_count(
        TimeInterval::FifteenMin,
        178955,
        &directions,
        individual_bicycles,
    );

    bikes15min.sort_unstable_by_key(|count| count.datetime);

    // Total number of periods.
    assert_eq!(bikes15min.len(), 778);

    // Check first and last periods are expected datetimes.
    let expected_first_dt =
        NaiveDateTime::parse_from_str("2022-04-27 07:30", "%Y-%m-%d %H:%M").unwrap();
    assert_eq!(bikes15min.first().unwrap().datetime, expected_first_dt);
    let expected_last_dt =
        NaiveDateTime::parse_from_str("2022-05-05 09:45", "%Y-%m-%d %H:%M").unwrap();
    assert_eq!(bikes15min.last().unwrap().datetime, expected_last_dt);

    // Spotcheck - periods with 0 bicycles on the first day.
    let empty_periods = bikes15min
        .iter()
        .filter(|c| {
            c.datetime.date() == NaiveDate::from_ymd_opt(2022, 4, 27).unwrap() && c.volume == 0
        })
        .count();
    assert_eq!(empty_periods, 18);
}

#[test]
fn counts_correct_178955() {
    let path = Path::new("test_files/jamar_vehicle_and_bicycle/178955.csv");
    let directions = Directions {
        direction1: LaneDirection::South,
        direction2: None,
        direction3: None,
        one_way_bicycle: false,
    };
    let individual_bicycles = IndividualBicycle::extract(path).unwrap();

    let mut bikes15min = create_binned_bicycle_vol_count(
        TimeInterval::FifteenMin,
        178955,
        &directions,
        individual_bicycles,
    );

    bikes15min.sort_unstable_by_key(|count| count.datetime);

    // Total for all days.
    assert_eq!(bikes15min.iter().map(|c| c.volume).sum::<u16>(), 996);

    // Spotcheck.
    let april28 = bikes15min
        .iter()
        .filter(|c| c.datetime.date() == NaiveDate::from_ymd_opt(2022, 4, 28).unwrap());

    assert_eq!(april28.clone().map(|c| c.volume).sum::<u16>(), 198);

    assert_eq!(
        bikes15min
            .iter()
            .filter(|c| c.datetime.date() == NaiveDate::from_ymd_opt(2022, 5, 4).unwrap())
            .map(|c| c.volume)
            .sum::<u16>(),
        117
    );

    let last = FifteenMinuteBicycle {
        recordnum: 178955,
        datetime: NaiveDateTime::parse_from_str("2022-05-05 9:45", "%Y-%m-%d %H:%M").unwrap(),
        volume: 6,
        cntdir: LaneDirection::South,
    };

    assert_eq!(&last, bikes15min.iter().last().unwrap())
}

#[test]
fn counts_correct_178959() {
    let path = Path::new("test_files/jamar_vehicle_and_bicycle/178959.csv");
    let directions = Directions {
        direction1: LaneDirection::East,
        direction2: Some(LaneDirection::West),
        direction3: None,
        one_way_bicycle: false,
    };

    let individual_bicycles = IndividualBicycle::extract(path).unwrap();

    let mut bikes15min = create_binned_bicycle_vol_count(
        TimeInterval::FifteenMin,
        178959,
        &directions,
        individual_bicycles,
    );

    bikes15min.sort_unstable_by_key(|count| (count.datetime, count.cntdir));

    // Total number of periods.
    assert_eq!(bikes15min.len(), 3816);

    // Total for all days
    assert_eq!(
        bikes15min
            .iter()
            .filter(|c| c.cntdir == LaneDirection::East)
            .map(|c| c.volume)
            .sum::<u16>(),
        784
    );
    assert_eq!(
        bikes15min
            .iter()
            .filter(|c| c.cntdir == LaneDirection::West)
            .map(|c| c.volume)
            .sum::<u16>(),
        181
    );

    // Spotcheck
    assert_eq!(
        bikes15min
            .iter()
            .filter(|c| c.datetime.date() == NaiveDate::from_ymd_opt(2021, 7, 20).unwrap())
            .map(|c| c.volume)
            .sum::<u16>(),
        91
    );

    assert_eq!(
        bikes15min
            .iter()
            .filter(|c| c.datetime
                == NaiveDateTime::parse_from_str("2021-08-02 16:15", "%Y-%m-%d %H:%M").unwrap())
            .map(|c| c.volume)
            .sum::<u16>(),
        2
    );

    let dt = NaiveDateTime::parse_from_str("2021-07-30 16:15", "%Y-%m-%d %H:%M").unwrap();
    assert_eq!(
        bikes15min
            .iter()
            .find(|c| c.datetime == dt && c.cntdir == LaneDirection::East)
            .unwrap(),
        &FifteenMinuteBicycle {
            recordnum: 178959,
            datetime: dt,
            volume: 2,
            cntdir: LaneDirection::East,
        }
    );

    let last = FifteenMinuteBicycle {
        recordnum: 178959,
        datetime: NaiveDateTime::parse_from_str("2021-08-09 08:30", "%Y-%m-%d %H:%M").unwrap(),
        volume: 0,
        cntdir: LaneDirection::West,
    };

    assert_eq!(&last, bikes15min.iter().last().unwrap())
}

#[test]
fn wrong_way_gets_added_to_direction1_181261() {
    let path = Path::new("test_files/jamar_bicycle/181261_include_wrong_way.txt");
    let directions = Directions {
        direction1: LaneDirection::West,
        direction2: None,
        direction3: None,
        one_way_bicycle: true,
    };

    let individual_bicycles = IndividualBicycle::extract(path).unwrap();

    // There are 10 records in the test file, but one is class 15 (not a bicycle)
    assert_eq!(individual_bicycles.len(), 9);

    // 1st should be in lane 2, 2nd in lane 2.
    assert_eq!(individual_bicycles[0].lane, 2);
    assert_eq!(individual_bicycles[1].lane, 1);

    let bikes15min = create_binned_bicycle_vol_count(
        TimeInterval::FifteenMin,
        181261,
        &directions,
        individual_bicycles,
    );

    // Total number of periods.
    assert_eq!(bikes15min.len(), 2);

    // All (both) periods should only have cntdir of West (direction1 above).
    for count in bikes15min {
        assert_eq!(count.cntdir, LaneDirection::West)
    }
}

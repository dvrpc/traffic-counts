use std::path::Path;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

use traffic_counts::{
    create_binned_bicycle_vol_count, extract_from_file::Extract, FieldMetadata,
    FifteenMinuteBicycle, IndividualBicycle, TimeInterval,
};

#[test]
fn empty_periods_created_correctly_178955() {
    let path = Path::new("test_files/bicycle/178955-s-1613-25.csv");
    let individual_bicycles = IndividualBicycle::extract(path).unwrap();
    let field_metadata = FieldMetadata::from_path(path).unwrap();

    let mut bikes15min = create_binned_bicycle_vol_count(
        TimeInterval::FifteenMin,
        field_metadata,
        individual_bicycles,
    );

    bikes15min.sort_unstable_by_key(|count| (count.date, count.time.time()));

    // Total number of periods.
    assert_eq!(bikes15min.len(), 778);

    // Check first and last periods are expected datetimes.
    let expected_first_dt =
        NaiveDateTime::parse_from_str("2022-04-27 07:30", "%Y-%m-%d %H:%M").unwrap();
    assert_eq!(
        NaiveDateTime::new(
            bikes15min.first().unwrap().date,
            bikes15min.first().unwrap().time.time()
        ),
        expected_first_dt
    );
    let expected_last_dt =
        NaiveDateTime::parse_from_str("2022-05-05 09:45", "%Y-%m-%d %H:%M").unwrap();
    assert_eq!(
        NaiveDateTime::new(
            bikes15min.last().unwrap().date,
            bikes15min.last().unwrap().time.time()
        ),
        expected_last_dt
    );

    // Spotcheck - periods with 0 bicycles on the first day.
    let empty_periods = bikes15min
        .iter()
        .filter(|c| c.date == NaiveDate::from_ymd_opt(2022, 4, 27).unwrap() && c.total == 0)
        .count();
    assert_eq!(empty_periods, 18);
}

#[test]
fn counts_correct_178955() {
    let path = Path::new("test_files/bicycle/178955-s-1613-25.csv");
    let individual_bicycles = IndividualBicycle::extract(path).unwrap();
    let field_metadata = FieldMetadata::from_path(path).unwrap();

    let mut bikes15min = create_binned_bicycle_vol_count(
        TimeInterval::FifteenMin,
        field_metadata,
        individual_bicycles,
    );

    bikes15min.sort_unstable_by_key(|count| (count.date, count.time.time()));

    // Total for all days.
    assert_eq!(bikes15min.iter().map(|c| c.total).sum::<u16>(), 996);
    assert_eq!(bikes15min.iter().filter_map(|c| c.indir).sum::<u16>(), 996);
    assert_eq!(bikes15min.iter().filter_map(|c| c.outdir).sum::<u16>(), 0);

    // Spotcheck.
    let april28 = bikes15min
        .iter()
        .filter(|c| c.date == NaiveDate::from_ymd_opt(2022, 4, 28).unwrap());

    assert_eq!(april28.clone().map(|c| c.total).sum::<u16>(), 198);
    assert_eq!(april28.clone().filter_map(|c| c.indir).sum::<u16>(), 198);
    assert_eq!(april28.filter_map(|c| c.outdir).sum::<u16>(), 0);

    assert_eq!(
        bikes15min
            .iter()
            .filter(|c| c.date == NaiveDate::from_ymd_opt(2022, 5, 4).unwrap())
            .map(|c| c.total)
            .sum::<u16>(),
        117
    );

    let date = NaiveDate::from_ymd_opt(2022, 5, 5).unwrap();
    let last = FifteenMinuteBicycle {
        recordnum: 178955,
        date,
        time: date.and_time(NaiveTime::from_hms_opt(9, 45, 0).unwrap()),
        total: 6,
        indir: Some(6),
        outdir: Some(0),
    };

    assert_eq!(&last, bikes15min.iter().last().unwrap())
}

#[test]
fn counts_correct_178959() {
    let path = Path::new("test_files/bicycle/178959-ew-2060-25.csv");
    let individual_bicycles = IndividualBicycle::extract(path).unwrap();
    let field_metadata = FieldMetadata::from_path(path).unwrap();

    let mut bikes15min = create_binned_bicycle_vol_count(
        TimeInterval::FifteenMin,
        field_metadata,
        individual_bicycles,
    );

    bikes15min.sort_unstable_by_key(|count| (count.date, count.time.time()));

    // Total number of periods.
    assert_eq!(bikes15min.len(), 1908);

    // Total for all days
    assert_eq!(bikes15min.iter().map(|c| c.total).sum::<u16>(), 965);
    assert_eq!(bikes15min.iter().filter_map(|c| c.indir).sum::<u16>(), 784);
    assert_eq!(bikes15min.iter().filter_map(|c| c.outdir).sum::<u16>(), 181);

    // Spotcheck
    assert_eq!(
        bikes15min
            .iter()
            .filter(|c| c.date == NaiveDate::from_ymd_opt(2021, 7, 20).unwrap())
            .map(|c| c.total)
            .sum::<u16>(),
        91
    );

    assert_eq!(
        bikes15min
            .iter()
            .filter(|c| c.time
                == NaiveDate::from_ymd_opt(2021, 8, 2)
                    .unwrap()
                    .and_time(NaiveTime::from_hms_opt(16, 15, 0).unwrap()))
            .map(|c| c.total)
            .sum::<u16>(),
        2
    );

    let date = NaiveDate::from_ymd_opt(2021, 7, 30).unwrap();
    let time = date.and_time(NaiveTime::from_hms_opt(16, 15, 0).unwrap());
    assert_eq!(
        bikes15min.iter().find(|c| c.time == time).unwrap(),
        &FifteenMinuteBicycle {
            recordnum: 178959,
            date,
            time,
            total: 3,
            indir: Some(2),
            outdir: Some(1),
        }
    );

    let date = NaiveDate::from_ymd_opt(2021, 8, 9).unwrap();
    let last = FifteenMinuteBicycle {
        recordnum: 178959,
        date,
        time: date.and_time(NaiveTime::from_hms_opt(8, 30, 0).unwrap()),
        total: 2,
        indir: Some(2),
        outdir: Some(0),
    };

    assert_eq!(&last, bikes15min.iter().last().unwrap())
}

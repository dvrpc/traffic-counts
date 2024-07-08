use std::path::Path;

use traffic_counts::*;

#[test]
fn metadata_parse_from_path_ok() {
    let path = Path::new("some/path/rc-166905-e-40972-35.txt");
    let metadata = CountMetadata::from_path(path).unwrap();
    let expected_metadata = {
        CountMetadata {
            technician: "rc".to_string(),
            record_num: 166905,
            directions: Directions::new(Direction::East, None, None),
            counter_id: 40972,
            speed_limit: Some(35),
        }
    };
    assert_eq!(metadata, expected_metadata);

    let path = Path::new("some/path/rc-166905-ew-40972-35.txt");
    let metadata = CountMetadata::from_path(path).unwrap();
    let expected_metadata = {
        CountMetadata {
            technician: "rc".to_string(),
            record_num: 166905,
            directions: Directions::new(Direction::East, Some(Direction::West), None),
            counter_id: 40972,
            speed_limit: Some(35),
        }
    };
    assert_eq!(metadata, expected_metadata);

    let path = Path::new("some/path/rc-166905-eee-40972-35.txt");
    let metadata = CountMetadata::from_path(path).unwrap();
    let expected_metadata = {
        CountMetadata {
            technician: "rc".to_string(),
            record_num: 166905,
            directions: Directions::new(
                Direction::East,
                Some(Direction::East),
                Some(Direction::East),
            ),
            counter_id: 40972,
            speed_limit: Some(35),
        }
    };
    assert_eq!(metadata, expected_metadata);
}

#[test]
fn metadata_parse_from_path_ok_with_na_speed_limit() {
    let path = Path::new("some/path/rc-166905-ew-40972-na.txt");
    let metadata = CountMetadata::from_path(path).unwrap();
    let expected_metadata = {
        CountMetadata {
            technician: "rc".to_string(),
            record_num: 166905,
            directions: Directions::new(Direction::East, Some(Direction::West), None),
            counter_id: 40972,
            speed_limit: None,
        }
    };
    assert_eq!(metadata, expected_metadata)
}

#[test]
fn metadata_parse_from_path_errs_if_too_few_parts() {
    let path = Path::new("some/path/rc-166905-ew-40972.txt");
    assert!(matches!(
        CountMetadata::from_path(path),
        Err(CountError::InvalidFileName {
            problem: FileNameProblem::TooFewParts,
            ..
        })
    ))
}

#[test]
fn metadata_parse_from_path_errs_if_too_many_parts() {
    let path = Path::new("some/path/rc-166905-ew-40972-35-extra.txt");
    assert!(matches!(
        CountMetadata::from_path(path),
        Err(CountError::InvalidFileName {
            problem: FileNameProblem::TooManyParts,
            ..
        })
    ))
}

#[test]
fn metadata_parse_from_path_errs_if_technician_bad() {
    let path = Path::new("some/path/12-letters-ew-40972-35.txt");
    assert!(matches!(
        CountMetadata::from_path(path),
        Err(CountError::InvalidFileName {
            problem: FileNameProblem::InvalidTech,
            ..
        })
    ))
}

#[test]
fn metadata_parse_from_path_errs_if_record_num_bad() {
    let path = Path::new("some/path/rc-letters-ew-40972-35.txt");
    assert!(matches!(
        CountMetadata::from_path(path),
        Err(CountError::InvalidFileName {
            problem: FileNameProblem::InvalidRecordNum,
            ..
        })
    ))
}

#[test]
fn metadata_parse_from_path_errs_if_directions_bad() {
    let path = Path::new("some/path/rc-166905-eb-letters-35.txt");
    assert!(matches!(
        CountMetadata::from_path(path),
        Err(CountError::InvalidFileName {
            problem: FileNameProblem::InvalidDirections,
            ..
        })
    ));
    let path = Path::new("some/path/rc-166905-be-letters-35.txt");
    assert!(matches!(
        CountMetadata::from_path(path),
        Err(CountError::InvalidFileName {
            problem: FileNameProblem::InvalidDirections,
            ..
        })
    ));
    let path = Path::new("some/path/rc-166905-cc-letters-35.txt");
    assert!(matches!(
        CountMetadata::from_path(path),
        Err(CountError::InvalidFileName {
            problem: FileNameProblem::InvalidDirections,
            ..
        })
    ));
    let path = Path::new("some/path/rc-166905-eeee-letters-35.txt");
    assert!(matches!(
        CountMetadata::from_path(path),
        Err(CountError::InvalidFileName {
            problem: FileNameProblem::InvalidDirections,
            ..
        })
    ));
    let path = Path::new("some/path/rc-166905--letters-35.txt");
    assert!(matches!(
        CountMetadata::from_path(path),
        Err(CountError::InvalidFileName {
            problem: FileNameProblem::InvalidDirections,
            ..
        })
    ));
}

#[test]
fn metadata_parse_from_path_errs_if_counter_id_bad() {
    let path = Path::new("some/path/rc-166905-ew-letters-35.txt");
    assert!(matches!(
        CountMetadata::from_path(path),
        Err(CountError::InvalidFileName {
            problem: FileNameProblem::InvalidCounterID,
            ..
        })
    ))
}

#[test]
fn metadata_parse_from_path_errs_if_speedlimit_bad() {
    let path = Path::new("some/path/rc-166905-ew-40972-abc.txt");
    assert!(matches!(
        CountMetadata::from_path(path),
        Err(CountError::InvalidFileName {
            problem: FileNameProblem::InvalidSpeedLimit,
            ..
        })
    ))
}

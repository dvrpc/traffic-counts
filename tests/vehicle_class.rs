use traffic_counts::*;

#[test]
fn vehicle_class_from_bad_num_errs() {
    assert!(VehicleClass::from_num(16).is_err());
}

#[test]
fn vehicle_class_from_0_15_ok() {
    for i in 0..=15 {
        assert!(VehicleClass::from_num(i).is_ok())
    }
}

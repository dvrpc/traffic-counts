use std::path::Path;

use traffic_counts::{extract_from_file::Extract, *};

#[test]
fn create_non_normal_volcount_correct_num_records() {
    let path = Path::new("test_files/vehicle/kh-165367-ee-38397-45.txt");
    let counted_vehicles = CountedVehicle::extract(path).unwrap();

    let metadata = CountMetadata::from_path(path).unwrap();

    // Create records for the non-normalized TC_VOLCOUNT table.
    // (the one with specific hourly fields - AM12, AM1, etc. - rather than a single
    // hour field and count)
    let non_normal_volcount = create_non_normal_volcount(metadata, counted_vehicles);

    assert_eq!(non_normal_volcount.len(), 5)
}

# traffic-counts

To see library-level documentation, clone this repository and run `cargo doc --open`.

## Environment Variables

Environment variables should be included in a .env file:

```.env
DATA_DIR="data"
DB_USERNAME=DVRPCTC_TEST 
DB_PASSWORD='password here'
```

## Tests

NOTE: the tests in the `db` module require database access, which is limited to white-listed IPs. Therefore, tests are ignored by default. To include them in the test suite, use `cargo test -- --include-ignored`.


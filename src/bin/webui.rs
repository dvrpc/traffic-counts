use std::env;
use std::error::Error;
use std::fmt::{self, Display};
use std::str::FromStr;
use std::sync::Mutex;

use axum::{
    extract::{Form, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Redirect, Response},
    routing::get,
    Router,
};
use axum_extra::routing::RouterExt;
use chrono::Timelike;
use http::header::REFERER;
use oracle::{pool::Pool, Error as OracleError, ErrorKind as OracleErrorKind};
use rinja_axum::Template;
use serde::{de, Deserialize, Deserializer};
use tower_http::services::ServeDir;

use traffic_counts::{
    aadv::{self, AadvEntry},
    db::{self, crud::Crud, ImportLogEntry},
    denormalize::{NonNormalAvgSpeedCount, NonNormalVolCount},
    CountKind, FifteenMinuteBicycle, FifteenMinutePedestrian, FifteenMinuteVehicle, LaneDirection,
    Metadata, RoadDirection, TimeBinnedSpeedRangeCount, TimeBinnedVehicleClassCount,
};

const ADMIN_PATH: &str = "/admin";
const ADMIN_METADATA_LIST_PATH: &str = "/admin/metadata-list";
const ADMIN_METADATA_DETAIL_PATH: &str = "/admin/metadata-detail";
const ADMIN_METADATA_INSERT_PATH: &str = "/admin/insert";
const ADMIN_METADATA_INSERT_FROM_EXISTING_PATH: &str = "/admin/insert-from-existing";
const ADMIN_COUNT_DATA_PATH: &str = "/admin/count";
const ADMIN_IMPORT_LOG_PATH: &str = "/admin/import-log";
const ADMIN_AADV_PATH: &str = "/admin/aadv";
const RECORD_CREATION_LIMIT: u32 = db::RECORD_CREATION_LIMIT;

static MESSAGE: Mutex<Option<String>> = Mutex::new(None);

/// Get any message out of the global MESSAGE Mutex and reset it to None.
///
/// This is for displaying messages to user in templates. It is reset because messages
/// should only be shown once and not be persisted across responses.
pub fn burn_after_reading() -> String {
    let message = MESSAGE.lock().unwrap().clone().unwrap_or_default();
    *MESSAGE.lock().unwrap() = None;
    message
}

#[derive(Clone)]
struct AppState {
    conn_pool: Pool,
    num_metadata_records: u32,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().expect("Unable to load .env file.");
    let username = env::var("DB_USERNAME").unwrap();
    let password = env::var("DB_PASSWORD").unwrap();
    let conn_pool = db::create_pool(username, password).unwrap();

    let conn = conn_pool.get().unwrap();
    let num_metadata_records = db::get_metadata_total_recs(&conn).unwrap();
    let state = AppState {
        conn_pool,
        num_metadata_records,
    };
    let app = Router::new()
        .route("/", get(home))
        // `route_with_tsr` redirects any url with a trailing slash to the same one without
        // the trailing slash.
        // It's from the axum_extra crate's `RouteExt`.
        .route_with_tsr(ADMIN_PATH, get(admin))
        .route_with_tsr(ADMIN_METADATA_LIST_PATH, get(get_metadata_list))
        .route_with_tsr(ADMIN_METADATA_DETAIL_PATH, get(get_metadata_detail))
        .route_with_tsr(
            ADMIN_METADATA_INSERT_PATH,
            get(get_insert).post(post_insert_empty),
        )
        .route_with_tsr(
            ADMIN_METADATA_INSERT_FROM_EXISTING_PATH,
            get(get_insert_from_existing).post(post_insert_from_existing),
        )
        .route_with_tsr(ADMIN_IMPORT_LOG_PATH, get(get_import_log))
        .route_with_tsr(ADMIN_COUNT_DATA_PATH, get(get_count_data))
        .route_with_tsr(ADMIN_AADV_PATH, get(get_aadv))
        .with_state(state)
        .nest_service("/static", ServeDir::new("static"));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

/// A trait to set the nav item button text & the heading for the main section of each template.
///
/// When implemented on a Template, it is callable with `heading()`.
pub trait Heading {
    const NAV_ITEM_TEXT: &str;
    fn heading(&self) -> String {
        Self::NAV_ITEM_TEXT.to_string()
    }
}

/// Query params used to filter for particular recordnum or clear filter.
#[derive(Debug, Deserialize)]
struct RecordnumFilterParams {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    recordnum: Option<u32>,
    clear: Option<String>,
}

/// The front page of the admin section.
///
/// It will sometimes be used to display successful posts and messages, in addition to the default
/// starting page.
#[derive(Template, Debug, Default)]
#[template(path = "admin/main.html")]
struct AdminMainTemplate {}

impl Heading for AdminMainTemplate {
    const NAV_ITEM_TEXT: &str = "Welcome";
}

async fn admin() -> AdminMainTemplate {
    AdminMainTemplate::default()
}

#[derive(Template, Debug, Default)]
#[template(path = "counts/metadata_list.html")]
struct AdminMetadataListTemplate {
    metadata: Vec<Metadata>,
    total_pages: u32,
    page: u32,
}

impl Heading for AdminMetadataListTemplate {
    const NAV_ITEM_TEXT: &str = "View Metadata Records";
}

#[derive(Deserialize)]
struct Page {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    page: Option<u32>,
}

/// Get list of all counts ([`Metadata`] records).
async fn get_metadata_list(
    State(state): State<AppState>,
    page: Query<Page>,
) -> AdminMetadataListTemplate {
    let results_per_page = 100;
    let page = page.0.page.unwrap_or(1);
    let total_pages = state.num_metadata_records / results_per_page + 1;
    let conn = state.conn_pool.get().unwrap();

    let mut template = AdminMetadataListTemplate {
        metadata: vec![],
        total_pages,
        page,
    };

    match db::get_metadata_paginated(&conn, Some((page - 1) * results_per_page), None) {
        Ok(v) => {
            template.metadata = v;
        }
        Err(e) => {
            *MESSAGE.lock().unwrap() = Some(format!("{e}"));
        }
    }
    template
}

#[derive(Template, Debug, Default)]
#[template(path = "counts/metadata_detail.html")]
struct AdminMetadataDetailTemplate {
    recordnum: Option<u32>,
    metadata: Option<Metadata>,
}

impl Heading for AdminMetadataDetailTemplate {
    const NAV_ITEM_TEXT: &str = "View Metadata";
}

/// Query params used to filter for particular recordnum or clear filter.
#[derive(Debug, Deserialize)]
struct AdminMetadataDetailParams {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    recordnum: Option<u32>,
}

/// Get count ([`Metadata`] record).
async fn get_metadata_detail(
    headers: HeaderMap,
    State(state): State<AppState>,
    params: Query<AdminMetadataDetailParams>,
) -> Response {
    let conn = state.conn_pool.get().unwrap();
    let params = params.0;

    let mut template = AdminMetadataDetailTemplate {
        recordnum: None,
        metadata: None,
    };

    if params.recordnum.is_none() {
        *MESSAGE.lock().unwrap() = Some("Please provide a record number.".to_string());
    } else if let Some(v) = params.recordnum {
        template.recordnum = Some(v);
        match db::get_metadata(&conn, v) {
            Ok(w) => {
                template.metadata = Some(w);
            }
            Err(e) => {
                // Handle the one error that is probable (no matching recordnum in db).
                if e.source().is_some_and(|v| {
                    matches!(
                        v.downcast_ref::<OracleError>().unwrap().kind(),
                        OracleErrorKind::NoDataFound
                    )
                }) {
                    *MESSAGE.lock().unwrap() = Some(format!("Record {v} not found."));
                } else {
                    *MESSAGE.lock().unwrap() = Some(format!("{e}"))
                }

                // Return to metadata list if coming from the search form there.
                if headers
                    .get(REFERER)
                    .is_some_and(|v| v.to_str().unwrap().contains(ADMIN_METADATA_LIST_PATH))
                {
                    return Redirect::to(ADMIN_METADATA_LIST_PATH).into_response();
                }
            }
        }
    }
    template.into_response()
}

#[derive(Template, Debug, Default)]
#[template(path = "counts/count_data.html")]
struct CountDataTemplate {
    recordnum: Option<u32>,
    non_normal_volume: Option<Vec<NonNormalVolCount>>,
    non_normal_avg_speed: Option<Vec<NonNormalAvgSpeedCount>>,
    fifteen_min_ped: Option<Vec<FifteenMinutePedestrian>>,
    fifteen_min_bike: Option<Vec<FifteenMinuteBicycle>>,
    fifteen_min_vehicle: Option<Vec<FifteenMinuteVehicle>>,
    fifteen_min_class: Option<Vec<TimeBinnedVehicleClassCount>>,
    fifteen_min_speed: Option<Vec<TimeBinnedSpeedRangeCount>>,
}

impl Heading for CountDataTemplate {
    const NAV_ITEM_TEXT: &str = "Count Data";
}
#[derive(Debug, Deserialize)]
enum CountDataFormat {
    Volume15Min,
    VolumeHourly,
    VolumeDayByHour,
    Class15Min,
    ClassHourly,
    Speed15Min,
    SpeedHourly,
    SpeedDayByHour,
}

impl Display for CountDataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CountDataFormat::Volume15Min => write!(f, "15-minute volume"),
            CountDataFormat::VolumeHourly => write!(f, "hourly volume"),
            CountDataFormat::VolumeDayByHour => write!(f, "volume by hour of day"),
            CountDataFormat::Class15Min => write!(f, "15-minute class"),
            CountDataFormat::ClassHourly => write!(f, "hourly class"),
            CountDataFormat::Speed15Min => write!(f, "15-minute speed"),
            CountDataFormat::SpeedHourly => write!(f, "hourly speed"),
            CountDataFormat::SpeedDayByHour => write!(f, "speed by hour of day"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct CountDataParams {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    recordnum: Option<u32>,
    format: Option<CountDataFormat>,
}

/// Get count data in various kinds/formats.
async fn get_count_data(
    State(state): State<AppState>,
    params: Query<CountDataParams>,
) -> CountDataTemplate {
    let conn = state.conn_pool.get().unwrap();
    let params = params.0;
    let mut template = CountDataTemplate {
        recordnum: None,
        non_normal_volume: None,
        non_normal_avg_speed: None,
        fifteen_min_ped: None,
        fifteen_min_bike: None,
        fifteen_min_vehicle: None,
        fifteen_min_class: None,
        fifteen_min_speed: None,
    };
    let recordnum = match params.recordnum {
        Some(v) => {
            template.recordnum = Some(v);
            v
        }
        None => {
            *MESSAGE.lock().unwrap() = Some("Please provide a record number.".to_string());
            return template;
        }
    };
    let format = match params.format {
        Some(v) => v,
        None => {
            *MESSAGE.lock().unwrap() =
                Some("Please provide the format for the data to be presented in.".to_string());
            return template;
        }
    };

    // Get the kind of count this, in order to check if the desired format is available for it.
    let count_kind = match db::get_count_kind(&conn, recordnum) {
        Ok(Some(v)) => v,
        Ok(None) => {
            *MESSAGE.lock().unwrap() = Some(
                "Cannot determine kind of count, and thus unable to fetch count data.".to_string(),
            );
            return template;
        }
        Err(_) => {
            *MESSAGE.lock().unwrap() = Some(
                "Error fetching count kind from database, and thus unable to fetch count data."
                    .to_string(),
            );

            return template;
        }
    };

    // Msg to user if no results are empty.
    fn no_records(format: CountDataFormat, recordnum: u32) -> Option<String> {
        Some(format!(
            "No {format} records found for recordnum {recordnum}."
        ))
    }

    // Get data according to format/count kind and put into appropriate variable of template.
    match format {
        CountDataFormat::Volume15Min => match count_kind {
            CountKind::FifteenMinVolume => match FifteenMinuteVehicle::select(&conn, recordnum) {
                Ok(v) if v.is_empty() => {
                    *MESSAGE.lock().unwrap() = no_records(CountDataFormat::Volume15Min, recordnum);
                }
                Ok(v) => template.fifteen_min_vehicle = Some(v),
                Err(e) => *MESSAGE.lock().unwrap() = Some(format!("{e}")),
            },
            CountKind::Bicycle1
            | CountKind::Bicycle2
            | CountKind::Bicycle3
            | CountKind::Bicycle4
            | CountKind::Bicycle5
            | CountKind::Bicycle6 => match FifteenMinuteBicycle::select(&conn, recordnum) {
                Ok(v) if v.is_empty() => {
                    *MESSAGE.lock().unwrap() = no_records(CountDataFormat::Volume15Min, recordnum);
                }
                Ok(v) => template.fifteen_min_bike = Some(v),
                Err(e) => *MESSAGE.lock().unwrap() = Some(format!("{e}")),
            },
            CountKind::Pedestrian | CountKind::Pedestrian2 => {
                match FifteenMinutePedestrian::select(&conn, recordnum) {
                    Ok(v) if v.is_empty() => {
                        *MESSAGE.lock().unwrap() =
                            no_records(CountDataFormat::Volume15Min, recordnum)
                    }
                    Ok(v) => template.fifteen_min_ped = Some(v),
                    Err(e) => *MESSAGE.lock().unwrap() = Some(format!("{e}")),
                }
            }
            _ => (),
        },
        CountDataFormat::VolumeHourly => {}
        CountDataFormat::VolumeDayByHour => {
            if matches!(
                count_kind,
                CountKind::Class | CountKind::Volume | CountKind::FifteenMinVolume
            ) {
                match NonNormalVolCount::select(&conn, recordnum) {
                    Ok(v) if v.is_empty() => {
                        *MESSAGE.lock().unwrap() =
                            no_records(CountDataFormat::VolumeDayByHour, recordnum)
                    }
                    Ok(v) => template.non_normal_volume = Some(v),
                    Err(e) => *MESSAGE.lock().unwrap() = Some(format!("{e}")),
                }
            } else {
                *MESSAGE.lock().unwrap() = Some(format!(
                    "{:?} format is not available for {:?} counts.",
                    format, count_kind
                ));
            }
        }
        CountDataFormat::Class15Min => {
            if count_kind == CountKind::Class {
                match TimeBinnedVehicleClassCount::select(&conn, recordnum) {
                    Ok(v) if v.is_empty() => {
                        *MESSAGE.lock().unwrap() =
                            no_records(CountDataFormat::Class15Min, recordnum)
                    }
                    Ok(v) => template.fifteen_min_class = Some(v),
                    Err(e) => *MESSAGE.lock().unwrap() = Some(format!("{e}")),
                }
            } else {
                *MESSAGE.lock().unwrap() = Some(format!(
                    "{:?} format is not available for {:?} counts.",
                    format, count_kind
                ));
            }
        }
        CountDataFormat::ClassHourly => {}
        CountDataFormat::Speed15Min => {
            if count_kind == CountKind::Speed || count_kind == CountKind::Class {
                match TimeBinnedSpeedRangeCount::select(&conn, recordnum) {
                    Ok(v) if v.is_empty() => {
                        *MESSAGE.lock().unwrap() =
                            no_records(CountDataFormat::Speed15Min, recordnum)
                    }
                    Ok(v) => template.fifteen_min_speed = Some(v),
                    Err(e) => *MESSAGE.lock().unwrap() = Some(format!("{e}")),
                }
            } else {
                *MESSAGE.lock().unwrap() = Some(format!(
                    "{:?} format is not available for {:?} counts.",
                    format, count_kind
                ));
            }
        }
        CountDataFormat::SpeedHourly => {}
        CountDataFormat::SpeedDayByHour => {
            if count_kind == CountKind::Speed || count_kind == CountKind::Class {
                match NonNormalAvgSpeedCount::select(&conn, recordnum) {
                    Ok(v) if v.is_empty() => {
                        *MESSAGE.lock().unwrap() =
                            no_records(CountDataFormat::SpeedDayByHour, recordnum)
                    }
                    Ok(v) => template.non_normal_avg_speed = Some(v),
                    Err(e) => *MESSAGE.lock().unwrap() = Some(format!("{e}")),
                }
            } else {
                *MESSAGE.lock().unwrap() = Some(format!(
                    "{:?} format is not available for {:?} counts.",
                    format, count_kind
                ));
            }
        }
    }
    template
}

#[derive(Template, Debug, Default)]
#[template(path = "admin/insert.html")]
struct AdminInsertTemplate {}

impl Heading for AdminInsertTemplate {
    const NAV_ITEM_TEXT: &str = "Create New Records";
}

#[derive(Debug, Deserialize)]
struct AdminInsertForm {
    number_to_create: Option<String>,
}

/// Show forms to create new count(s) ([`Metadata`] record(s)), either empty or from existing one.
async fn get_insert() -> AdminInsertTemplate {
    AdminInsertTemplate::default()
}

/// Process form to create new empty count(s) ([`Metadata`] record(s)).
async fn post_insert_empty(
    State(state): State<AppState>,
    Form(input): Form<AdminInsertForm>,
) -> Response {
    let conn = state.conn_pool.get().unwrap();
    let template = AdminInsertTemplate::default();

    match input.number_to_create {
        Some(v) => {
            // Parse string from user.
            match v.parse::<u32>() {
                // If valid u32, try to get the metadata for that recordnum.
                Ok(v) => match db::insert_empty_metadata(&conn, v) {
                    Ok(w) => {
                        // Store recordnum of first (and perhaps only) one created.
                        let first_recordnum = w.clone()[0];

                        // Add links to each new one created.
                        let records = w
                        .into_iter()
                        .map(|r| format!(
                            r#"<a href="{ADMIN_METADATA_DETAIL_PATH}?recordnum={r}">{r}</a>"#)
                        )
                        .collect::<Vec<String>>();

                        *MESSAGE.lock().unwrap() =
                            Some(format!("New records created: {}", records.join(", ")));

                        return Redirect::to(&format!(
                            "{ADMIN_METADATA_DETAIL_PATH}?recordnum={first_recordnum}"
                        ))
                        .into_response();
                    }
                    Err(e) => *MESSAGE.lock().unwrap() = Some(format!("Error: {e}.")),
                },
                Err(_) => {
                    *MESSAGE.lock().unwrap() = Some("Please insert a valid number.".to_string());
                }
            }
        }
        None => {
            *MESSAGE.lock().unwrap() = Some(format!(
                "Please specify a number of records to create, from 1 to {}.",
                db::RECORD_CREATION_LIMIT
            ));
        }
    };

    template.into_response()
}

#[derive(Template, Debug, Default)]
#[template(path = "admin/insert_from_existing.html")]
struct AdminInsertFromExistingTemplate {
    metadata: Option<Metadata>,
}

impl Heading for AdminInsertFromExistingTemplate {
    const NAV_ITEM_TEXT: &str = "Create New Records from Existing Count";
}

#[derive(Deserialize, Debug)]
struct AdminInsertFromExistingForm {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    create_from_existing: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    number_to_create: Option<u32>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    submit_fields: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    recordnum: Option<u32>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    count_kind: Option<CountKind>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    cntdir: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    trafdir: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    indir: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    outdir: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    fromlmt: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    tolmt: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    latitude: Option<f32>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    longitude: Option<f32>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    x: Option<f32>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    y: Option<f32>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    bikepeddesc: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    bikepedfacility: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    bikepedgroup: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    comments: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    description: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    fc: Option<u32>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    isurban: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    mcd: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    mp: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    offset: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    prj: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    program: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    rdprefix: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    rdsuffix: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    road: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    route: Option<u32>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    seg: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    sidewalk: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    source: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    sr: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    sri: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    stationid: Option<String>,
}

/// Show form to initiate creation of new count ([`Metadata`] record(s)) from existing count.
async fn get_insert_from_existing() -> AdminInsertFromExistingTemplate {
    AdminInsertFromExistingTemplate::default()
}

/// Get existing count ([`Metadata`] record) from database,
/// show/process form to select fields to use.
async fn post_insert_from_existing(
    State(state): State<AppState>,
    Form(input): Form<AdminInsertFromExistingForm>,
) -> Response {
    let conn = state.conn_pool.get().unwrap();
    let mut template = AdminInsertFromExistingTemplate::default();

    // Get metadata from the existing count user wants to create new one from.
    if input.create_from_existing.is_some() {
        match input.recordnum {
            Some(v) => match db::get_metadata(&conn, v) {
                Ok(v) => {
                    template.metadata = Some(v);
                }
                Err(e) => {
                    if e.source().is_some_and(|v| {
                        matches!(
                            v.downcast_ref::<OracleError>().unwrap().kind(),
                            OracleErrorKind::NoDataFound
                        )
                    }) {
                        *MESSAGE.lock().unwrap() = Some(format!("Record {v} not found."))
                    } else {
                        *MESSAGE.lock().unwrap() = Some(format!("{e}"))
                    }
                }
            },
            None => {
                *MESSAGE.lock().unwrap() =
                    Some("Please specify a recordnum to use as a template.".to_string());
            }
        }
    }

    // Process creating new count from existing one.
    if input.submit_fields.is_some() {
        match input.number_to_create {
            Some(v) => {
                // Create a Metadata instance to use to
                // Convert direction types from strings.
                let cntdir = if let Some(v) = &input.cntdir {
                    match RoadDirection::from_str(v) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            *MESSAGE.lock().unwrap() = Some(format!("{e}"));
                            return template.into_response();
                        }
                    }
                } else {
                    None
                };
                let trafdir = if let Some(v) = &input.trafdir {
                    match RoadDirection::from_str(v) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            *MESSAGE.lock().unwrap() = Some(format!("{e}"));
                            return template.into_response();
                        }
                    }
                } else {
                    None
                };
                let indir = if let Some(v) = &input.indir {
                    match LaneDirection::from_str(v) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            *MESSAGE.lock().unwrap() = Some(format!("{e}"));
                            return template.into_response();
                        }
                    }
                } else {
                    None
                };
                let outdir = if let Some(v) = &input.outdir {
                    match LaneDirection::from_str(v) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            *MESSAGE.lock().unwrap() = Some(format!("{e}"));
                            return template.into_response();
                        }
                    }
                } else {
                    None
                };

                let metadata = Metadata {
                    amending: None,
                    ampeak: None,
                    bikepeddesc: input.bikepeddesc,
                    bikepedfacility: input.bikepedfacility,
                    bikepedgroup: input.bikepedgroup,
                    cntdir,
                    comments: input.comments,
                    count_kind: input.count_kind,
                    counter_id: None,
                    createheaderdate: None,
                    datelastcounted: None,
                    description: input.description,
                    fc: input.fc,
                    fromlmt: input.fromlmt,
                    importdatadate: None,
                    indir,
                    isurban: input.isurban,
                    latitude: input.latitude,
                    longitude: input.longitude,
                    mcd: input.mcd,
                    mp: input.mp,
                    offset: input.offset,
                    outdir,
                    pmending: None,
                    pmpeak: None,
                    prj: input.prj,
                    program: input.program,
                    recordnum: None,
                    rdprefix: input.rdprefix,
                    rdsuffix: input.rdsuffix,
                    road: input.road,
                    route: input.route,
                    seg: input.seg,
                    sidewalk: input.sidewalk,
                    speedlimit: None,
                    source: input.source,
                    sr: input.sr,
                    sri: input.sri,
                    stationid: input.stationid,
                    technician: None,
                    tolmt: input.tolmt,
                    trafdir,
                    x: input.x,
                    y: input.y,
                };

                match db::insert_metadata_from_existing(&conn, v, metadata) {
                    Ok(w) => {
                        // Store recordnum of first (and perhaps only) one created.
                        let first_recordnum = w.clone()[0];

                        // Add links to each new one created.
                        let records = w.into_iter().map(|r| format!(r#"<a href="{ADMIN_METADATA_DETAIL_PATH}?recordnum={r}">{r}</a>"#)).collect::<Vec<String>>();

                        *MESSAGE.lock().unwrap() =
                            Some(format!("New records created: {}", records.join(", ")));

                        return Redirect::to(&format!(
                            "{ADMIN_METADATA_DETAIL_PATH}?recordnum={first_recordnum}"
                        ))
                        .into_response();
                    }
                    Err(e) => *MESSAGE.lock().unwrap() = Some(format!("Error: {e}.")),
                }
            }
            None => {
                *MESSAGE.lock().unwrap() = Some(format!(
                    "Please specify a number of records to create, from 1 to {}.",
                    db::RECORD_CREATION_LIMIT
                ));
            }
        }
    }

    template.into_response()
}

#[derive(Template, Debug, Default)]
#[template(path = "admin/import_log.html")]
struct AdminImportLogTemplate {
    recordnum: Option<u32>,
    log_entries: Vec<ImportLogEntry>,
}

impl Heading for AdminImportLogTemplate {
    const NAV_ITEM_TEXT: &str = "View Import Log";
}

/// Show import log - for all or one count.
async fn get_import_log(
    State(state): State<AppState>,
    params: Query<RecordnumFilterParams>,
) -> AdminImportLogTemplate {
    let conn = state.conn_pool.get().unwrap();
    let params = params.0;
    let mut template = AdminImportLogTemplate {
        recordnum: None,
        log_entries: vec![],
    };

    // Default to getting all entries, because even if user requests entries for a specific
    // recordnum, we want to show all of them if nothing is found.
    match db::get_import_log(&conn, None) {
        Ok(v) if v.is_empty() => {
            template.log_entries = v;
            *MESSAGE.lock().unwrap() = Some("No import log entries found.".to_string());
        }
        Ok(v) => template.log_entries = v,
        Err(e) => {
            *MESSAGE.lock().unwrap() = Some(format!(
                "Error fetching import log entries from database: {e}"
            ))
        }
    }

    // Retain entries for specific recordnum only.
    if let Some(v) = params.recordnum {
        if template
            .log_entries
            .iter()
            .any(|entry| entry.recordnum == v)
        {
            template.recordnum = Some(v);
            template.log_entries.retain(|entry| entry.recordnum == v);
        } else {
            *MESSAGE.lock().unwrap() =
                Some(format!("No import log entries found for recordnum {v}."));
        }
    }

    template
}

/// Show AADV records - for all or one count.
#[derive(Template, Debug, Default)]
#[template(path = "counts/aadv.html")]
struct AadvTemplate {
    recordnum: Option<u32>,
    aadv: Vec<AadvEntry>,
}

impl Heading for AadvTemplate {
    const NAV_ITEM_TEXT: &str = "View Current and Historical AADV";
}

async fn get_aadv(
    State(state): State<AppState>,
    params: Query<RecordnumFilterParams>,
) -> AadvTemplate {
    let conn = state.conn_pool.get().unwrap();
    let params = params.0;
    let mut template = AadvTemplate {
        recordnum: None,
        aadv: vec![],
    };

    // Default to getting all entries, because even if user requests entries for a specific
    // recordnum, we want to show all of them if nothing is found.
    match aadv::get_aadv(&conn, None) {
        Ok(v) if v.is_empty() => {
            template.aadv = v;
            *MESSAGE.lock().unwrap() = Some("No AADV entries found.".to_string());
        }
        Ok(v) => template.aadv = v,
        Err(e) => {
            *MESSAGE.lock().unwrap() =
                Some(format!("Error fetching AADV entries from database: {e}."));
        }
    }

    // Retain entries for specific recordnum only.
    if let Some(v) = params.recordnum {
        if template.aadv.iter().any(|entry| entry.recordnum == v) {
            template.recordnum = Some(v);
            template.aadv.retain(|entry| entry.recordnum == v);
        } else {
            *MESSAGE.lock().unwrap() = Some(format!("No AADV entries found for recordnum {v}."));
        }
    }

    template
}

#[derive(Template, Default, Debug)]
#[template(path = "home.html")]
struct HomeTemplate {}

impl Heading for HomeTemplate {
    const NAV_ITEM_TEXT: &str = "Welcome";
}

async fn home() -> HomeTemplate {
    HomeTemplate::default()
}

fn empty_string_as_none<'de, D, T>(de: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: fmt::Display,
{
    let opt = Option::<String>::deserialize(de)?;
    match opt.as_deref() {
        None | Some("") => Ok(None),
        Some(s) => FromStr::from_str(s).map_err(de::Error::custom).map(Some),
    }
}

// Any filter defined in the module `filters` is accessible in your template.
mod filters {
    /// Display None variant of Options as empty strings.
    pub fn opt<T: std::fmt::Display>(s: &Option<T>) -> rinja::Result<String> {
        match s {
            Some(s) => Ok(s.to_string()),
            None => Ok(String::new()),
        }
    }
}

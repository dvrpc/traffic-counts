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
use chrono::{NaiveDate, Timelike};
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
const ADMIN_METADATA_EDIT_PATH: &str = "/admin/edit";
const ADMIN_COUNT_DATA_PATH: &str = "/admin/count";
const ADMIN_IMPORT_LOG_PATH: &str = "/admin/import-log";
const ADMIN_AADV_PATH: &str = "/admin/aadv";
const RECORD_CREATION_LIMIT: u32 = db::RECORD_CREATION_LIMIT;

static MESSAGE: Mutex<Option<String>> = Mutex::new(None);

/// Set value in global MESSAGE mutex.
fn message(value: impl Into<String>) {
    match MESSAGE.lock() {
        Ok(mut m) => *m = Some(value.into()),
        Err(e) => {
            MESSAGE.clear_poison();
            let mut e = e.into_inner();
            *e = Some(value.into());
        }
    }
}

/// Retrieve, reset, and return value from global MESSAGE Mutex.
///
/// This is for displaying messages to user in templates. It is reset because messages
/// should only be shown once and not be persisted across responses.
pub fn burn_after_reading() -> String {
    let message = match MESSAGE.lock() {
        Ok(v) => v.clone().unwrap_or_default(),
        Err(e) => {
            MESSAGE.clear_poison();
            let e = e.into_inner();
            (e.clone().unwrap_or_default()).to_string()
        }
    };
    match MESSAGE.lock() {
        Ok(mut m) => *m = None,
        Err(e) => {
            MESSAGE.clear_poison();
            let mut e = e.into_inner();
            *e = None;
        }
    }
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
            get(get_insert).post(post_insert),
        )
        .route_with_tsr(ADMIN_METADATA_EDIT_PATH, get(get_edit).post(post_edit))
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
    recordnum: Option<String>,
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
        Err(e) => message(format!("{e}")),
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

/// Get count ([`Metadata`] record).
async fn get_metadata_detail(
    headers: HeaderMap,
    State(state): State<AppState>,
    params: Query<RecordnumFilterParams>,
) -> Response {
    let conn = state.conn_pool.get().unwrap();
    let params = params.0;

    let mut template = AdminMetadataDetailTemplate {
        recordnum: None,
        metadata: None,
    };

    let recordnum = match params.recordnum {
        Some(v) => match v.parse::<u32>() {
            Ok(v) => v,
            Err(_) => {
                message("Please provide a valid number.");
                if headers
                    .get(REFERER)
                    .is_some_and(|v| v.to_str().unwrap().contains(ADMIN_METADATA_LIST_PATH))
                {
                    return Redirect::to(ADMIN_METADATA_LIST_PATH).into_response();
                } else {
                    return template.into_response();
                }
            }
        },
        None => {
            message("Please provide a valid number.");
            if headers
                .get(REFERER)
                .is_some_and(|v| v.to_str().unwrap().contains(ADMIN_METADATA_LIST_PATH))
            {
                return Redirect::to(ADMIN_METADATA_LIST_PATH).into_response();
            } else {
                return template.into_response();
            }
        }
    };

    template.recordnum = Some(recordnum);
    match db::get_metadata(&conn, recordnum) {
        Ok(v) => template.metadata = Some(v),
        Err(e) => {
            // Handle the one error that is probable (no matching recordnum in db).
            if e.source().is_some_and(|v| {
                matches!(
                    v.downcast_ref::<OracleError>().unwrap().kind(),
                    OracleErrorKind::NoDataFound
                )
            }) {
                message(format!("Record {recordnum} not found."));
            } else {
                message(format!("{e}"));
            }
            if headers
                .get(REFERER)
                .is_some_and(|v| v.to_str().unwrap().contains(ADMIN_METADATA_LIST_PATH))
            {
                return Redirect::to(ADMIN_METADATA_LIST_PATH).into_response();
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
            message("Please provide a record number.");
            return template;
        }
    };
    let format = match params.format {
        Some(v) => v,
        None => {
            message("Please provide the format for the data to be presented in.");
            return template;
        }
    };

    // Get the kind of count this, in order to check if the desired format is available for it.
    let count_kind = match db::get_count_kind(&conn, recordnum) {
        Ok(Some(v)) => v,
        Ok(None) => {
            message("Cannot determine kind of count, and thus unable to fetch count data.");
            return template;
        }
        Err(_) => {
            message(
                "Error fetching count kind from database, and thus unable to fetch count data.",
            );

            return template;
        }
    };

    // Msg to user if no results are empty.
    fn no_records(format: CountDataFormat, recordnum: u32) -> String {
        format!("No {format} records found for recordnum {recordnum}.")
    }

    // Get data according to format/count kind and put into appropriate variable of template.
    match format {
        CountDataFormat::Volume15Min => match count_kind {
            CountKind::FifteenMinVolume => match FifteenMinuteVehicle::select(&conn, recordnum) {
                Ok(v) if v.is_empty() => {
                    message(no_records(CountDataFormat::Volume15Min, recordnum));
                }
                Ok(v) => template.fifteen_min_vehicle = Some(v),
                Err(e) => message(format!("{e}")),
            },
            CountKind::Bicycle1
            | CountKind::Bicycle2
            | CountKind::Bicycle3
            | CountKind::Bicycle4
            | CountKind::Bicycle5
            | CountKind::Bicycle6 => match FifteenMinuteBicycle::select(&conn, recordnum) {
                Ok(v) if v.is_empty() => {
                    message(no_records(CountDataFormat::Volume15Min, recordnum));
                }
                Ok(v) => template.fifteen_min_bike = Some(v),
                Err(e) => message(format!("{e}")),
            },
            CountKind::Pedestrian | CountKind::Pedestrian2 => {
                match FifteenMinutePedestrian::select(&conn, recordnum) {
                    Ok(v) if v.is_empty() => {
                        message(no_records(CountDataFormat::Volume15Min, recordnum))
                    }
                    Ok(v) => template.fifteen_min_ped = Some(v),
                    Err(e) => message(format!("{e}")),
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
                        message(no_records(CountDataFormat::VolumeDayByHour, recordnum))
                    }
                    Ok(v) => template.non_normal_volume = Some(v),
                    Err(e) => message(format!("{e}")),
                }
            } else {
                message(format!(
                    "{:?} format is not available for {:?} counts.",
                    format, count_kind
                ));
            }
        }
        CountDataFormat::Class15Min => {
            if count_kind == CountKind::Class {
                match TimeBinnedVehicleClassCount::select(&conn, recordnum) {
                    Ok(v) if v.is_empty() => {
                        message(no_records(CountDataFormat::Class15Min, recordnum))
                    }
                    Ok(v) => template.fifteen_min_class = Some(v),
                    Err(e) => message(format!("{e}")),
                }
            } else {
                message(format!(
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
                        message(no_records(CountDataFormat::Speed15Min, recordnum))
                    }
                    Ok(v) => template.fifteen_min_speed = Some(v),
                    Err(e) => message(format!("{e}")),
                }
            } else {
                message(format!(
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
                        message(no_records(CountDataFormat::SpeedDayByHour, recordnum))
                    }
                    Ok(v) => template.non_normal_avg_speed = Some(v),
                    Err(e) => message(format!("{e}")),
                }
            } else {
                message(format!(
                    "{:?} format is not available for {:?} counts.",
                    format, count_kind
                ));
            }
        }
    }
    template
}

#[derive(Template, Debug, Default, Deserialize)]
#[template(path = "admin/insert.html")]
struct AdminInsertTemplate {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    number_to_create: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    recordnum: Option<String>,
    metadata: Option<Metadata>,
}

impl Heading for AdminInsertTemplate {
    const NAV_ITEM_TEXT: &str = "Create New Records";
}

/// Show forms to create new count(s) ([`Metadata`] record(s)), either empty or from existing one.
async fn get_insert() -> AdminInsertTemplate {
    AdminInsertTemplate::default()
}

#[derive(Debug, Deserialize)]
struct AdminInsertForm {
    number_to_create: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    recordnum: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    submit_fields: Option<String>,
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

/// Process forms (from both steps) to create new count(s) ([`Metadata`] record(s)).
async fn post_insert(
    State(state): State<AppState>,
    Form(input): Form<AdminInsertForm>,
) -> Response {
    let conn = state.conn_pool.get().unwrap();
    let mut template = AdminInsertTemplate::default();

    // Get number of counts user wants to create.
    let number_to_create = match input.number_to_create {
        Some(v) => match v.parse::<u32>() {
            Ok(v) if v > 0 && v < 50 => {
                template.number_to_create = Some(v.to_string());
                v
            }
            Ok(_) | Err(_) => {
                message("Please provide a valid number of records to create.");
                return template.into_response();
            }
        },
        None => {
            message(format!(
                "Please specify a number of records to create, from 1 to {}.",
                db::RECORD_CREATION_LIMIT
            ));
            return template.into_response();
        }
    };

    // Determine what to do next based on whether or not user provide existing count to use.
    match input.recordnum {
        // Finish - handle creation of empty record(s).
        None => match db::insert_empty_metadata(&conn, number_to_create) {
            Ok(w) => {
                // Store recordnum of first (and perhaps only) one created.
                let first_recordnum = w.clone()[0];

                // Add links to each new one created.
                let records = w
                    .into_iter()
                    .map(|r| {
                        format!(r#"<a href="{ADMIN_METADATA_DETAIL_PATH}?recordnum={r}">{r}</a>"#)
                    })
                    .collect::<Vec<String>>();

                message(format!("New records created: {}", records.join(", ")));

                return Redirect::to(&format!(
                    "{ADMIN_METADATA_DETAIL_PATH}?recordnum={first_recordnum}"
                ))
                .into_response();
            }
            Err(e) => message(format!("Error: {e}.")),
        },
        // Send user to form to select which fields to use, or process that form.
        Some(v) => {
            // First, get the recordnum - either from initial user-provided number or
            // from hidden input value.
            let recordnum = match v.parse::<u32>() {
                Ok(v) => {
                    template.recordnum = Some(v.to_string());
                    v
                }
                Err(_) => {
                    message("Please provide a valid number for the recordnum.");
                    return template.into_response();
                }
            };

            match input.submit_fields {
                // Display form for user to chose and submit fields to use.
                None => match db::get_metadata(&conn, recordnum) {
                    Ok(metadata) => template.metadata = Some(metadata),
                    Err(e) => {
                        if e.source().is_some_and(|v| {
                            matches!(
                                v.downcast_ref::<OracleError>().unwrap().kind(),
                                OracleErrorKind::NoDataFound
                            )
                        }) {
                            message(format!("Record {recordnum} not found."))
                        } else {
                            message(format!("{e}"))
                        }
                    }
                },
                // User has chosen/submitted fields - create new records from existing one.
                Some(_) => {
                    // Convert direction types from strings.
                    let cntdir = if let Some(v) = &input.cntdir {
                        match RoadDirection::from_str(v) {
                            Ok(v) => Some(v),
                            Err(e) => {
                                message(format!("{e}"));
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
                                message(format!("{e}"));
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
                                message(format!("{e}"));
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
                                message(format!("{e}"));
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

                    match db::insert_metadata_from_existing(&conn, number_to_create, metadata) {
                        Ok(v) => {
                            // Store recordnum of first (and perhaps only) one created.
                            let first_recordnum = v.clone()[0];

                            // Add links to each new one created.
                            let records = v
                                .into_iter()
                                .map(|r| {
                                    format!(r#"<a href="{ADMIN_METADATA_DETAIL_PATH}?recordnum={r}">{r}</a>"#)
                                })
                                .collect::<Vec<String>>();

                            message(format!("New records created: {}", records.join(", ")));

                            return Redirect::to(&format!(
                                "{ADMIN_METADATA_DETAIL_PATH}?recordnum={first_recordnum}"
                            ))
                            .into_response();
                        }
                        Err(e) => message(format!("Error: {e}.")),
                    }
                }
            }
        }
    }
    template.into_response()
}

#[derive(Template, Debug, Default, Deserialize)]
#[template(path = "admin/edit.html")]
struct AdminEditTemplate {
    metadata: Option<Metadata>,
}

impl Heading for AdminEditTemplate {
    const NAV_ITEM_TEXT: &str = "Edit Count Metadata";
}

/// Show forms to edit fields of count [`Metadata`].
async fn get_edit(
    State(state): State<AppState>,
    params: Query<RecordnumFilterParams>,
) -> AdminEditTemplate {
    let conn = state.conn_pool.get().unwrap();
    let mut template = AdminEditTemplate::default();

    if let Some(v) = &params.recordnum {
        match v.parse::<u32>() {
            Ok(recordnum) => match db::get_metadata(&conn, recordnum) {
                Ok(metadata) => template.metadata = Some(metadata),
                Err(e) => {
                    if e.source().is_some_and(|e| {
                        matches!(
                            e.downcast_ref::<OracleError>().unwrap().kind(),
                            OracleErrorKind::NoDataFound
                        )
                    }) {
                        message(format!("Record {recordnum} not found."))
                    } else {
                        message(format!("{e}"))
                    }
                }
            },
            Err(_) => message("Please provide a valid number."),
        }
    }
    template
}

#[derive(Debug, Deserialize)]
struct AdminEditForm {
    recordnum: u32,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    bikepeddesc: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    bikepedfacility: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    bikepedgroup: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    cntdir: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    count_kind: Option<CountKind>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    comments: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    counter_id: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    datelastcounted: Option<NaiveDate>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    description: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    fc: Option<u32>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    fromlmt: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    indir: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    isurban: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    latitude: Option<f32>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    longitude: Option<f32>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    mcd: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    mp: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    offset: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    outdir: Option<String>,
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
    speedlimit: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    sr: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    sri: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    stationid: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    technician: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    tolmt: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    trafdir: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    x: Option<f32>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    y: Option<f32>,
}

/// Process form to edit count [`Metadata`].
async fn post_edit(
    State(state): State<AppState>,
    Form(input): Form<AdminEditForm>,
) -> AdminEditTemplate {
    dbg!("here1 in post_edit");
    let conn = state.conn_pool.get().unwrap();
    let mut template = AdminEditTemplate::default();

    // Get the metadata from db.
    let mut metadata = match db::get_metadata(&conn, input.recordnum) {
        Ok(v) => v,
        Err(e) => {
            if e.source().is_some_and(|e| {
                matches!(
                    e.downcast_ref::<OracleError>().unwrap().kind(),
                    OracleErrorKind::NoDataFound
                )
            }) {
                message(format!("Record {} not found.", input.recordnum));
            } else {
                message(format!("{e}"));
            }
            return template;
        }
    };

    // Convert direction types from strings.
    if let Some(v) = &input.cntdir {
        match RoadDirection::from_str(v) {
            Ok(v) => metadata.cntdir = Some(v),
            Err(e) => message(format!("{e}")),
        }
    };
    if let Some(v) = &input.trafdir {
        match RoadDirection::from_str(v) {
            Ok(v) => metadata.trafdir = Some(v),
            Err(e) => message(format!("{e}")),
        }
    };
    if let Some(v) = &input.indir {
        match LaneDirection::from_str(v) {
            Ok(v) => metadata.indir = Some(v),
            Err(e) => message(format!("{e}")),
        }
    };
    if let Some(v) = &input.outdir {
        match LaneDirection::from_str(v) {
            Ok(v) => metadata.outdir = Some(v),
            Err(e) => message(format!("{e}")),
        }
    };
    if let Some(v) = &input.speedlimit {
        match v.parse::<u8>() {
            Ok(v) => metadata.speedlimit = Some(v),
            Err(e) => message(format!("{e}")), 
        }
    };

    dbg!(&input.speedlimit);
    dbg!(&input.fromlmt);

    // Update the fields.
    metadata.bikepeddesc = input.bikepeddesc;
    metadata.bikepedfacility = input.bikepedfacility;
    metadata.bikepedgroup = input.bikepedgroup;
    metadata.comments = input.comments;
    metadata.count_kind = input.count_kind;
    metadata.counter_id = input.counter_id;
    metadata.datelastcounted = input.datelastcounted;
    metadata.description = input.description;
    metadata.fc = input.fc;
    metadata.fromlmt = input.fromlmt;
    metadata.isurban = input.isurban;
    metadata.latitude = input.latitude;
    metadata.longitude = input.longitude;
    metadata.mcd = input.mcd;
    metadata.mp = input.mp;
    metadata.offset = input.offset;
    metadata.prj = input.prj;
    metadata.program = input.program;
    metadata.rdprefix = input.rdprefix;
    metadata.rdsuffix = input.rdsuffix;
    metadata.road = input.road;
    metadata.route = input.route;
    metadata.seg = input.seg;
    metadata.sidewalk = input.sidewalk;
    metadata.source = input.source;
    metadata.sr = input.sr;
    metadata.sri = input.sri;
    metadata.stationid = input.stationid;
    metadata.technician = input.technician;
    metadata.tolmt = input.tolmt;
    metadata.x = input.x;
    metadata.y = input.y;

    dbg!(&metadata);

    template.metadata = Some(metadata);

    template
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
            message("No import log entries found.");
        }
        Ok(v) => template.log_entries = v,
        Err(e) => message(format!(
            "Error fetching import log entries from database: {e}"
        )),
    }

    // Get any user-provided recordnum.
    let recordnum = match params.recordnum {
        Some(v) => match v.parse::<u32>() {
            Ok(v) => Some(v),
            Err(_) => {
                message("Please provide a valid number.");
                return template;
            }
        },
        None => None,
    };

    // Retain entries for specific recordnum only.
    if let Some(v) = recordnum {
        if template
            .log_entries
            .iter()
            .any(|entry| entry.recordnum == v)
        {
            template.recordnum = Some(v);
            template.log_entries.retain(|entry| entry.recordnum == v);
        } else {
            message(format!("No import log entries found for recordnum {v}."));
        }
    }

    template
}

/// Show AADV records - for all or one count.
#[derive(Template, Debug, Default)]
#[template(path = "counts/aadv.html")]
struct AadvTemplate {
    recordnum: Option<String>,
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
            message("No AADV entries found.");
        }
        Ok(v) => template.aadv = v,
        Err(e) => {
            message(format!("Error fetching AADV entries from database: {e}."));
        }
    }

    // Get any user-provided recordnum.
    let recordnum = match params.recordnum {
        Some(v) => match v.parse::<u32>() {
            Ok(v) => Some(v),
            Err(_) => {
                message("Please provide a valid number.");
                return template;
            }
        },
        None => None,
    };

    // Retain entries for specific recordnum only.
    if let Some(v) = recordnum {
        if template.aadv.iter().any(|entry| entry.recordnum == v) {
            template.recordnum = Some(v.to_string());
            template.aadv.retain(|entry| entry.recordnum == v);
        } else {
            message(format!("No AADV entries found for recordnum {v}."));
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
    use rinja::filters::Safe;
    /// Display None variant of Options as empty strings.
    pub fn opt<T: std::fmt::Display>(s: &Option<T>) -> rinja::Result<String> {
        match s {
            Some(s) => Ok(s.to_string()),
            None => Ok(String::new()),
        }
    }

    /// Display input text.
    pub fn input<'a, T: std::fmt::Display>(s: &Option<T>, field: &'a str) -> rinja::Result<Safe<String>> {
        let s = match s {
            Some(s) => s.to_string(),
            None => String::new(),
        };
        Ok(Safe(format!("<input type='text' name='{field}' value='{s}' style='text-align:right' />")))
    }
}

use std::env;
use std::error::Error;
use std::fmt;
use std::str::FromStr;

use axum::{
    extract::{Form, Query, State},
    routing::get,
    Router,
};
use axum_extra::routing::RouterExt;
use chrono::Timelike;
use oracle::{pool::Pool, Error as OracleError, ErrorKind as OracleErrorKind};
use rinja_axum::Template;
use serde::{de, Deserialize, Deserializer};
use tower_http::services::ServeDir;

use traffic_counts::{
    aadv::{self, AadvEntry},
    db::{self, crud::Crud, ImportLogEntry},
    denormalize::{NonNormalAvgSpeedCount, NonNormalVolCount},
    CountKind, FifteenMinuteBicycle, FifteenMinutePedestrian, FifteenMinuteVehicle, Metadata,
    TimeBinnedSpeedRangeCount, TimeBinnedVehicleClassCount,
};

const ADMIN_PATH: &str = "/admin";
const ADMIN_METADATA_LIST_PATH: &str = "/admin/metadata-list";
const ADMIN_METADATA_DETAIL_PATH: &str = "/admin/metadata-detail";
const ADMIN_METADATA_INSERT_PATH: &str = "/admin/insert";
const ADMIN_COUNT_DATA_PATH: &str = "/admin/count";
const ADMIN_IMPORT_LOG_PATH: &str = "/admin/import-log";
const ADMIN_AADV_PATH: &str = "/admin/aadv";
const RECORD_CREATION_LIMIT: u32 = db::RECORD_CREATION_LIMIT;

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
        .route_with_tsr(ADMIN_IMPORT_LOG_PATH, get(get_view_import_log))
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

/// The condition of the response - getting input (possibly again after bad input) or success.
#[derive(Default, PartialEq, Debug)]
enum ResponseCondition {
    #[default]
    GetInput,
    Success,
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
struct AdminMainTemplate {
    message: Option<String>,
}

impl Heading for AdminMainTemplate {
    const NAV_ITEM_TEXT: &str = "Welcome";
}

async fn admin() -> AdminMainTemplate {
    AdminMainTemplate::default()
}

#[derive(Template, Debug, Default)]
#[template(path = "counts/metadata_list.html")]
struct AdminMetadataListTemplate {
    message: Option<String>,
    metadata: Vec<Metadata>,
    total_pages: u32,
    page: u32,
}

impl Heading for AdminMetadataListTemplate {
    const NAV_ITEM_TEXT: &str = "View Count Metadata Records";
}

#[derive(Deserialize)]
struct Page {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    page: Option<u32>,
}

async fn get_metadata_list(
    State(state): State<AppState>,
    page: Query<Page>,
) -> AdminMetadataListTemplate {
    let results_per_page = 100;
    let page = page.0.page.unwrap_or(1);
    let total_pages = state.num_metadata_records / results_per_page;
    let conn = state.conn_pool.get().unwrap();
    let mut template = AdminMetadataListTemplate {
        message: None,
        metadata: vec![],
        total_pages,
        page,
    };

    match db::get_metadata_paginated(&conn, Some(page * results_per_page), None) {
        Ok(v) => {
            template.metadata = v;
        }
        Err(e) => {
            template.message = Some(format!("{e}"));
        }
    }
    template
}

#[derive(Template, Debug, Default)]
#[template(path = "counts/metadata_detail.html")]
struct AdminMetadataDetailTemplate {
    message: Option<String>,
    recordnum: Option<u32>,
    metadata: Option<Metadata>,
}

impl Heading for AdminMetadataDetailTemplate {
    const NAV_ITEM_TEXT: &str = "View Count Metadata";
}

/// Query params used to filter for particular recordnum or clear filter.
#[derive(Debug, Deserialize)]
struct AdminMetadataDetailParams {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    recordnum: Option<u32>,
}

async fn get_metadata_detail(
    State(state): State<AppState>,
    params: Query<AdminMetadataDetailParams>,
) -> AdminMetadataDetailTemplate {
    let conn = state.conn_pool.get().unwrap();
    let params = params.0;
    let mut template = AdminMetadataDetailTemplate {
        message: None,
        recordnum: None,
        metadata: None,
    };

    if params.recordnum.is_none() {
        template.message = Some("Please provide a record number.".to_string());
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
                    template.message = Some(format!("Record {v} not found."))
                } else {
                    template.message = Some(format!("{e}"))
                }
            }
        }
    }
    template
}

#[derive(Template, Debug, Default)]
#[template(path = "counts/count_data.html")]
struct CountDataTemplate {
    message: Option<String>,
    condition: ResponseCondition,
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

#[derive(Debug, Deserialize)]
struct CountDataParams {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    recordnum: Option<u32>,
    format: Option<CountDataFormat>,
}

async fn get_count_data(
    State(state): State<AppState>,
    params: Query<CountDataParams>,
) -> CountDataTemplate {
    let conn = state.conn_pool.get().unwrap();
    let params = params.0;
    let mut count_data = CountDataTemplate {
        message: None,
        recordnum: None,
        condition: ResponseCondition::GetInput,
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
            count_data.recordnum = Some(v);
            v
        }
        None => {
            count_data.message = Some("Please provide a record number.".to_string());
            return count_data;
        }
    };
    let format = match params.format {
        Some(v) => v,
        None => {
            count_data.message =
                Some("Please provide the format for the data to be presented in.".to_string());
            return count_data;
        }
    };

    // Get the kind of count this, in order to check if the desired format is available for it.
    let count_kind = match db::get_count_kind(&conn, recordnum) {
        Ok(Some(v)) => v,
        Ok(None) => {
            count_data.message = Some(
                "Cannot determine kind of count, and thus unable to fetch count data.".to_string(),
            );
            return count_data;
        }
        Err(_) => {
            count_data.message = Some(
                "Error fetching count kind from database, and thus unable to fetch count data."
                    .to_string(),
            );
            return count_data;
        }
    };

    // Get data according to format/count kind and put into appropriate variable of template.
    match format {
        CountDataFormat::Volume15Min => match count_kind {
            CountKind::FifteenMinVolume => {
                count_data.fifteen_min_vehicle =
                    match FifteenMinuteVehicle::select(&conn, recordnum) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            count_data.message = Some(format!("{e}"));
                            return count_data;
                        }
                    };
            }
            CountKind::Bicycle1
            | CountKind::Bicycle2
            | CountKind::Bicycle3
            | CountKind::Bicycle4
            | CountKind::Bicycle5
            | CountKind::Bicycle6 => {
                count_data.fifteen_min_bike = match FifteenMinuteBicycle::select(&conn, recordnum) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        count_data.message = Some(format!("{e}"));
                        return count_data;
                    }
                };
            }
            CountKind::Pedestrian | CountKind::Pedestrian2 => {
                count_data.fifteen_min_ped = match FifteenMinutePedestrian::select(&conn, recordnum)
                {
                    Ok(v) => Some(v),
                    Err(e) => {
                        count_data.message = Some(format!("{e}"));
                        return count_data;
                    }
                };
            }
            _ => (),
        },
        CountDataFormat::VolumeHourly => {}
        CountDataFormat::VolumeDayByHour => {
            if matches!(
                count_kind,
                CountKind::Class | CountKind::Volume | CountKind::FifteenMinVolume
            ) {
                count_data.non_normal_volume = match NonNormalVolCount::select(&conn, recordnum) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        count_data.message = Some(format!("{e}"));
                        return count_data;
                    }
                };
            } else {
                count_data.message = Some(format!(
                    "{:?} format is not available for {:?} counts.",
                    format, count_kind
                ));
            }
        }
        CountDataFormat::Class15Min => {
            if count_kind == CountKind::Class {
                count_data.fifteen_min_class =
                    match TimeBinnedVehicleClassCount::select(&conn, recordnum) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            count_data.message = Some(format!("{e}"));
                            return count_data;
                        }
                    };
            } else {
                count_data.message = Some(format!(
                    "{:?} format is not available for {:?} counts.",
                    format, count_kind
                ));
            }
        }
        CountDataFormat::ClassHourly => {}
        CountDataFormat::Speed15Min => {
            if count_kind == CountKind::Speed || count_kind == CountKind::Class {
                count_data.fifteen_min_speed =
                    match TimeBinnedSpeedRangeCount::select(&conn, recordnum) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            count_data.message = Some(format!("{e}"));
                            return count_data;
                        }
                    };
            } else {
                count_data.message = Some(format!(
                    "{:?} format is not available for {:?} counts.",
                    format, count_kind
                ));
            }
        }
        CountDataFormat::SpeedHourly => {}
        CountDataFormat::SpeedDayByHour => {
            if count_kind == CountKind::Speed || count_kind == CountKind::Class {
                count_data.non_normal_avg_speed =
                    match NonNormalAvgSpeedCount::select(&conn, recordnum) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            count_data.message = Some(format!("{e}"));
                            return count_data;
                        }
                    };
            } else {
                count_data.message = Some(format!(
                    "{:?} format is not available for {:?} counts.",
                    format, count_kind
                ));
            }
        }
    }
    count_data
}

#[derive(Template, Debug, Default)]
#[template(path = "admin/insert.html")]
struct AdminInsertTemplate {
    message: Option<String>,
    condition: ResponseCondition,
}

impl Heading for AdminInsertTemplate {
    const NAV_ITEM_TEXT: &str = "Insert Empty Records";
}

#[derive(Deserialize, Debug)]
struct AdminInsertForm {
    number_to_create: Option<u32>,
}

async fn get_insert() -> AdminInsertTemplate {
    AdminInsertTemplate::default()
}

async fn post_insert(
    State(state): State<AppState>,
    Form(input): Form<AdminInsertForm>,
) -> AdminInsertTemplate {
    let conn = state.conn_pool.get().unwrap();

    let (message, condition) = match input.number_to_create {
        Some(v) => match db::insert_empty_metadata(&conn, v) {
            Ok(w) => (
                format!("New records created {:?}", w),
                ResponseCondition::Success,
            ),
            Err(e) => (format!("Error: {e}."), ResponseCondition::GetInput),
        },
        None => (
            format!(
                "Please specify a number of records to create, from 1 to {}",
                db::RECORD_CREATION_LIMIT
            ),
            ResponseCondition::GetInput,
        ),
    };

    AdminInsertTemplate {
        message: Some(message),
        condition,
    }
}

#[derive(Template, Debug, Default)]
#[template(path = "admin/import_log.html")]
struct AdminImportLogTemplate {
    message: Option<String>,
    recordnum: Option<u32>,
    log_records: Vec<ImportLogEntry>,
}

impl Heading for AdminImportLogTemplate {
    const NAV_ITEM_TEXT: &str = "View Import Log";
}

async fn get_view_import_log(
    State(state): State<AppState>,
    params: Query<RecordnumFilterParams>,
) -> AdminImportLogTemplate {
    let conn = state.conn_pool.get().unwrap();
    let params = params.0;
    let mut template = AdminImportLogTemplate {
        message: None,
        recordnum: None,
        log_records: vec![],
    };

    if params.clear.is_some() || params.recordnum.is_none() {
        match db::get_import_log(&conn, None) {
            Ok(v) => {
                template.log_records = v;
            }
            Err(e) => {
                template.message = Some(format!("Error: {e}"));
            }
        }
    } else if let Some(v) = params.recordnum {
        template.recordnum = Some(v);
        match db::get_import_log(&conn, Some(v)) {
            Ok(w) if w.is_empty() => {
                template.message = Some(format!("No import log records found for recordnum {v}."));
            }
            Ok(w) => {
                template.log_records = w;
            }
            Err(e) => {
                template.message = Some(format!("Error: {e}"));
            }
        }
    }
    template
}

#[derive(Template, Debug, Default)]
#[template(path = "counts/aadv.html")]
struct AadvTemplate {
    message: Option<String>,
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
        message: None,
        recordnum: None,
        aadv: vec![],
    };

    if params.clear.is_some() || params.recordnum.is_none() {
        match aadv::get_aadv(&conn, None) {
            Ok(v) => template.aadv = v,
            Err(e) => {
                template.message = Some(format!("Error fetching AADV from database: {e}."));
            }
        }
    } else if let Some(v) = params.recordnum {
        template.recordnum = Some(v);
        match aadv::get_aadv(&conn, Some(v)) {
            Ok(w) => template.aadv = w,
            Err(e) => {
                template.message = Some(format!("Error fetching AADV from database: {e}"));
            }
        }
    }

    template
}

#[derive(Template, Default, Debug)]
#[template(path = "home.html")]
struct HomeTemplate {
    message: Option<String>,
}

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

pub fn display_some<T>(value: &Option<T>) -> String
where
    T: std::fmt::Display,
{
    match value {
        Some(value) => value.to_string(),
        None => String::new(),
    }
}

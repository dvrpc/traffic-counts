use std::collections::HashMap;
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
    db::{self, crud::Crud, ImportLogEntry},
    denormalize::NonNormalVolCount,
    CountKind, Metadata,
};

const ADMIN_PATH: &str = "/admin";
const ADMIN_METADATA_LIST_PATH: &str = "/admin/metadata-list";
const ADMIN_METADATA_DETAIL_PATH: &str = "/admin/metadata-detail";
const ADMIN_METADATA_INSERT_PATH: &str = "/admin/insert";
const ADMIN_COUNT_DATA: &str = "/admin/count";
const ADMIN_IMPORT_LOG_PATH: &str = "/admin/import-log";

/// A trait to set the heading for the main section of the page by template.
pub trait Heading {
    fn heading() -> String;
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
        .route_with_tsr(
            ADMIN_IMPORT_LOG_PATH,
            get(get_view_import_log).post(post_view_import_log),
        )
        .route_with_tsr(ADMIN_COUNT_DATA, get(get_count_data))
        .with_state(state)
        .nest_service("/static", ServeDir::new("static"));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

/// The condition of the response - getting input (possibly again after bad input) or success.
#[derive(Default, PartialEq, Debug)]
enum ResponseCondition {
    #[default]
    GetInput,
    Success,
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
    fn heading() -> String {
        "Welcome".to_string()
    }
}

async fn admin() -> AdminMainTemplate {
    AdminMainTemplate::default()
}

#[derive(Template, Debug, Default)]
#[template(path = "admin/metadata_list.html")]
struct AdminMetadataListTemplate {
    message: Option<String>,
    condition: ResponseCondition,
    metadata: Vec<Metadata>,
    total_pages: u32,
    page: u32,
}

impl Heading for AdminMetadataListTemplate {
    fn heading() -> String {
        "View Count Metadata Records".to_string()
    }
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
    let mut message = None;

    let conn = state.conn_pool.get().unwrap();

    let metadata = match db::get_metadata_paginated(&conn, Some(page * results_per_page), None) {
        Ok(v) => v,
        // TODO: handle this later
        Err(e) => {
            message = Some(format!("{e}"));
            vec![]
        }
    };
    let total_pages = state.num_metadata_records / results_per_page;
    AdminMetadataListTemplate {
        message,
        condition: ResponseCondition::GetInput,
        metadata,
        total_pages,
        page,
    }
}

#[derive(Template, Debug, Default)]
#[template(path = "admin/metadata_detail.html")]
struct AdminMetadataDetailTemplate {
    message: Option<String>,
    condition: ResponseCondition,
    metadata: Option<Metadata>,
}

impl Heading for AdminMetadataDetailTemplate {
    fn heading() -> String {
        "View Count Metadata".to_string()
    }
}

async fn get_metadata_detail(
    State(state): State<AppState>,
    recordnum: Query<HashMap<String, u32>>,
) -> AdminMetadataDetailTemplate {
    let conn = state.conn_pool.get().unwrap();
    let mut detail = AdminMetadataDetailTemplate {
        message: None,
        condition: ResponseCondition::GetInput,
        metadata: None,
    };
    let recordnum = match recordnum.0.get("recordnum") {
        Some(v) => v,
        None => {
            detail.message = Some("Please provide a record number.".to_string());
            return detail;
        }
    };

    detail.metadata = match db::get_metadata(&conn, *recordnum) {
        Ok(v) => Some(v),
        Err(e) => {
            // Handle the one error that is probable (no matching recordnum in db).
            if e.source().is_some_and(|v| {
                matches!(
                    v.downcast_ref::<OracleError>().unwrap().kind(),
                    OracleErrorKind::NoDataFound
                )
            }) {
                detail.message = Some(format!("Record {recordnum} not found."))
            } else {
                detail.message = Some(format!("{e}"))
            }
            return detail;
        }
    };
    detail
}

#[derive(Template, Debug, Default)]
#[template(path = "admin/count_data.html")]
struct AdminCountDataTemplate {
    message: Option<String>,
    condition: ResponseCondition,
    non_normal_volcount: Option<Vec<NonNormalVolCount>>,
}

impl Heading for AdminCountDataTemplate {
    fn heading() -> String {
        "Count data".to_string()
    }
}

async fn get_count_data(
    State(state): State<AppState>,
    recordnum: Query<HashMap<String, u32>>,
) -> AdminCountDataTemplate {
    let conn = state.conn_pool.get().unwrap();
    let mut count_data = AdminCountDataTemplate {
        message: None,
        condition: ResponseCondition::GetInput,
        non_normal_volcount: None,
    };
    let recordnum = match recordnum.0.get("recordnum") {
        Some(v) => v,
        None => {
            count_data.message = Some("Please provide a record number.".to_string());
            return count_data;
        }
    };
    count_data.non_normal_volcount = match NonNormalVolCount::select(&conn, *recordnum) {
        Ok(v) => Some(v),
        Err(e) => {
            count_data.message = Some(format!("{e}"));
            return count_data;
        }
    };
    count_data
}

#[derive(Template, Debug, Default)]
#[template(path = "admin/insert.html")]
struct AdminInsertTemplate {
    message: Option<String>,
    condition: ResponseCondition,
}

impl Heading for AdminInsertTemplate {
    fn heading() -> String {
        "Insert Empty Records".to_string()
    }
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
    log_records: Vec<ImportLogEntry>,
}

impl Heading for AdminImportLogTemplate {
    fn heading() -> String {
        "View Import Log".to_string()
    }
}

#[derive(Deserialize, Debug)]
struct AdminImportLogForm {
    // We really want an `Option<u32>` here, but for some reason serde cannot handle
    // the None variant properly, so have to parse it manually.
    #[serde(default)]
    recordnum: String,
    clear: Option<String>,
}

async fn get_view_import_log(State(state): State<AppState>) -> AdminImportLogTemplate {
    let conn = state.conn_pool.get().unwrap();
    let (message, log_records) = match db::get_import_log(&conn, None) {
        Ok(v) => (Some("".to_string()), v),
        Err(e) => (Some(format!("Error: {e}")), vec![]),
    };

    AdminImportLogTemplate {
        message,
        log_records,
    }
}

async fn post_view_import_log(
    State(state): State<AppState>,
    Form(input): Form<AdminImportLogForm>,
) -> AdminImportLogTemplate {
    let conn = state.conn_pool.get().unwrap();

    let (message, log_records) = if input.clear.is_some() {
        match db::get_import_log(&conn, None) {
            Ok(v) => (Some("".to_string()), v),
            Err(e) => (Some(format!("Error: {e}")), vec![]),
        }
    } else if input.recordnum.is_empty() {
        (Some("Please specify a recordnum.".to_string()), vec![])
    } else {
        match input.recordnum.parse() {
            Ok(v) => match db::get_import_log(&conn, Some(v)) {
                Ok(w) if w.is_empty() => (
                    Some(format!("No import log records found for recordnum {v}.")),
                    vec![],
                ),
                Ok(w) => (None, w),
                Err(e) => (Some(format!("Error: {e}.")), vec![]),
            },
            Err(e) => (Some(format!("Error: {e}.")), vec![]),
        }
    };

    AdminImportLogTemplate {
        message,
        log_records,
    }
}

#[derive(Template, Default, Debug)]
#[template(path = "home.html")]
struct HomeTemplate {
    message: Option<String>,
}

impl Heading for HomeTemplate {
    fn heading() -> String {
        "main".to_string()
    }
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

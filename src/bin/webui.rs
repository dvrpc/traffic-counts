use std::env;
use std::fmt;
use std::str::FromStr;

use axum::{
    extract::{Form, Query, State},
    routing::get,
    Router,
};
use axum_extra::routing::RouterExt;
use oracle::pool::Pool;
use rinja_axum::Template;
use serde::{de, Deserialize, Deserializer};
use tower_http::services::ServeDir;

use traffic_counts::{
    db::{self, LogRecord},
    Metadata,
};

const ADMIN_URL: &str = "/admin";

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
        .route_with_tsr(ADMIN_URL, get(admin))
        .route_with_tsr(&format!("{ADMIN_URL}/view"), get(get_view))
        .route_with_tsr(
            &format!("{ADMIN_URL}/insert"),
            get(get_insert).post(post_insert),
        )
        .route_with_tsr(
            &format!("{ADMIN_URL}/view-import-log"),
            get(get_view_import_log).post(post_view_import_log),
        )
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
#[template(path = "admin/view.html")]
struct AdminViewTemplate {
    message: Option<String>,
    condition: ResponseCondition,
    metadata: Vec<Metadata>,
    total_pages: u32,
    page: u32,
}

impl Heading for AdminViewTemplate {
    fn heading() -> String {
        "View Count Metadata Records".to_string()
    }
}

#[derive(Deserialize)]
struct Page {
    #[serde(default, deserialize_with = "empty_string_as_none")]
    page: Option<u32>,
}

async fn get_view(State(state): State<AppState>, page: Query<Page>) -> AdminViewTemplate {
    let results_per_page = 100;
    let page = page.0.page.unwrap_or(1);
    let mut message = None;

    let conn = state.conn_pool.get().unwrap();

    let metadata = match db::get_metadata(&conn, Some(page * results_per_page), None) {
        Ok(v) => v,
        // TODO: handle this later
        Err(e) => {
            message = Some(format!("{e}"));
            vec![]
        }
    };
    let total_pages = state.num_metadata_records / results_per_page;
    AdminViewTemplate {
        message,
        condition: ResponseCondition::GetInput,
        metadata,
        total_pages,
        page,
    }
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
#[template(path = "admin/view_import_log.html")]
struct AdminViewImportLogTemplate {
    message: Option<String>,
    log_records: Vec<LogRecord>,
}

impl Heading for AdminViewImportLogTemplate {
    fn heading() -> String {
        "View Import Log".to_string()
    }
}

#[derive(Deserialize, Debug)]
struct AdminViewImportLogForm {
    // We really want an `Option<u32>` here, but for some reason serde cannot handle
    // the None variant properly, so have to parse it manually.
    #[serde(default)]
    recordnum: String,
    clear: Option<String>,
}

async fn get_view_import_log(State(state): State<AppState>) -> AdminViewImportLogTemplate {
    let conn = state.conn_pool.get().unwrap();
    let (message, log_records) = match db::get_import_log(&conn, None) {
        Ok(v) => (Some("".to_string()), v),
        Err(e) => (Some(format!("Error: {e}")), vec![]),
    };

    AdminViewImportLogTemplate {
        message,
        log_records,
    }
}

async fn post_view_import_log(
    State(state): State<AppState>,
    Form(input): Form<AdminViewImportLogForm>,
) -> AdminViewImportLogTemplate {
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

    AdminViewImportLogTemplate {
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

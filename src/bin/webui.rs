use std::env;

use axum::{
    extract::{Form, State},
    routing::get,
    Router,
};
use oracle::pool::Pool;
use rinja_axum::Template;
use serde::Deserialize;
use tower_http::services::ServeDir;

use traffic_counts::db::{self, LogRecord};

const ADMIN_URL: &str = "/admin";

#[derive(Clone)]
struct AppState {
    conn_pool: Pool,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().expect("Unable to load .env file.");
    let username = env::var("DB_USERNAME").unwrap();
    let password = env::var("DB_PASSWORD").unwrap();
    let conn_pool = db::create_pool(username, password).unwrap();

    let state = AppState { conn_pool };
    let app = Router::new()
        .route("/", get(home))
        .route(ADMIN_URL, get(admin))
        .route(
            &format!("{ADMIN_URL}/insert"),
            get(get_insert).post(post_insert),
        )
        .route(
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

/// A trait to set the heading for the <main> section of the page by template.
pub trait Heading {
    fn heading() -> String;
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

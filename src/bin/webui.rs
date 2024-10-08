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
            &format!("{ADMIN_URL}/insert-one"),
            get(get_insert_one).post(post_insert_one),
        )
        .route(
            &format!("{ADMIN_URL}/insert-many"),
            get(get_insert_many).post(post_insert_many),
        )
        .route(&format!("{ADMIN_URL}/show-full-log"), get(show_full_log))
        .route(
            &format!("{ADMIN_URL}/show-one-log"),
            get(get_show_one_log).post(post_show_one_log),
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
#[template(path = "admin/insert_one.html")]
struct AdminInsertOneTemplate {
    message: Option<String>,
    condition: ResponseCondition,
}

impl Heading for AdminInsertOneTemplate {
    fn heading() -> String {
        "Insert One New Record".to_string()
    }
}

async fn get_insert_one() -> AdminInsertOneTemplate {
    AdminInsertOneTemplate {
        message: None,
        condition: ResponseCondition::GetInput,
    }
}

async fn post_insert_one(State(state): State<AppState>) -> AdminInsertOneTemplate {
    let conn = state.conn_pool.get().unwrap();
    let (message, condition) = match db::insert_empty_metadata(&conn, 1) {
        Ok(v) => (
            Some(format!("New record created {}", v[0])),
            ResponseCondition::Success,
        ),
        Err(e) => (Some(format!("Error: {e}")), ResponseCondition::GetInput),
    };
    AdminInsertOneTemplate { message, condition }
}

#[derive(Template, Debug, Default)]
#[template(path = "admin/insert_many.html")]
struct AdminInsertManyTemplate {
    message: Option<String>,
    condition: ResponseCondition,
}

impl Heading for AdminInsertManyTemplate {
    fn heading() -> String {
        "Insert Many Empty Records".to_string()
    }
}

#[derive(Deserialize, Debug)]
struct AdminInsertManyForm {
    number_to_create: Option<u32>,
}

async fn get_insert_many() -> AdminInsertManyTemplate {
    AdminInsertManyTemplate::default()
}

async fn post_insert_many(
    State(state): State<AppState>,
    Form(input): Form<AdminInsertManyForm>,
) -> AdminInsertManyTemplate {
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

    AdminInsertManyTemplate {
        message: Some(message),
        condition,
    }
}

#[derive(Template, Debug, Default)]
#[template(path = "admin/show_full_log.html")]
struct AdminShowFullLogTemplate {
    message: Option<String>,
    log_records: Vec<LogRecord>,
}

impl Heading for AdminShowFullLogTemplate {
    fn heading() -> String {
        "Show Full Import Log".to_string()
    }
}

async fn show_full_log(State(state): State<AppState>) -> AdminShowFullLogTemplate {
    let conn = state.conn_pool.get().unwrap();
    let (message, log_records) = match db::get_import_log(&conn, None) {
        Ok(v) => (Some("".to_string()), v),
        Err(e) => (Some(format!("Error: {e}")), vec![]),
    };

    AdminShowFullLogTemplate {
        message,
        log_records,
    }
}

#[derive(Template, Debug, Default)]
#[template(path = "admin/show_one_log.html")]
struct AdminShowOneLogTemplate {
    message: Option<String>,
    condition: ResponseCondition,
    log_records: Vec<LogRecord>,
}

impl Heading for AdminShowOneLogTemplate {
    fn heading() -> String {
        "Show Import Log for Specific Record".to_string()
    }
}

#[derive(Deserialize, Debug)]
struct AdminShowOneLogForm {
    recordnum: Option<u32>,
}

async fn get_show_one_log() -> AdminShowOneLogTemplate {
    AdminShowOneLogTemplate::default()
}

async fn post_show_one_log(
    State(state): State<AppState>,
    Form(input): Form<AdminShowOneLogForm>,
) -> AdminShowOneLogTemplate {
    let conn = state.conn_pool.get().unwrap();
    let (message, condition, log_records) = match input.recordnum {
        Some(v) => match db::get_import_log(&conn, Some(v)) {
            Ok(w) => {
                if w.is_empty() {
                    (
                        Some(format!("No import log records found for recordnum {v}.")),
                        ResponseCondition::GetInput,
                        vec![],
                    )
                } else {
                    (None, ResponseCondition::Success, w)
                }
            }
            Err(e) => (
                Some(format!("Error: {e}.")),
                ResponseCondition::GetInput,
                vec![],
            ),
        },
        None => (
            Some("Please specify a recordnum.".to_string()),
            ResponseCondition::GetInput,
            vec![],
        ),
    };

    AdminShowOneLogTemplate {
        message,
        condition,
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

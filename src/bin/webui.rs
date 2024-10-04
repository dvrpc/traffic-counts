use std::convert::AsRef;
use std::env;
use std::fmt::Display;
use std::str::FromStr;

use axum::{extract::Form, routing::get, Router};
use rinja_axum::Template;
use serde::{Deserialize, Serialize};
use strum_macros::{AsRefStr, EnumString};
use tower_http::services::ServeDir;

use traffic_counts::db::{self, LogRecord};

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(home))
        .route("/admin", get(admin).post(process_admin))
        .route("/viewer", get(viewer))
        .nest_service("/static", ServeDir::new("static"));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(Serialize, Debug, PartialEq, Clone, AsRefStr, EnumString)]
enum AdminAction {
    Start,
    InsertOne,
    InsertOneConfirm,
    InsertMany,
    InsertWithTemplate,
    EditOne,
    CreatePackets,
    ImportEcoCounter,
    ImportJamar,
    ShowFullLog,
    ShowOneLog,
    InsertFactors,
    UpdateFactors,
}

/// These will be used as text for sidebar menu buttons and/or section titles, depending on use.
impl Display for AdminAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let r = match self {
            AdminAction::Start => "Welcome",
            AdminAction::InsertOne => "Insert One Empty Record",
            AdminAction::InsertOneConfirm => "Insert One Empty Record",
            AdminAction::InsertMany => "Insert Many Empty Records",
            AdminAction::InsertWithTemplate => {
                "Insert One or More Records Using Existing Record as Template"
            }
            AdminAction::EditOne => "Edit Record",
            AdminAction::CreatePackets => "Create Packets",
            AdminAction::ImportEcoCounter => "Import from EcoCounter",
            AdminAction::ImportJamar => "Import from Jamar",
            AdminAction::ShowFullLog => "Show Full Import Log",
            AdminAction::ShowOneLog => "Show Import Log for Specific Record",
            AdminAction::InsertFactors => "Insert Factors",
            AdminAction::UpdateFactors => "Update Factors",
        };
        write!(f, "{r}")
    }
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct Input {
    action: String,
    recordnum: Option<u32>,
}

#[derive(Template, Debug)]
#[template(path = "admin_main.html")]
struct AdminMainTemplate<'a> {
    header_text: &'a str,
    admin_action: AdminAction,
    log_records: Option<Vec<LogRecord>>,
    message: Option<String>,
}
async fn admin() -> AdminMainTemplate<'static> {
    AdminMainTemplate {
        header_text: "Traffic Counts: Admin",
        admin_action: AdminAction::Start,
        log_records: None,
        message: None,
    }
}

async fn process_admin(Form(input): Form<Input>) -> AdminMainTemplate<'static> {
    let action = AdminAction::from_str(&input.action).unwrap();
    let recordnum = match &input.recordnum {
        Some(v) => Some(v),
        None => None,
    };

    dotenvy::dotenv().expect("Unable to load .env file.");
    let username = env::var("DB_USERNAME").unwrap();
    let password = env::var("DB_PASSWORD").unwrap();
    let pool = db::create_pool(username, password).unwrap();
    let conn = pool.get().unwrap();

    let mut template = AdminMainTemplate {
        header_text: "Traffic Counts: Admin",
        admin_action: action.clone(),
        log_records: None,
        message: None,
    };

    match action {
        AdminAction::Start => (),
        AdminAction::ShowFullLog => {
            let log_records = Some(db::get_import_log(&conn, None).unwrap());
            template.log_records = log_records;
            template.admin_action = AdminAction::ShowFullLog;
        }
        AdminAction::ShowOneLog => {
            if let Some(v) = recordnum {
                match db::get_import_log(&conn, Some(*v)) {
                    Ok(w) => {
                        template.log_records = Some(w);
                    }
                    Err(_) => {
                        template.message =
                            Some(format!("Recordnum {v} not found or error running query."));
                    }
                }
            }
        }
        AdminAction::InsertOneConfirm => match db::insert_empty_metadata(&conn) {
            Ok(v) => {
                template.message = Some(format!("New record created {v}"));
            }
            Err(e) => {
                template.message = Some(format!("Error: {e}"));
            }
        },
        _ => {}
    };

    template
}

#[derive(Template)]
#[template(path = "viewer_main.html")]
struct ViewerMainTemplate<'a> {
    header_text: &'a str,
}
async fn viewer() -> ViewerMainTemplate<'static> {
    ViewerMainTemplate {
        header_text: "Traffic Counts: Viewer",
    }
}

#[derive(Template)]
#[template(path = "home.html")]
struct HomeTemplate {}

async fn home() -> HomeTemplate {
    HomeTemplate {}
}

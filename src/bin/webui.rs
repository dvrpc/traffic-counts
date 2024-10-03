use std::env;
use std::fmt::Display;

use axum::{extract::Form, routing::get, Router};
use rinja_axum::Template;
use serde::{Deserialize, Serialize};
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

#[derive(Serialize, Debug, PartialEq, Clone)]
enum AdminAction {
    Start,
    InsertOne,
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

impl<'a> AdminAction {
    fn long_display(&self) -> &'a str {
        match self {
            AdminAction::Start => "Welcome",
            AdminAction::InsertOne => "Insert One Empty Record",
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
        }
    }
    fn from_str(str: &'a str) -> Self {
        match str {
            "show_full_log" => AdminAction::ShowFullLog,
            "show_one_log" => AdminAction::ShowOneLog,
            _ => AdminAction::InsertOne,
        }
    }
}

impl Display for AdminAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let r = match self {
            AdminAction::Start => "start",
            AdminAction::InsertOne => "insert_one",
            AdminAction::InsertMany => "insert_many",
            AdminAction::InsertWithTemplate => "insert_with_template",
            AdminAction::EditOne => "edit",
            AdminAction::CreatePackets => "create_packets",
            AdminAction::ImportEcoCounter => "import_ecocounter",
            AdminAction::ImportJamar => "import_jamar",
            AdminAction::ShowFullLog => "show_full_log",
            AdminAction::ShowOneLog => "show_one_log",
            AdminAction::InsertFactors => "insert_factors",
            AdminAction::UpdateFactors => "update_factors",
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
    let action = AdminAction::from_str(&input.action.to_string());
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
            template.log_records = log_records.clone();
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

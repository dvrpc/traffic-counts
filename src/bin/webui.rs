use axum::{extract::Form, routing::get, Router};
use rinja_axum::Template;
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use tower_http::services::ServeDir;

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/admin", get(admin).post(process_admin))
        .route("/viewer", get(viewer))
        .nest_service("/static", ServeDir::new("static"));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(Serialize, Debug, strum::EnumIter, PartialEq)]
enum AdminAction {
    Start,
    InsertOne,
    InsertMany,
    InsertWithTemplate,
    Edit,
    CreatePackets,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct Input {
    action: String,
}

#[derive(Template, Debug)]
#[template(path = "admin_main.html")]
struct AdminMainTemplate<'a> {
    header_text: &'a str,
    test_response: &'a str,
    all_actions: Vec<AdminAction>,
    action: AdminAction,
}
async fn admin() -> AdminMainTemplate<'static> {
    let all_actions = AdminAction::iter().collect::<Vec<_>>();

    AdminMainTemplate {
        header_text: "Traffic Counts: Admin",
        test_response: "nothing",
        all_actions: all_actions,
        action: AdminAction::Start,
    }
}
async fn process_admin(Form(input): Form<Input>) -> AdminMainTemplate<'static> {
    dbg!(input);
    let all_actions = AdminAction::iter().collect::<Vec<_>>();

    let test_response = "this";
    // let test_response = match
    //     AdminAction::InsertOne => ""
    AdminMainTemplate {
        header_text: "Traffic Counts: Admin",
        test_response,
        all_actions: all_actions,
        action: AdminAction::Start,
    }
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

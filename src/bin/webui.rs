use axum::{routing::get, Router};
use rinja_axum::Template;

#[tokio::main]
async fn main() {
    // build our application with a single route
    // let hello = HelloTemplate { name: "world" };
    // println!("{}", hello.render().unwrap());
    // let app = Router::new().route("/", get(|| async { "Hello, World!" }));
    let app = Router::new().route("/", get(hello));

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(Template)]
#[template(path = "hello.html")]
struct HelloTemplate<'a> {
    // the name of the struct can be anything
    name: &'a str, // the field name should match the variable name
                   // in your template
}

async fn hello() -> HelloTemplate<'static> {
    HelloTemplate { name: "world" }
}

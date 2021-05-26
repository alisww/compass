use rustventually::*;
use actix_web::{get, web, App, HttpServer, HttpResponse, Error};
use deadpool_postgres::{Config, ManagerConfig, Client, Pool, RecyclingMethod };
use tokio_postgres::{NoTls};
use std::fs::File;
use std::io::prelude::*;
use serde_json::Value as JSONValue;

#[get("/search")]
async fn search(req: web::Query<JSONValue>, db_pool: web::Data<Pool>, schema: web::Data<Schema>) -> Result<HttpResponse,Error> {
    let client: Client = db_pool.get().await.map_err(CompassError::PoolError)?;
    let res = json_search(&client,&schema,&req).await.unwrap(); // don't unwrap. please
    Ok(HttpResponse::Ok().json(res))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let mut cfg = Config::new();
    cfg.dbname = Some("eventually-dev".to_string());
    cfg.manager = Some(ManagerConfig { recycling_method: RecyclingMethod::Fast });
    let pool = cfg.create_pool(NoTls).unwrap();

    let mut file = File::open("schema.yaml").unwrap();
    let mut s = String::new();
    file.read_to_string(&mut s).unwrap();
    let schema: Schema = serde_yaml::from_str(&s).unwrap();

    let server = HttpServer::new(move || {
        App::new()
            .data(pool.clone())
            .data(schema.clone())
            .service(search)
    })
    .bind("localhost:4444".to_string())?
    .run();

    server.await
}

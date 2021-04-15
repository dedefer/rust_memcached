mod memcached;
mod api;
mod settings;

use actix_web::{
    HttpServer, App,
    middleware::Logger,
};
use env_logger;
use std::io::{
    Result, Error,
    ErrorKind::InvalidInput,
};

use crate::settings::Settings;

#[actix_web::main]
async fn main() -> Result<()> {
    env_logger::init();
    let Settings { memory_limit, gc_interval, addr } = Settings::new()
        .map_err(|err| Error::new(InvalidInput, err))?;

    HttpServer::new(move ||
        App::new()
        .service(api::memcached(memory_limit as usize, gc_interval.into()))
        .wrap(Logger::default())
    ).workers(1)
    .bind(addr)?
    .run()
    .await
}

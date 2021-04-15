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

use crate::{
    memcached::Memcached,
    settings::Settings,
};


#[actix_web::main]
async fn main() -> Result<()> {
    env_logger::init();
    let Settings {
        memory_limit, gc_interval,
        addr, workers
    } = Settings::new()
        .map_err(|err| Error::new(InvalidInput, err))?;

    let mc = Memcached::new(memory_limit as usize);
    let service_factory = mc.service(gc_interval.into());

    let mut builder = HttpServer::new(move ||
        App::new()
        .service(service_factory())
        .wrap(Logger::default())
    );

    if let Some(workers) = workers {
        builder = builder.workers(workers as usize);
    }

    builder.bind(addr)?
    .run()
    .await
}

use std::thread;
use serde::{Serialize, Deserialize};
use actix_web::{
    post, HttpResponse, Responder, Scope,
    web::{Data, scope, Json},
};
use std::sync::{RwLock, Arc};
use std::time::Duration;
use duration_string::DurationString;

use crate::memcached::Memcached;


pub fn memcached(memory_limit: usize, gc_interval: Duration) -> Scope {
    let mc = Arc::new(RwLock::new(
        Memcached::new(memory_limit)
    ));

    let mc_for_gc = mc.clone();
    thread::spawn(move || gc(mc_for_gc, gc_interval));

    scope("/")
        .app_data(Data::from(mc))
        .service(get)
        .service(set)
        .service(delete)
}


#[derive(Deserialize)]
struct GetReq {
    key: String,
}

#[derive(Serialize)]
struct GetResp {
    data: String,
}

#[post("/get")]
async fn get(
    mc: Data<RwLock<Memcached>>,
    req: Json<GetReq>,
) -> impl Responder {
    match mc.read().unwrap().get(&req.key) {
        Some(data) => HttpResponse::Ok().json(GetResp { data: String::from_utf8(data).unwrap() }),
        None => HttpResponse::NotFound().finish(),
    }
}

#[derive(Deserialize)]
struct SetReq {
    key: String,
    data: String,
    ttl: Option<DurationString>,
}

#[post("/set")]
async fn set(
    mc: Data<RwLock<Memcached>>,
    req: Json<SetReq>,
) -> impl Responder {
    match mc.write().unwrap().set(
        &req.key, req.data.as_bytes(),
        req.ttl.map(Into::into),
    ) {
        true => HttpResponse::Ok(),
        false => HttpResponse::NotModified(),
    }.finish()
}

#[derive(Deserialize)]
struct DeleteReq {
    key: String,
}

#[post("/delete")]
async fn delete(
    mc: Data<RwLock<Memcached>>,
    req: Json<DeleteReq>,
) -> impl Responder {
    match mc.write().unwrap().delete(&req.key) {
        true => HttpResponse::Ok(),
        false => HttpResponse::NotFound(),
    }.finish()
}

fn gc(mc: Arc<RwLock<Memcached>>, interval: Duration) {
    loop {
        thread::sleep(interval);
        {
            let mut mc = mc.write().unwrap();
            mc.collect_garbage();
        }
    }
}

use serde::{Serialize, Deserialize};
use actix_web::{
    post, HttpResponse as Code,
    Responder, Scope,
    web::{Data, scope, Json},
};
use std::{
    thread,
    sync::{RwLock, Arc},
    time::Duration,
};
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
        Some(data) => Code::Ok().json(GetResp { data: as_string(data) }),
        None => Code::NotFound().finish(),
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
        true => Code::Ok(),
        false => Code::NotModified(),
    }.finish()
}

#[derive(Deserialize)]
struct DeleteReq {
    key: String,
}

#[derive(Serialize)]
struct DeleteResp {
    data: String,
}

#[post("/delete")]
async fn delete(
    mc: Data<RwLock<Memcached>>,
    req: Json<DeleteReq>,
) -> impl Responder {
    match mc.write().unwrap().delete(&req.key) {
        Some(data) => Code::Ok().json(DeleteResp { data: as_string(data) }),
        None => Code::NotFound().finish(),
    }
}

fn gc(mc: Arc<RwLock<Memcached>>, interval: Duration) {
    loop {
        thread::sleep(interval);
        mc.write().unwrap().collect_garbage();
    }
}

fn as_string(vec: Vec<u8>) -> String {
    String::from_utf8(vec).unwrap()
}

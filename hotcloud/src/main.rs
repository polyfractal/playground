#![feature(custom_derive, plugin)]

#[macro_use] extern crate log;
extern crate env_logger;
extern crate toml;
extern crate rustc_serialize;
extern crate rand;
extern crate hyper;
extern crate threadpool;
extern crate time;
extern crate chrono;

mod config;
mod query;
mod util;
mod generator;

use config::Config;
use hyper::Client;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering, ATOMIC_USIZE_INIT, ATOMIC_BOOL_INIT};
use std::thread;

pub static ACTIVE_THREADS: AtomicUsize = ATOMIC_USIZE_INIT;
pub static GENERATOR_RUNNING: AtomicBool = ATOMIC_BOOL_INIT;

pub enum Disruption {
    Node(usize),
    Query(Vec<usize>),
    Metric(Vec<usize>)
}

fn main() {
    env_logger::init().unwrap();

    let config = Config::parse("config.toml".to_owned());
    let client = Arc::new(Client::new());

    debug!("Resetting index...");
    client.delete("http://localhost:9200/data/").send();
    client.put("http://localhost:9200/data/").body(&config.es.mapping).send();

    client.delete("http://localhost:9200/hotcloud/").send();
    client.put("http://localhost:9200/hotcloud/").body(&config.es.hotcloudmapping).send();

    client.get("http://localhost:9200/_cluster/health?wait_for_status=yellow").send();
    client.delete("http://localhost:9200/_search/template/hotcloud").send();
    client.post("http://localhost:9200/_search/template/hotcloud").body(&config.es.query).send();

    generator::generate_timeline(&client, &config, false);

    while ::ACTIVE_THREADS.load(Ordering::SeqCst) > 0 {
        thread::sleep_ms(500);
    }

    let _ = client.put("http://localhost:9200/data/_refresh").send();
    query::run_hotcloud(&client, config);
}

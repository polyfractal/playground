
use config::Config;
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread;
use rand::distributions::{Normal, IndependentSample};
use rand::{thread_rng, ThreadRng, Rng};
use std::collections::HashMap;
use hyper::Client;
use rustc_serialize::{Encodable, Encoder};
use rustc_serialize::json::{self};
use threadpool::ThreadPool;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use chrono::{Duration, UTC};
use chrono::offset::TimeZone;
use std::fs::OpenOptions;
use std::io::Write;
use ::Disruption;

// The generated data-point for a particular (node,metric,query) tuple.
// value contains the generated gaussian, disruption represents if/what
// disruption was active
#[derive(RustcDecodable, RustcEncodable)]
struct TupleResult {
    node: usize,
    metric: usize,
    query: usize,
    hour: String,
    value: f64,
    disruption: usize
}

// Gaussian distribution
struct NormalParams {
    mean: usize,
    std: usize
}

// Run the simulation, generating a "cluster history" and simulated disruptions
pub fn generate_timeline(client: &Arc<Client>, config: &Config, json: bool) {

    // We generate gaussians on another thread and cache them in a channel
    // to be grabbed when necessary, since gaussian generation is a bit slow
    let rx = start_normal_generator();

    // Threadpool for the client bulks
    let pool = ThreadPool::new(config.threads);
    let mut bulk: Vec<TupleResult> = Vec::with_capacity(config.es.bulk_size);

    let mut rng = thread_rng();

    // Generate a list of disruptions that will be seeded into our timeline
    let disruptions = generate_disruptions(&config, &mut rng);

    // Generate the "regular" and "disrupted" distributions for
    // every (node,metric,query) tupe
    let distributions = generate_distributions(&config, &mut rng);

    // Generate the timeline
    debug!("Generating timeline...");
    let (mut disruption, mut counter) = (None, 0);
    for hour in 0..config.hours {

        debug!("{} -- {}", hour, bulk.len());

        // if we are not currently in a disruption, check to see
        // if there is one this hour
        if counter <= 0 {
            disruption = disruptions.get(&hour);
            counter = disruption.map_or(0, |&(_, x)| x);
        }

        if disruption.is_some() {
            print!(".");
        }

        // iterate through all the (node,query,metric) tuples
        for node in 0..config.nodes {
            for query in 0..config.queries {
                for metric in 0..config.metrics {

                    // Set the disruption flag for this tuple at this hour, based on the
                    // disruption type
                    let d = distributions.get(&(node, query, metric)).unwrap();
                    let is_disrupted = counter > 0 && match disruption {
                        None => false,
                        Some(&(Disruption::Query(ref v), _)) => v.contains(&query),
                        Some(&(Disruption::Metric(ref v), _)) => v.contains(&metric),
                        Some(&(Disruption::Node(ref v), _)) => *v == node
                    };

                    // Grab a gaussian from the normalGenerator thread and use the "regular"
                    // or "disrupted" distributions to find the final value
                    let value = match is_disrupted {
                        true => (rx.recv().unwrap() * d.1.std as f64) + d.1.mean as f64,
                        false => (rx.recv().unwrap() * d.0.std as f64) + d.0.mean as f64
                    };

                    let timestamp = UTC.ymd(2015, 10, 20).and_hms(0, 0, 0) + Duration::hours(hour as i64);

                    bulk.push(TupleResult {
                        node: node,
                        metric: metric,
                        query: query,
                        hour: timestamp.format("%Y-%m-%dT%H:%M:%S").to_string(),
                        value: value,
                        disruption: ::util::disruption_to_usize(&disruption)
                    });
                }
            }

            if bulk.len() >= config.es.bulk_size {
                // Option to dump to a JSON file instead of ES
                if json {
                    let mut file = OpenOptions::new()
                        .read(false)
                        .write(true)
                        .create(true)
                        .append(true)
                        .open("output.json")
                        .unwrap();

                    for b in bulk {
                        let _ = file.write(&json::encode(&b).unwrap().into_bytes());
                        let _ = file.write(b"\n");
                    }

                    bulk = Vec::with_capacity(config.es.bulk_size);

                } else {
                    // Otherwise wait for a free thread and fire off the bulk in the background
                    while ::ACTIVE_THREADS.load(Ordering::SeqCst) >= config.threads {
                        thread::sleep_ms(500);
                    }
                    debug!(".");
                    let client_clone = client.clone();
                    pool.execute(move|| {
                        ::util::send_bulk("http://localhost:9200/data/data/_bulk", &client_clone, bulk);
                    });
                    bulk = Vec::with_capacity(config.es.bulk_size);
                }
            }
        }

        // Decrease the disruption counter.  Disruptions are 2-24 hours long,
        // when counter reaches zero the disruption is over
        if counter > 0 {
            counter -= 1;
        }
    }

    ::GENERATOR_RUNNING.store(false, Ordering::SeqCst);
    ::util::send_bulk("http://localhost:9200/data/data/_bulk", &client, bulk);
}

// Background thread to generate gaussian values
fn start_normal_generator() -> Receiver<f64> {

    // sync channel will buffer until full then block the thread
    let (tx, rx) = sync_channel(32768);

    debug!("Starting gaussian thread...");
    thread::spawn(move|| {
        ::GENERATOR_RUNNING.store(true, Ordering::SeqCst);
        let normal = Normal::new(0.0, 1.0);
        let mut counter = 0;
        loop {
            let v = normal.ind_sample(&mut thread_rng());
            match tx.try_send(v) {
                Ok(_) => {},
                Err(_) => thread::sleep_ms(50)
            }

            if counter > 1024 {
                counter = 0;
                if ::GENERATOR_RUNNING.load(Ordering::SeqCst) == false {
                    break;
                }
            }

            counter += 1;
        }
    });

    rx
}

// Generate a timeline of simulated disruptions to seed
fn generate_disruptions(config: &Config, rng: &mut ThreadRng) -> HashMap<usize, (Disruption, usize)> {

    debug!("Generating disruptions...");
    let mut disruptions = HashMap::with_capacity(config.disruptions);
    for _ in 0..config.disruptions {

        // Disruptions start after the 48th hour, and can last 2-24 hours long
        let (start, length) = (rng.gen_range(48, config.hours - 24), rng.gen_range(2, 24));
        let disruption = match rng.gen_range(1,4) {
            // Node disruption: all (metric,queries) on the node are disrupted
            1 => {
                let id = rng.gen_range(0, config.nodes);
                debug!("Node Disruption: {}-{} [{:?}]", start, start+length, id);
                Disruption::Node(id)
            },
            // Query disruption: all (metric, node) with the query(ies) are disrupted
            2 => {
                // random number of queries from 1 to one-tenth of the total queries
                let num_queries = rng.gen_range(1,config.queries/10);
                let mut v: Vec<usize> = (0..num_queries).map(|_| rng.gen_range(0, config.queries)).collect();
                v.sort();
                v.dedup();
                debug!("Query Disruption: {}-{} [{:?}]", start, start+length, v);
                Disruption::Query(v)
            },
            // Metric disruption: all (node,query) with the metric are disrupted
            _ => {
                // one-to-all metrics disrupted
                let num_metrics = rng.gen_range(1,config.metrics);
                let mut v: Vec<usize> = (0..num_metrics).map(|_| rng.gen_range(0, config.metrics)).collect();
                v.sort();
                v.dedup();
                debug!("Metric Disruption: {}-{} [{:?}]", start, start+length, v);
                Disruption::Metric(v)
            }
        };
        disruptions.insert(start, (disruption, length));
    }

    disruptions
}

// Generate the distributions for each (node, query, metric) tuple
fn generate_distributions(config: &Config, rng: &mut ThreadRng)
                            -> HashMap<(usize, usize, usize), (NormalParams, NormalParams)> {

    debug!("generating distributions per (node,query,metric) tuple...");
    let mut distributions = HashMap::with_capacity(config.nodes * config.queries * config.metrics);
    for node in 0..config.nodes {
        for query in 0..config.queries {
            for metric in 0..config.metrics {

                // Each tuple gets a "regular" and "disrupted"
                // distribution from the allowed bounds
                let regular = NormalParams {
                    mean: rng.gen_range(config.regular_distribution.min_mean, config.regular_distribution.max_mean),
                    std: rng.gen_range(config.regular_distribution.min_std, config.regular_distribution.max_std)
                };

                let disrupted = NormalParams {
                    mean: rng.gen_range(config.disrupted_distribution.min_mean, config.disrupted_distribution.max_mean),
                    std: rng.gen_range(config.disrupted_distribution.min_std, config.disrupted_distribution.max_std)
                };

                distributions.insert((node, query, metric), (regular, disrupted));
            }
        }
    }

    distributions
}

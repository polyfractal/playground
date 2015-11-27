

use config::Config;
use hyper::Client;
use rustc_serialize::{Encodable, Encoder};
use rustc_serialize::json::{self};
use std::sync::Arc;
use std::io::Read;
use chrono::{Duration, UTC};
use chrono::offset::TimeZone;
use std::sync::atomic::Ordering;
use std::thread;
use hyper::status::StatusCode;

#[derive(RustcDecodable, Debug)]
pub struct Response {
    pub aggregations: AggsResponse
}

#[derive(RustcDecodable, Debug)]
pub struct AggsResponse {
    pub metrics: MetricsAgg
}

#[derive(RustcDecodable, Debug)]
pub struct MetricsAgg {
    pub buckets: Vec<MetricBucket>
}

#[derive(RustcDecodable, Debug)]
pub struct MetricBucket {
    pub key: usize,
    pub ninetieth_surprise: NinetiethValues
}

#[derive(RustcDecodable, Debug)]
pub struct DoubleValue {
    pub value: f64
}

#[derive(RustcDecodable, Debug)]
pub struct DoubleValues {
    pub values: Vec<f64>
}

#[derive(RustcDecodable, Debug)]
pub struct NinetiethValues {
    pub values: NinetiethValue
}

#[derive(Debug)]
pub struct NinetiethValue {
    pub value: f64
}

#[allow(unused_qualifications)]
impl ::rustc_serialize::Decodable for NinetiethValue {
    fn decode<__D: ::rustc_serialize::Decoder>(__arg_0: &mut __D) -> ::std::result::Result<NinetiethValue, __D::Error> {
        __arg_0.read_struct("NinetiethValue", 1usize, |_d| -> _ {
            ::std::result::Result::Ok(NinetiethValue{value:
               match _d.read_struct_field("90.0", 0usize, ::rustc_serialize::Decodable::decode)
               {
                   ::std::result::Result::Ok(__try_var) => __try_var,
                   ::std::result::Result::Err(__try_var) => return ::std::result::Result::Err(__try_var),
               }
           ,})
       })
   }
}

#[derive(RustcDecodable, Debug)]
pub struct MedianValues {
    pub values: MedianValue
}

#[derive(Debug)]
pub struct MedianValue {
    pub value: f64
}

#[allow(unused_qualifications)]
impl ::rustc_serialize::Decodable for MedianValue {
    fn decode<__D: ::rustc_serialize::Decoder>(__arg_0: &mut __D) -> ::std::result::Result<MedianValue, __D::Error> {
        __arg_0.read_struct("MedianValue", 1usize, |_d| -> _ {
            ::std::result::Result::Ok(MedianValue{value:
               match _d.read_struct_field("50.0", 0usize, ::rustc_serialize::Decodable::decode)
               {
                   ::std::result::Result::Ok(__try_var) => __try_var,
                   ::std::result::Result::Err(__try_var) => return ::std::result::Result::Err(__try_var),
               }
           ,})
       })
   }
}

#[derive(RustcDecodable, Debug)]
pub struct ExtendedStats {
    pub count: usize,
    pub min: f64,
    pub max: f64,
    pub avg: f64,
    pub sum: f64,
    pub sum_of_squares: f64,
    pub variance: f64,
    pub std_deviation: f64,
    pub std_deviation_bounds: StdDeviationBounds
}

#[derive(RustcDecodable, Debug)]
pub struct StdDeviationBounds {
    pub upper: f64,
    pub lower: f64
}

#[derive(RustcEncodable, RustcDecodable, Debug)]
pub struct HotCloudQuery {
    pub template: SearchTemplate,
    pub params: SearchParams
}

impl HotCloudQuery {
    pub fn new(hour: usize) -> HotCloudQuery {
        HotCloudQuery {
            template: SearchTemplate::new(),
            params: SearchParams::new(hour)
        }
    }
}

#[derive(RustcEncodable, RustcDecodable, Debug)]
pub struct SearchTemplate {
    pub id: String
}

impl SearchTemplate {
    pub fn new() -> SearchTemplate {
        SearchTemplate {
            id: "hotcloud".to_owned()
        }
    }
}

#[derive(RustcEncodable, RustcDecodable, Debug)]
pub struct SearchParams {
    pub start: String,
    pub end: String
}

impl SearchParams {
    pub fn new(hour: usize) -> SearchParams {
        let start = UTC.ymd(2015, 1, 1).and_hms(0, 0, 0) + Duration::hours(hour as i64) - Duration::days(1);
        let end = UTC.ymd(2015, 1, 1).and_hms(0, 0, 0) + Duration::hours(hour as i64);

        SearchParams {
            start: start.format("%Y-%m-%dT%H:%M:%S").to_string(),
            end: end.format("%Y-%m-%dT%H:%M:%S").to_string(),
        }
    }
}

#[derive(RustcDecodable, RustcEncodable)]
struct HotcloudResult {
    metric: usize,
    hour: String,
    value: f64
}

// Run the query portion of Atlas, to pre-cache the values (which simplifies the demo)
pub fn run_hotcloud(client: &Arc<Client>, config: Config) {
    let config = Arc::new(config);

    // We can query in parallel to speed up the process, divide the timeline
    // by number of threads
    let mut guards = Vec::with_capacity(config.threads);
    let batch_size: usize = config.hours / config.threads;

    for i in 0..config.threads {
        let start = batch_size * i;
        let end = start + batch_size;
        let config_clone = config.clone();
        let client_clone = client.clone();
        guards.push(thread::spawn(move ||{
            query_thread(start, end, client_clone, config_clone);
        }));
    }

    for guard in guards {
        let _ = guard.join();
    }

}

fn query_thread(start: usize, end: usize, client: Arc<Client>, config: Arc<Config>) {
    let mut bulk: Vec<HotcloudResult> = Vec::with_capacity(config.es.bulk_size);
    debug!("Running Hotcloud Queries ({} to {})...", start, end);

    let batch_size: usize = config.hours / config.threads;

    let mut c = 0;
    for hour in start..end {

        debug!("{}", hour);
        let body = HotCloudQuery::new(hour);
        let body = json::encode(&body).unwrap();

        // Execute a query using the pre-saved search template and extensive
        // filter_path filtering
        let mut hyper_response = client.post("http://localhost:9200/data/data/_search/template?filter_path=aggregations.**.ninetieth_surprise,aggregations.metrics.buckets.key")
              .body(&body)
              .send().unwrap();

        let mut body = String::new();
        let _ = match hyper_response.status {
            StatusCode::Ok => {hyper_response.read_to_string(&mut body)},
            _ => continue
        };

        let decoded: Response = json::decode(&body).unwrap();

        if decoded.aggregations.metrics.buckets.len() == 0 {
            continue;
        }

        // Extract the five 90th percentile surprise values and re-index into ES
        for metric in decoded.aggregations.metrics.buckets {
            bulk.push(HotcloudResult {
                metric: metric.key,
                hour: (UTC.ymd(2015, 1, 1).and_hms(0, 0, 0) + Duration::hours(hour as i64)).format("%Y-%m-%dT%H:%M:%S").to_string(),
                value: metric.ninetieth_surprise.values.value
            });
        }

        if bulk.len() >= 500 {
            debug!("{}%", (c as f32 / batch_size as f32)*100f32);

            while ::ACTIVE_THREADS.load(Ordering::SeqCst) >= config.threads {
                thread::sleep_ms(500);
            }
            debug!(".");
            ::util::send_bulk("http://localhost:9200/hotcloud/data/_bulk", &client, bulk);
            bulk = Vec::with_capacity(config.es.bulk_size);
        }

        c += 1;
    }

    ::util::send_bulk("http://localhost:9200/hotcloud/data/_bulk", &client, bulk);

    // manual refresh
    let _ = client.put("http://localhost:9200/hotcloud/_refresh").send();
}

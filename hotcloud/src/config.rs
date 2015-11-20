
use std::fs::File;
use std::io::prelude::*;
use toml::{Parser, Value};
use toml;

#[derive(RustcDecodable, Debug)]
pub struct ES {
    pub mapping: String,
    pub hotcloudmapping: String,
    pub query: String,
    pub bulk_size: usize
}

impl ES {
    fn new() -> ES {
        ES {
            mapping: String::new(),
            hotcloudmapping: String::new(),
            query: String::new(),
            bulk_size: 10000
        }
    }
}

#[derive(RustcDecodable, Debug)]
pub struct Distribution {
    pub min_mean: usize,
    pub max_mean: usize,
    pub min_std: usize,
    pub max_std: usize
}

pub enum DistributionType {
    Regular,
    Disrupted
}

impl Distribution {
    pub fn new(dist_type: DistributionType) -> Distribution {
        match dist_type {
            DistributionType::Regular => Distribution {
                min_mean: 20,
                max_mean: 40,
                min_std: 1,
                max_std: 10
            },
            DistributionType::Disrupted => Distribution {
                min_mean: 60,
                max_mean: 200,
                min_std: 20,
                max_std: 100
            }
        }
    }
}

#[derive(RustcDecodable, Debug)]
pub struct Config  {
    pub nodes: usize,
    pub queries: usize,
    pub metrics: usize,
    pub hours: usize,
    pub disruptions: usize,
    pub threads: usize,
    pub regular_distribution: Distribution,
    pub disrupted_distribution: Distribution,
    pub es: ES
}

impl Config {

    /// Returns a default configuration if we don't have/find a
    /// config file
    pub fn new() -> Config {
        Config {
            nodes: 10,
            queries: 10,
            metrics: 10,
            hours: 3000,
            disruptions: 50,
            threads: 2,
            regular_distribution: Distribution::new(DistributionType::Regular),
            disrupted_distribution: Distribution::new(DistributionType::Disrupted),
            es: ES::new()
        }
    }

    pub fn parse(path: String) -> Config {
        let mut config_toml = String::new();

        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(_)  => {
                error!("Could not find config file, using default!");
                return Config::new();
            }
        };

        file.read_to_string(&mut config_toml)
                .unwrap_or_else(|err| panic!("Error while reading config: [{}]", err));

        let mut parser = Parser::new(&config_toml);
        let toml = parser.parse();

        if toml.is_none() {
            for err in &parser.errors {
                let (loline, locol) = parser.to_linecol(err.lo);
                let (hiline, hicol) = parser.to_linecol(err.hi);
                println!("{}:{}:{}-{}:{} error: {}",
                         path, loline, locol, hiline, hicol, err.desc);
            }
            panic!("Exiting.");
        }

        let config = Value::Table(toml.unwrap());
        match toml::decode(config) {
            Some(t) => t,
            None => panic!("Error while deserializing config")
        }
    }
}

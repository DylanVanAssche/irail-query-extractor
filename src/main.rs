#[macro_use] extern crate error_chain;
extern crate reqwest;
extern crate flate2;
extern crate tar;
extern crate serde;
extern crate serde_json;
extern crate chrono;

use flate2::read::GzDecoder;
use tar::Archive;
use std::fs::{File, read_dir};
use std::io::{prelude::*, BufReader};
use chrono::{DateTime, Utc};

error_chain! {
    foreign_links {
        Io(std::io::Error);
        HttpRequest(reqwest::Error);
    }
}

#[derive(Debug)]
struct Query {
    arrival_stop: String,
    departure_stop: String,
    query_time: DateTime<Utc>,
    user_agent: String,
    query_type: String,
}

impl Route {
    fn transfers(&self) -> usize {
        self.connections.iter().count()
    }

    fn departure_stop(&self) -> &String {
        &self.connections.first().unwrap().departure_stop
    }

    fn arrival_stop(&self) -> &String {
        &self.connections.first().unwrap().arrival_stop
    }

    fn departure_time(&self) -> &DateTime<Utc> {
        &self.connections.first().unwrap().departure_time
    }

    fn arrival_time(&self) -> &DateTime<Utc> {
        &self.connections.first().unwrap().arrival_time
    }
}

#[derive(Debug)]
struct Journey {
    query: Query,
    routes: Vec<Route>,
}

#[derive(Debug)]
struct Route {
    connections: Vec<Connection>,
}

#[derive(Debug)]
struct Connection {
    departure_time: DateTime<Utc>,
    arrival_time: DateTime<Utc>,
    arrival_stop: String,
    departure_stop: String,
    vehicle: String,
}

const IRAIL_LOGS: &str = "https://gtfs.irail.be/logs/";
const IRAIL_VEHICLE: &str = "https://api.irail.be/vehicle?format=json&id=";
const ARCHIVE_DIR: &str = "./archive";

fn fetch_logs() -> Result<()> {
    // 30 days in November, :02 leading zero
    for day in 1..31 {
        let logfile = &format!("irailapi-201911{:02}.log.tar.gz", day)[..];
        println!("Log file: {}", logfile);

        // Get archive
        let res = reqwest::get(&format!("{}{}", IRAIL_LOGS, logfile)[..])?;
        println!("Status: {}", res.status());
        println!("Headers:\n{:#?}", res.headers());

        // Untar it
        let tar = GzDecoder::new(res);
        let mut archive = Archive::new(tar);
        archive.unpack(ARCHIVE_DIR)?;
    }
    Ok(())
}

fn add_vehicle_data(json: &serde_json::Value) -> Result<Journey> {
    let journeyoptions = &json["query"]["journeyoptions"];
    let mut id = 0;
    let mut routes: Vec<Route> = Vec::new();
    for option in journeyoptions.as_array() {
        for journey in option {
            let mut connections: Vec<Connection> = Vec::new();
            id = id + 1;
            for legs in journey["journeys"].as_array() {
                for leg in legs {
                    let departure_time = Utc::now();
                    let arrival_time = Utc::now();
                    let vehicle = leg["trip"].to_string().replace("\"", "");
                    
                    let mut res = reqwest::get(&format!("{}{}", IRAIL_VEHICLE, vehicle)[..])?;
                    println!("URL: {}{} STATUS: {}", IRAIL_VEHICLE, vehicle, res.status());
                    if res.status() != 200 {
                        return Err("Incomplete vehicle data".into());
                    }
                    let res: serde_json::Value = res.json()?;

                    let mut start = false;
                    for s in res["stops"]["stop"].as_array() {
                        for stop in s {
                            // Stop if we hit the transfer station or arrival station of this vehicle
                            if &stop["stationinfo"]["@id"].to_string() == &leg["arrivalStop"].to_string() {
                                println!("Found arrival station");
                                break;
                            }

                            // Start from departure station or transfer station of this vehicle
                            if &stop["stationinfo"]["@id"].to_string() == &leg["departureStop"].to_string() {
                                start = true;
                                println!("Found departure station");
                            }
                            
                            if start {
                                &connections.push(Connection {
                                    departure_time: departure_time,
                                    arrival_time: arrival_time,
                                    departure_stop: leg["departureStop"].to_string().replace("\"", ""),
                                    arrival_stop: leg["arrivalStop"].to_string().replace("\"", ""),
                                    vehicle: res["vehicle"].to_string(),
                                });       
                            }
                        }
                    }
                }
            }
            let route = Route {
                connections: connections,
            };
            routes.push(route);
        }
    }
    let query = Query {
        query_time: Utc::now(),
        query_type: json["querytype"].to_string(),
        user_agent: json["useragent"].to_string(),
        arrival_stop: json["arrivalStop"]["@id"].to_string(),
        departure_stop: json["departureStop"]["@id"].to_string(),
    };

    let journey = Journey {
        query: query,
        routes: routes,
    };

    Ok(journey)
}

fn extend_logs() -> Result<()> {
    for entry in read_dir(ARCHIVE_DIR)? {
        let entry = entry?;
        let path = entry.path();
        println!("Log file: {}", path.to_str().unwrap());
        let logfile = File::open(path)?;
        let reader = BufReader::new(logfile);

        for line in reader.lines() {
            let line = line?;
            let json: serde_json::Value = serde_json::from_str(&line[..]).expect("JSON formatting error!");

            match json["querytype"].to_string().as_str() {
                "\"connections\"" => {
                    match add_vehicle_data(&json) {
                        Ok(journey) => {
                            println!("{:#?}", journey);
                        }
                        _ => {
                            println!("Skipped query: {:#?}", &json);
                            continue;
                        }
                    }
                    /*    let journeys: Vec<Journey> = Vec::new();
                        

                        connections.push(Query {
                        arrival_stop: &json["arrivalStop"].to_string()[..],
                        departure_stop: &json["departureStop"].to_string()[..],
                        journeys: journeys});*/
                },
                _ => {},
            };
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    //fetch_logs().unwrap();
    println!("Unpacking OK, performing analytics...");

    extend_logs().unwrap();
    println!("Analytics complete!");
    Ok(())
}

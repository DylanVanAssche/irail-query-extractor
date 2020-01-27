#[macro_use] 
extern crate error_chain;

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

extern crate reqwest;
extern crate flate2;
extern crate tar;
extern crate chrono;

use flate2::read::GzDecoder;
use tar::Archive;
use std::fs::{File, read_dir};
use std::io::{prelude::*, BufReader};
use chrono::{DateTime, Utc, NaiveDateTime};

error_chain! {
    foreign_links {
        Io(std::io::Error);
        HttpRequest(reqwest::Error);
    }
}

#[derive(Debug, Serialize)]
struct Query {
    arrival_stop: String,
    departure_stop: String,
    #[serde(with = "my_date_format")]
    query_time: DateTime<Utc>,
    user_agent: String,
    query_type: String,
}

mod my_date_format {
    use chrono::{DateTime, Utc, TimeZone};
    use serde::{self, Deserialize, Serializer, Deserializer};

    const FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S.00Z";

    // The signature of a serialize_with function must follow the pattern:
    //
    //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
    //    where
    //        S: Serializer
    //
    // although it may also be generic over the input types T.
    pub fn serialize<S>(
        date: &DateTime<Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<'de, D>(D) -> Result<T, D::Error>
    //    where
    //        D: Deserializer<'de>
    //
    // although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Utc.datetime_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}

impl Route {
    fn departure_stop(&self) -> &String {
        &self._connections.first().unwrap().departure_stop
    }

    fn arrival_stop(&self) -> &String {
        &self._connections.first().unwrap().arrival_stop
    }

    fn departure_time(&self) -> &DateTime<Utc> {
        &self._connections.first().unwrap().departure_time
    }

    fn arrival_time(&self) -> &DateTime<Utc> {
        &self._connections.first().unwrap().arrival_time
    }

    fn number_of_connections(&self) -> usize {
        self._connections.iter().count()
    }

    fn number_of_transfers(&self) -> usize {
        self._transfers as usize
    }

    fn connection(&self) -> &Vec<Connection> {
        &self._connections
    }
}

impl Journey {
    fn average_number_of_transfers(&self) -> f32 {
        let average: f32 = self.routes.iter().fold(0, |accumulator, item| accumulator + item.number_of_transfers()) as f32 / self.routes.iter().count() as f32;
        average
    }
}

#[derive(Serialize, Debug)]
struct Journey {
    query: Query,
    routes: Vec<Route>,
}

#[derive(Serialize, Debug)]
struct Route {
    #[serde(rename = "connections")]
    _connections: Vec<Connection>,
    #[serde(rename = "transfers")]
    _transfers: u32,
}

#[derive(Serialize, Debug)]
struct Connection {
    #[serde(with = "my_date_format")]
    #[serde(rename = "departureTime")]
    departure_time: DateTime<Utc>,
    #[serde(with = "my_date_format")]
    #[serde(rename = "arrivalTime")]
    arrival_time: DateTime<Utc>,
    #[serde(rename = "arrivalStop")]
    arrival_stop: String,
    #[serde(rename = "departureStop")]
    departure_stop: String,
    #[serde(rename = "gtfs:vehicle")]
    vehicle: String,
}

const IRAIL_LOGS: &str = "https://gtfs.irail.be/logs/";
const IRAIL_VEHICLE: &str = "https://api.irail.be/vehicle?format=json&id=";
const VEHICLE_URI: &str = "http://irail.be/vehicle";
const ARCHIVE_DIR: &str = "./archive";
const OUTPUT_DIR: &str = "./output";

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
            let mut transfers: i32 = -1; // We count the number of vehicles, the first vehicle isn't a transfer and returns this to 0
            id = id + 1;
            for legs in journey["journeys"].as_array() {
                for leg in legs {
                    let vehicle = leg["trip"].to_string().replace("\"", "");
                    
                    let mut res = reqwest::get(&format!("{}{}", IRAIL_VEHICLE, vehicle)[..])?;
                    println!("URL: {}{} STATUS: {}", IRAIL_VEHICLE, vehicle, res.status());
                    if res.status() != 200 {
                        return Err("Incomplete vehicle data".into());
                    }
                    let res: serde_json::Value = res.json()?;

                    let mut start = false;
                    transfers = transfers + 1;
                    let mut previous_stop = String::new();
                    let mut previous_time = Utc::now();
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
                                previous_stop = stop["stationinfo"]["@id"].to_string().replace("\"", "");
                                let departure_time = NaiveDateTime::from_timestamp(stop["scheduledDepartureTime"].to_string().replace("\"", "").parse::<i64>().unwrap(), 0);
                                previous_time = DateTime::from_utc(departure_time, Utc);
                                continue;
                            }
                            
                            if start {
                                let arrival_time = NaiveDateTime::from_timestamp(stop["scheduledArrivalTime"].to_string().replace("\"", "").parse::<i64>().unwrap(), 0);
                                let arrival_time: DateTime<Utc> = DateTime::from_utc(arrival_time, Utc);

                                &connections.push(Connection {
                                    departure_time: previous_time,
                                    arrival_time: arrival_time,
                                    departure_stop: previous_stop, //stop["departureStop"].to_string().replace("\"", ""),
                                    arrival_stop: stop["stationinfo"]["@id"].to_string().replace("\"", ""),
                                    vehicle: format!("{}/{}", VEHICLE_URI, res["vehicle"].to_string().replace("\"", "").replace("BE.NMBS.", "")),
                                });       

                                // Departure stop next connection = current stop
                                previous_stop = stop["stationinfo"]["@id"].to_string().replace("\"", "");
                                let departure_time = NaiveDateTime::from_timestamp(stop["scheduledDepartureTime"].to_string().replace("\"", "").parse::<i64>().unwrap(), 0);
                                previous_time = DateTime::from_utc(departure_time, Utc);
                                continue;
                            }
                        }
                    }
                }
            }
            let route = Route {
                _connections: connections,
                _transfers: transfers as u32,
            };
            routes.push(route);
        }
    }

    let query_time: DateTime<Utc> = DateTime::parse_from_rfc3339(&json["querytime"].to_string().replace("\"", "")[..]).unwrap().with_timezone(&Utc);
    let query = Query {
        query_time: query_time,
        query_type: json["querytype"].to_string().replace("\"", ""),
        user_agent: json["user_agent"].to_string().replace("\"", ""),
        arrival_stop: json["query"]["arrivalStop"]["@id"].to_string().replace("\"", ""),
        departure_stop: json["query"]["departureStop"]["@id"].to_string().replace("\"", ""),
    };

    let journey = Journey {
        query: query,
        routes: routes,
    };

    println!("Journey fully extracted!");
    Ok(journey)
}

fn extend_logs() -> Result<()> {
    let mut journey_counter = 0;
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
                            journey_counter = journey_counter + 1;
                            let mut file = File::create(format!("{}/{}.json", OUTPUT_DIR, journey_counter))?;
                            let serialized = serde_json::to_string(&journey).unwrap();         
                            file.write_all(serialized.as_bytes()).expect("Writing journey to file failed!");
                            println!("Written journey {}", journey_counter);
                        }
                        _ => {
                            println!("Skipped query: {:#?}", &json);
                            continue;
                        }
                    }
                },
                _ => {},
            };
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    fetch_logs().unwrap();
    println!("Unpacking OK, performing analytics...");

    extend_logs().unwrap();
    println!("Analytics complete!");
    Ok(())
}

#[macro_use]
extern crate error_chain;
extern crate reqwest;
extern crate flate2;
extern crate tar;

use flate2::read::GzDecoder;
use tar::Archive;

error_chain! {
    foreign_links {
        Io(std::io::Error);
        HttpRequest(reqwest::Error);
    }
}

const IRAIL_LOGS: &str = "https://gtfs.irail.be/logs/";
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

fn perform_analytics() -> Result<()> {


    Ok(())
}

fn main() -> Result<()> {
    fetch_logs();
    println!("Unpacking OK, performing analytics...");

    perform_analytics();
    println!("Analytics complete!")
    Ok(())
}

use std::io::Write;

use chrono::Utc;
use env_logger::{Builder, Env, fmt::{Color, Formatter}};
use log::Level;
use log::Record;

pub fn init_log() {
    let mut builder = env_logger::Builder::new();

    builder.format(|buf, record| {
        writeln!(buf, "{} [{:4}] [{}:{}] {:>5}: {} ", Utc::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                 std::thread::current().name().or(Some("unknown")).unwrap(),
                 record.file().unwrap(),
                 record.line().unwrap(),
                 record.level(),
                 record.args())
    });
    builder.filter_level(log::LevelFilter::Debug);
    builder.init();


}
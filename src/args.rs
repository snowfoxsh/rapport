// src/args.rs

use clap::Parser;

/// Command-line arguments for the UDP proxy server
#[derive(Parser, Debug)]
#[command(name = "Mass UDP Proxy Server")]
#[command(author = "Patrick Unick <dev_storm@winux.com>")]
#[command(about = "A simple high-performance UDP proxy server", long_about = None)]
pub struct Cli {
    /// Path to the configuration file
    #[arg(
        short,
        long,
        value_name = "FILE",
        default_value = "/etc/massudpproxy.toml"
    )]
    pub config_file: String,

    #[arg(long, value_name = "UNLIMITED_RESOURCE", default_value_t = true)]
    pub unlimited_resource: bool,

    /// Number of Tokio worker threads
    #[arg(short, long, value_name = "THREADS", default_value_t = 4)]
    pub threads: usize,
}

use crate::dns::HostSocket;
use crate::port_range::PortRange;
use serde_derive::Deserialize;
use std::net::{IpAddr, SocketAddr};
use tokio::io;

fn default_connection_timeout() -> u32 { 30 }
fn default_log_level() -> String { "info".to_string() }
fn default_socket_buffer_size() -> usize { 4096 }
fn default_pool_count() -> usize { 256 }
fn default_pool_buffer_size() -> usize { 4096 }

#[derive(Deserialize, Debug)]
pub struct BufferConfig {
    #[serde(default = "default_socket_buffer_size")]
    pub socket_buffer_size: usize,
    #[serde(default)]
    pub use_pool: bool,
    #[serde(default = "default_pool_count")]
    pub pool_count: usize,
    #[serde(default = "default_pool_buffer_size")]
    pub pool_buffer_size: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            socket_buffer_size: default_socket_buffer_size(),
            use_pool: false,
            pool_count: default_pool_count(),
            pool_buffer_size: default_pool_buffer_size(),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct Config {
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout: u32,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub buffers: BufferConfig,
    pub routes: Vec<RouteConfig>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum RouteConfig {
    SinglePort {
        local: SocketAddr,
        remote: SocketAddr,
    },
    ManyPorts {
        local_addr: IpAddr,
        remote_addr: IpAddr,
        ports: Vec<u16>,
    },
    ManyComplexPorts {
        local_addr: IpAddr,
        remote_addr: IpAddr,
        local_ports: Vec<u16>,
        remote_ports: Vec<u16>,
    },
    SimpleRange {
        local_addr: IpAddr,
        remote_addr: IpAddr,
        port_range: PortRange,
    },
    ComplexPortRange {
        local_addr: IpAddr,
        remote_addr: IpAddr,
        local_port_range: PortRange,
        remote_port_range: PortRange,
    },
}

impl Config {
    pub async fn load_file(path: String) -> io::Result<Config> {
        let content = tokio::fs::read_to_string(path).await?;
        let config: Config =
            toml::from_str(&content).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(config)
    }

    pub fn to_listen_routes(&self) -> Vec<(SocketAddr, HostSocket)> {
        let mut pairs = Vec::new();
        for route in &self.routes {
            match route {
                RouteConfig::SinglePort { local, remote } => {
                    pairs.push((*local, HostSocket::from_socketaddr(*remote)));
                }
                RouteConfig::ManyPorts { local_addr, remote_addr, ports } => {
                    for &port in ports {
                        pairs.push((
                            SocketAddr::new(*local_addr, port),
                            HostSocket::from_socketaddr(SocketAddr::new(*remote_addr, port)),
                        ));
                    }
                }
                RouteConfig::ManyComplexPorts { local_addr, remote_addr, local_ports, remote_ports } => {
                    for (&lp, &rp) in local_ports.iter().zip(remote_ports.iter()) {
                        pairs.push((
                            SocketAddr::new(*local_addr, lp),
                            HostSocket::from_socketaddr(SocketAddr::new(*remote_addr, rp)),
                        ));
                    }
                }
                RouteConfig::SimpleRange { local_addr, remote_addr, port_range } => {
                    for port in port_range.clone() {
                        pairs.push((
                            SocketAddr::new(*local_addr, port),
                            HostSocket::from_socketaddr(SocketAddr::new(*remote_addr, port)),
                        ));
                    }
                }
                RouteConfig::ComplexPortRange { local_addr, remote_addr, local_port_range, remote_port_range } => {
                    for (lp, rp) in local_port_range.clone().zip(remote_port_range.clone()) {
                        pairs.push((
                            SocketAddr::new(*local_addr, lp),
                            HostSocket::from_socketaddr(SocketAddr::new(*remote_addr, rp)),
                        ));
                    }
                }
            }
        }
        pairs
    }
}

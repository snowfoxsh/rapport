use crate::dns::HostSocket;
use crate::port_range::PortRange;
use crate::router::Router;
use serde_derive::Deserialize;
use std::net::{IpAddr, SocketAddr};
use tokio::io;
use tokio::net::ToSocketAddrs;

const fn default_buffer_pool_size() -> usize {
    10_000
}

#[derive(Deserialize, Debug)]
pub struct Config {
    #[serde(default = "default_buffer_pool_size")]
    pub buffer_pool_permits: usize,
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

    pub fn router(&self) -> Router {
        let mut router = Router::new();

        for route in &self.routes {
            match route {
                RouteConfig::SinglePort { local, remote } => {
                    router.add_route(*local, *remote);
                }
                RouteConfig::ManyPorts {
                    local_addr,
                    remote_addr,
                    ports,
                } => {
                    for &port in ports {
                        router.add_route(
                            SocketAddr::new(*local_addr, port),
                            SocketAddr::new(*remote_addr, port),
                        );
                    }
                }
                RouteConfig::ManyComplexPorts {
                    local_addr,
                    remote_addr,
                    local_ports,
                    remote_ports,
                } => {
                    assert_eq!(
                        local_ports.len(),
                        remote_ports.len(),
                        "cannot have an unequal number of local and remote ports"
                    );

                    for (&local_port, &remote_port) in local_ports.iter().zip(remote_ports.iter()) {
                        router.add_route(
                            SocketAddr::new(*local_addr, local_port),
                            SocketAddr::new(*remote_addr, remote_port),
                        );
                    }
                }
                RouteConfig::SimpleRange {
                    local_addr: local,
                    remote_addr: remote,
                    port_range,
                } => router.add_direct_routes(*local, *remote, port_range.clone()),
                RouteConfig::ComplexPortRange {
                    local_addr: local,
                    remote_addr: remote,
                    local_port_range,
                    remote_port_range,
                } => {
                    router.add_offset_routes(*local, local_port_range.clone(), *remote, remote_port_range.clone())
                        .expect(format!("local port range {local_port_range:?} must be the same length as remote port range {remote_port_range:?}").as_str());
                }
                
            }
        }

        router
    }
}

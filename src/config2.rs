use std::net::SocketAddr;
use std::time::Duration;
use hickory_resolver::name_server::{GenericNameServer, NameServer};
use nestify::nest;
use serde_derive::Deserialize;

// #[derive(Deserialize, Debug)]
// pub struct Config2 {
//     dns: Option<DnsConfig>,
//     udp: Vec<UdpRoute>,
// }
//
// #[derive(Deserialize, Debug)]
// pub struct DnsConfig {
//     nameservers: Vec<String>,
//     attempts: usize,
//     timeout: Duration,
// }
//
// struct UdpConfig {
//
// }
//
//
// #[derive(Deserialize, Debug)]
// pub struct UdpRoute {
//     os_socket_buffer_size: Option<usize>,
//     use_os_buffer_pool: Option<>
// }
//
// pub enum UdpRouteExt {
//
// }

const fn default_true() -> bool { true }
const fn default_false() -> bool { false }

const fn default_recv_socket_buffer_size() -> usize { 2048 }

nest! {
    #[derive(Deserialize, Debug)]*
    pub struct Config2 {
        // default, use the system default
        dns: Option<pub struct DnsConfig {
            // sometime maybe make this a type not a String
            nameservers: Vec<String>,
            attempts: usize,
            timeout: Duration,
        }>,
        udp: pub struct UdpConfig {
            default_socket_options: UdpSocketOptions,
            routes: Vec<pub struct UdpRoute {
                #[serde(flatten)]
                socket_options: struct UdpSocketOptions {
                    os_socket_buffer_size: Option<usize>,
                    #[serde(default="default_recv_socket_buffer_size")]
                    recv_scoket_buffer_size: usize,
                    #[serde(default="default_true")]
                    allocate_from_buffer_pool: bool,
                },
                route_options: #[serde(untagged)] enum UdpRouteOptions {
                    SinglePortSimple {
                        always_send_from_socket: Option<SocketAddr>,
                        local: SocketAddr, // dns enable
                        // remote: ,
                    },
                    ManyPorts {

                    },
                }
            }>,
        }
    }
}
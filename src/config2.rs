use std::net::SocketAddr;
use std::time::Duration;
use hickory_resolver::name_server::{GenericNameServer, NameServer};
use nestify::nest;
use serde_derive::Deserialize;
use tokio::runtime::Builder;
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
const fn default_thread_count() -> usize { 10 }

nest! {
    #[derive(Deserialize, Debug)]*
    pub struct Config2 {
        #[serde(default="default_thread_count")]
        threads: usize,
        buffers_in_pool: usize,
        // default, use the system default
        dns: Option<pub struct DnsConfig {
            // sometime maybe make this a type not a String
            nameservers: Vec<String>,
            attempts: usize,
            timeout: Duration,
        }>,
        udp: pub struct UdpConfig {
            #[serde(flatten)]
            default_socket_options: UdpSocketOptions,
            routes: Vec<pub struct UdpRoute {
                #[serde(flatten)]
                socket_options: struct UdpSocketOptions {
                    os_socket_buffer_size: Option<usize>,
                    #[serde(default="default_recv_socket_buffer_size")]
                    recv_scoket_buffer_size: usize,
                    #[serde(default="default_false")]
                    allocate_from_buffer_pool: bool,
                },
                #[serde(flatten)]
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
/*
# notes

lazy start the buffer pool
if you specify the buffer pool size, then it should start the buffer pool maybe?

add log level support. eventually clean up the logs to be as useful as possible.

integrate metrics. they should be behind a feature flag. eventually, a web dashboard would be very cool for metrics.

config should be "last definition wins"

when configuring with command line arguments, it makes sense to just specify routes one by one.
if a user needs something more complex than that, they should just make a config file.

command line arguments will override the config values, including routes.

config will need to map into single routes and config objects for the sockets

## dns
i think it is okay to just resolve dns whenever. this will respect time to live.
if the dns resolves to a non-address im not sure if it should error or warn or panic.
i think error might be the best but panic would be the most aggressive ofc.

i think that using a global dns object in a OnceLock to lazily create the dns would be the best option.
some servers will likely not use domains and they should not be subject to the performance cost of using it.

i will create these types
HostAddr (www.winux.com) -[resolve().await]-> IpAddr
IpAddr:Port -> IpAddr:Port
SocketAddr -> SocketAddr
HostSocket (www.winux.com:0001) -[resolve().await]-> SocketAddr

for the pool, when you run out of buffers, then add the created buffer to the pool.
every few seconds sweep through the pool to flush the excess buffers.
this would be a very "intelligent"
log (info): high watermark was 100 buffers
high watermark = how big is the pool when i do the sweep

change: should have a buffer pool by default

add minimum and maximum buffer amount
*/
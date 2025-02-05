mod args;
mod configure;
mod connection;
mod pool;
mod port_range;
mod router;
mod timer;
mod config2;
mod dns;

use std::fmt::Display;
use std::io::Error;
use std::marker::PhantomData;
use bytes::BytesMut;
use dashmap::DashMap;
// use log::{debug, info};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{lookup_host, UdpSocket};
use tokio::{io, task};

use crate::pool::get_connection_pool;
use connection::Connection;
use dashmap::mapref::one::Ref;
use futures::task::waker;
use hickory_resolver::AsyncResolver;
use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use lendpool::LendPool;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, span, trace, trace_span, warn, Instrument, Level, Span};
use tracing::field::debug;
use tracing::instrument::Instrumented;
use tracing_subscriber::util::SubscriberInitExt;
use crate::dns::{init_resolver, HostSocket};

struct Stream {
    /// shared reference to active connections
    /// Arc<T> because we need to share to timeout thread
    active: Arc<DashMap<SocketAddr, Arc<Connection>>>, // active connections to the socket
    
    /// static routing table
    router: StreamRouter, // C:x -> S:y
    
    /// Packets are sent to this socket, they are then routed to the appropriate socket based on the routing table
    socket: Arc<UdpSocket>, // todo: make this not Arc<T>
    
    socket_addr: SocketAddr,
    
    /// Store the span for logging
    _span: Span,
}

impl Stream {
    pub async fn bind(router: StreamRouter) -> io::Result<Stream> {
        // create the stream span
        let _span = span!(Level::DEBUG, "stream", listen_sock=%router.recv);
        
        let socket = UdpSocket::bind(router.recv).await?;
        let socket = Arc::new(socket);

        let active: Arc<DashMap<SocketAddr, _>> = Arc::new(DashMap::new());

        get_connection_pool().add(Arc::clone(&active)).await;

        Ok(Self {
            socket,
            socket_addr: router.recv,
            active,
            router,
            
            _span
        })
    }

    pub fn socket(&self) -> &UdpSocket {
        self.socket.as_ref()
    }

    pub async fn send(&self, sent_from: SocketAddr, bytes: &[u8]) -> io::Result<()> {
        // let span = span!(Level::TRACE, "sending packet", sending_from=%sent_from,);
        // let _enter = span.enter();
        trace!(sending_from=%sent_from, "sending packet");

        // lookup the correct route
        let Some(send_to) = self.router.solve_route(&sent_from) else {
            trace!(sent_from=%sent_from, "dropped packet");
            return Ok(())
        };
        
        let resolved_addr = send_to.socket().resolve().await.unwrap();

        // get a handle on the connection
        let connection: Arc<Connection> = if let Some(active) = self.active.get(&resolved_addr) {
            // the connection exists
            Arc::clone(active.value())
        } else {
            // a new connection must be made
            let connection = Connection::new(sent_from, (Arc::clone(&self.socket), self.socket_addr), resolved_addr).await?;

            self.active.insert(resolved_addr, connection.clone());

            trace!(
                send_socket=%connection.send_socket_addr(),
                recv_socket=%connection.recv_socket_addr(),
                origin_socket=%connection.origin_socket_addr(),
                "creating connection"
            );
            connection
        };

        // send the bytes to the server
        let sent_size = connection.send(bytes).await?;

        trace!(sent_size=sent_size, "sent packet");

        Ok(())
    }

    fn listen(self) -> Instrumented<JoinHandle<io::Result<()>>> {
        let base_span = self._span.clone();
        tokio::spawn(async move {
            debug!(socket = %self.router.recv, "started listening for incoming packets");

            let mut buf = BytesMut::with_capacity(SOCK_BUFFER_SIZE);

            loop {
                // create a span for this packet
                let packet_span = span!(
                    Level::TRACE,
                    "packet",
                    socket = %self.router.recv
                );

                // wrap the body in an async block and attach the span
                let result: io::Result<()> = async {
                    // wait until the socket is ready for reading
                    self.socket().readable().await?;

                    // attempt to receive data into the buffer.
                    let (length, from) = match self.socket().try_recv_buf_from(&mut buf) {
                        Ok((length, from)) => (length, from),
                        // if nothing is available, continue the loop.
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                        Err(e) => {
                            error!(error = ?e, "failed to receive packet from socket");
                            return Err(e);
                        }
                    };

                    trace!(from = %from, packet_length = length, "packet received successfully");

                    // process the received packet.
                    self.send(from, &buf[..length]).await?;
                    Ok(())
                }.instrument(packet_span).await;

                if let Err(e) = result {
                    error!(error = ?e, socket = %self.router.recv, "packet processing error; terminating listener");
                    return Err(e);
                }
            }
        }).instrument(base_span)
    }
}

struct StreamRouter {
    recv: SocketAddr,
    routes: DashMap<SocketAddr, HostSocket>,
    default: Option<HostSocket>,
}

impl StreamRouter {
    fn recv(addr: SocketAddr) -> Self {
        Self {
            default: None,
            recv: addr,
            routes: DashMap::new(),
        }
    }

    pub fn add_route(&mut self, from_addr: SocketAddr, to_addr: HostSocket) {
        // create the route
        self.routes.insert(from_addr, to_addr);
    }

    pub fn route(mut self, from_addr: SocketAddr, to_addr: HostSocket) -> Self {
        self.add_route(from_addr, to_addr);
        self
    }

    pub fn add_default(&mut self, to_addr: HostSocket) {
        self.default = Some(to_addr)
    }

    pub fn default(mut self, to_addr: HostSocket) -> Self{
        self.add_default(to_addr);
        self
    }

    pub fn solve_route(&self, addr: &SocketAddr) -> Option<SocketOrDefault> {
        // dns lookup should happen in the router
        
        // todo: feature reverse lookup maybe
        if let Some(route) =  self.routes.get(addr) {
            Some(SocketOrDefault::DashRef(route))
        } else {
            self.default.as_ref().map(|x: &HostSocket| SocketOrDefault::Ref(x))
        }
    }
}

// this type exists to get value at the last possible moment
enum SocketOrDefault<'a> {
    DashRef(Ref<'a, SocketAddr, HostSocket>),
    Ref(&'a HostSocket)
}

impl<'a> SocketOrDefault<'a> {
    fn socket(&'a self) -> &'a HostSocket {
        match self {
            SocketOrDefault::DashRef(r) => {r.value()}
            SocketOrDefault::Ref(r) => {r}
        }
    }
}

// struct StreamOptions {
//     socket_os_buffer_size: Option<usize>,
//     socket_buffer_size: usize,
//     allocate_from_pool: bool,
// }
// 
// struct ConnectionOptions {
//     
// }
pub const SOCK_BUFFER_SIZE: usize = 4096;

#[tokio::main]
async fn main() -> io::Result<()> {
    // start logging
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();
    
    // todo: add tokio thread count here
    info!(version=env!("CARGO_PKG_VERSION"), "rapport");
    info!("starting server");

    // init the things
    let _ = init_resolver(None);
    let _ = get_connection_pool();


    let router = StreamRouter::recv("127.0.0.1:5000".parse().unwrap()).route(
        "127.0.0.1:7000".parse().unwrap(),
        "127.0.0.1:6000".parse().unwrap(),
    );    
    
    let stream = Stream::bind(router).await?;
    
    stream.listen().await??;

    info!("server shutdown");
    Ok(())
}

#[tokio::test]
async fn test_dns() {
    let resolver = AsyncResolver::tokio_from_system_conf().unwrap();
    AsyncResolver::tokio(ResolverConfig::new(), ResolverOpts::default());
    
    // let host = Host::parse("www.winux.com").unwrap();
    let host = "www.winux.com";
    
    
    // let 
    // for _ in (0..5) {
    //     let resolv 
    // }
    
    
    let mut times = vec![];
    for _ in (0..10) {
        let resolve1 = Instant::now();
        let s = resolver.lookup_ip(host).await.unwrap();
        times.push(resolve1.elapsed())
    }
    println!("{times:?}");

    // let resolve1 = Instant::now();
    // let s = lookup_host(format!("{host}:3000")).await.unwrap().next().unwrap();
    // let resolve1 = resolve1.elapsed();
    
    // let resolve2 = Instant::now();
    let s = resolver.lookup_ip(host).await.unwrap().iter().next().unwrap();
    // let resolve2 = resolve2.elapsed();
    // let addr1 = host.

    println!("{host:?}->{s}");
    // println!("t1: {resolve1:?}, t2: {resolve2:?}");
}


// #[tokio::test]
// async fn test_reverse() {
//     let resolver = AsyncResolver::tokio_from_system_conf().unwrap();
//     AsyncResolver::tokio(ResolverConfig::new(), ResolverOpts::default());
// 
//     let time1  = Instant::now();
//     let s = "3.80.25.196".parse::<IpAddr>().unwrap().reverse_lookup(&resolver).await.unwrap();
//     let time1 = time1.elapsed();
// 
//     let time2 = Instant::now();
//     let s = "3.80.25.196".parse::<IpAddr>().unwrap().reverse_lookup(&resolver).await.unwrap();
//     let time2 = time2.elapsed();
//     
//     println!("{:?}", time1);
//     println!("{:?}", time2);
// }

mod args;
mod configure;
mod connection;
mod pool;
mod port_range;
mod router;
mod timer;
mod config2;
mod dns;

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::io;
use tokio::task::JoinHandle;
use tracing::{debug, debug_span, error, info, trace, Instrument, Span};
use tracing::instrument::Instrumented;
use clap::Parser;
use crate::args::Cli;
use crate::configure::Config;
use crate::pool::get_connection_pool;
use crate::dns::{init_resolver, HostSocket};
use connection::Connection;
use dashmap::mapref::one::Ref;

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
        let _span = debug_span!("stream", listen=%router.recv);

        let socket = UdpSocket::bind(router.recv).await?;
        info!(listen=%router.recv, "stream bound");
        let socket = Arc::new(socket);

        let active: Arc<DashMap<SocketAddr, _>> = Arc::new(DashMap::new());
        get_connection_pool().add(Arc::clone(&active)).await;

        Ok(Self {
            socket,
            socket_addr: router.recv,
            active,
            router,
            _span,
        })
    }

    pub fn socket(&self) -> &UdpSocket {
        self.socket.as_ref()
    }

    pub async fn send(&self, sent_from: SocketAddr, bytes: &[u8]) -> io::Result<()> {
        let Some(send_to) = self.router.solve_route(&sent_from) else {
            trace!(from=%sent_from, "no route, dropped");
            return Ok(());
        };

        let resolved_addr = send_to.socket().resolve().await.unwrap();

        let connection: Arc<Connection> = if let Some(active) = self.active.get(&resolved_addr) {
            Arc::clone(active.value())
        } else {
            let connection = Connection::new(
                sent_from,
                (Arc::clone(&self.socket), self.socket_addr),
                resolved_addr,
            ).await?;
            self.active.insert(resolved_addr, connection.clone());
            connection
        };

        let sent_size = connection.send(bytes).await?;
        trace!(from=%sent_from, to=%resolved_addr, bytes=sent_size, "forwarded to upstream");

        Ok(())
    }

    fn listen(self) -> Instrumented<JoinHandle<io::Result<()>>> {
        let base_span = self._span.clone();
        let stream = Arc::new(self);

        tokio::spawn(async move {
            debug!(listen=%stream.router.recv, "listening");
            let mut buf = BytesMut::with_capacity(SOCK_BUFFER_SIZE);

            loop {
                stream.socket().readable().await?;

                // Drain all packets currently in the OS buffer before yielding.
                loop {
                    match stream.socket().try_recv_buf_from(&mut buf) {
                        Ok((length, from)) => {
                            trace!(from=%from, bytes=length, "received");
                            let bytes = Bytes::copy_from_slice(&buf[..length]);
                            let stream = Arc::clone(&stream);
                            tokio::spawn(async move {
                                if let Err(e) = stream.send(from, &bytes).await {
                                    error!(error=%e, from=%from, "packet error");
                                }
                            });
                            buf.clear();
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(e) => {
                            error!(error=%e, listen=%stream.router.recv, "listener terminated");
                            return Err(e);
                        }
                    }
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!(version = env!("CARGO_PKG_VERSION"), "rapport");
    info!("starting server");

    let _ = init_resolver(None);
    let _ = get_connection_pool();

    let args = Cli::parse();

    let config = Config::load_file(args.config_file.clone()).await
        .map_err(|e| io::Error::new(e.kind(), format!("{}: {}", args.config_file, e)))?;

    let listen_routes = config.to_listen_routes();

    if listen_routes.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "no routes configured"));
    }

    let mut handles = Vec::with_capacity(listen_routes.len());
    for (listen_addr, forward_to) in listen_routes {
        let router = StreamRouter::recv(listen_addr).default(forward_to);
        let stream = Stream::bind(router).await?;
        handles.push(stream.listen());
    }

    info!(stream_count = handles.len(), "all streams bound, listening");

    for handle in handles {
        handle.await??;
    }

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

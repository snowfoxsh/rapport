mod args;
mod configure;
mod connection;
mod pool;
mod port_range;
mod router;
mod timer;
mod config2;
mod dns;

use std::marker::PhantomData;
use bytes::BytesMut;
use dashmap::DashMap;
use log::{debug, info};
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
use tokio::task::JoinHandle;

struct Stream {
    // Arc<T> because we need to share to timeout thread
    active: Arc<DashMap<SocketAddr, Arc<Connection>>>, // active connections to the socket
    router: StreamRouter, // C:x -> S:y
    socket: Arc<UdpSocket>,
}

impl Stream {
    pub async fn bind(router: StreamRouter) -> io::Result<Stream> {
        let socket = UdpSocket::bind(router.recv).await?;
        let socket = Arc::new(socket);

        let active = Arc::new(DashMap::new());

        get_connection_pool().add(active.clone()).await;

        Ok(Self {
            socket,
            active,
            router,
        })
    }

    pub fn socket(&self) -> &UdpSocket {
        self.socket.as_ref()
    }

    pub async fn send(&self, sent_from: SocketAddr, bytes: &[u8]) -> io::Result<()> {
        // lookup the correct route
        // if: no route
        // then: drop the packet
        // find connection
        // if: no connection
        // then: create connection
        // send from connection socket

        // lookup the correct route
        let Some(send_to) = self.router.solve_route(&sent_from) else {
            debug!("DROP; FROM {:?}", sent_from);
            return Ok(())
        };

        // get a handle on the connection
        let connection: Arc<Connection> = if let Some(active) = self.active.get(send_to.socket()) {
            active.value().clone()
        } else {
            let connection = Connection::new(sent_from, Arc::clone(&self.socket), *send_to.socket()).await?;
            let connection: Arc<Connection> = Arc::new(connection);

            self.active.insert(*send_to.socket(), connection.clone());
            connection
        };

        // send the bytes to the server
        let sent_size = connection.send(bytes).await?;
        debug!(
            "SENT; LOCATION: {:?}, LEN: {sent_size}",
            connection.recv_in()
        );

        Ok(())
    }

    fn listen(self) -> JoinHandle<io::Result<()>> {
        task::spawn(async move {
            debug!("LISTEN; {}", self.router.recv);
            let mut buf = BytesMut::with_capacity(SOCK_BUFFER_SIZE);

            loop {
                self.socket().readable().await?;

                // todo: cache buffer size to determine when to shrink
                let (length, from) = match self.socket().try_recv_buf_from(&mut buf) {
                    Ok((length, from)) => (length, from),
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(e),
                };

                debug!("RECV; FROM {:?}, LEN: {length}", from);

                self.send(from, &buf[..length]).await?;
            }
        })
    }
}

struct StreamRouter {
    recv: SocketAddr,
    routes: DashMap<SocketAddr, SocketAddr>,
    default: Option<SocketAddr>,
}

impl StreamRouter {
    fn recv(addr: SocketAddr) -> Self {
        Self {
            default: None,
            recv: addr.into(),
            routes: DashMap::new(),
        }
    }

    pub fn add_route(&mut self, from_addr: SocketAddr, to_addr: SocketAddr) {
        // create the route
        self.routes.insert(from_addr, to_addr);
    }

    pub fn route(mut self, from_addr: SocketAddr, to_addr: SocketAddr) -> Self {
        self.add_route(from_addr, to_addr);
        self
    }

    pub fn add_default(&mut self, to_addr: SocketAddr) {
        self.default = Some(to_addr)
    }

    pub fn default(mut self, to_addr: SocketAddr) -> Self{
        self.add_default(to_addr);
        self
    }

    pub fn solve_route(&self, addr: &SocketAddr) -> Option<SolvedSocket> {
        if let Some(route) =  self.routes.get(addr) {
            Some(SolvedSocket::DashRef(route))
        } else {
            self.default.as_ref().map(|x: &SocketAddr| SolvedSocket::Ref(x))
        }
    }
}

// this type exists to get value at the last possible moment
enum SolvedSocket<'a> {
    DashRef(Ref<'a, SocketAddr, SocketAddr>),
    Ref(&'a SocketAddr)
}

impl<'a> SolvedSocket<'a> {
    fn socket(&'a self) -> &'a SocketAddr {
        match self {
            SolvedSocket::DashRef(r) => {r.value()}
            SolvedSocket::Ref(r) => {r}
        }
    }
}

struct StreamOptions {
    socket_os_buffer_size: Option<usize>,
    socket_buffer_size: usize,
    allocate_from_pool: bool,
}

struct ConnectionOptions {
    
}
pub const SOCK_BUFFER_SIZE: usize = 4096;

#[tokio::main]
async fn main() -> io::Result<()> {
    // start logging
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .init();

    info!("SERVER STARTING");

    let router = StreamRouter::recv("127.0.0.1:5000".parse().unwrap()).route(
        "127.0.0.1:7000".parse().unwrap(),
        "127.0.0.1:6000".parse().unwrap(),
    );

    let stream = Stream::bind(router).await?;

    stream.listen().await??;

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

fn a(addr: impl ToSocketAddrs) {
    addr.to_socket_addrs();
}
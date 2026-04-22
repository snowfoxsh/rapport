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
use crate::pool::{get_connection_pool, init_buffer_pool, try_get_buffer_pool};
use crate::dns::{init_resolver, HostSocket};
use connection::Connection;
use dashmap::mapref::one::Ref;

struct Stream {
    active: Arc<DashMap<SocketAddr, Arc<Connection>>>,
    router: StreamRouter,
    socket: Arc<UdpSocket>,
    socket_addr: SocketAddr,
    socket_buffer_size: usize,
    connection_timeout: u32,
    _span: Span,
}

impl Stream {
    pub async fn bind(router: StreamRouter, socket_buffer_size: usize, connection_timeout: u32) -> io::Result<Stream> {
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
            socket_buffer_size,
            connection_timeout,
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
                self.socket_buffer_size,
                self.connection_timeout,
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
        let buffer_size = self.socket_buffer_size;
        let stream = Arc::new(self);

        tokio::spawn(async move {
            debug!(listen=%stream.router.recv, "listening");
            // Scratch buffer used when pool is disabled.
            let mut scratch = BytesMut::with_capacity(buffer_size);

            loop {
                stream.socket().readable().await?;

                loop {
                    if let Some(mut loan) = try_get_buffer_pool().and_then(|p| p.loan()) {
                        loan.clear();
                        match stream.socket().try_recv_buf_from(&mut *loan) {
                            Ok((length, from)) => {
                                trace!(from=%from, bytes=length, "received");
                                let stream = Arc::clone(&stream);
                                tokio::spawn(async move {
                                    if let Err(e) = stream.send(from, &loan[..length]).await {
                                        error!(error=%e, from=%from, "packet error");
                                    }
                                    // loan drops here → returns to pool
                                });
                            }
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                drop(loan);
                                break;
                            }
                            Err(e) => {
                                drop(loan);
                                error!(error=%e, listen=%stream.router.recv, "listener terminated");
                                return Err(e);
                            }
                        }
                    } else {
                        match stream.socket().try_recv_buf_from(&mut scratch) {
                            Ok((length, from)) => {
                                trace!(from=%from, bytes=length, "received");
                                let bytes = Bytes::copy_from_slice(&scratch[..length]);
                                let stream = Arc::clone(&stream);
                                tokio::spawn(async move {
                                    if let Err(e) = stream.send(from, &bytes).await {
                                        error!(error=%e, from=%from, "packet error");
                                    }
                                });
                                scratch.clear();
                            }
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(e) => {
                                error!(error=%e, listen=%stream.router.recv, "listener terminated");
                                return Err(e);
                            }
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
        if let Some(route) = self.routes.get(addr) {
            Some(SocketOrDefault::DashRef(route))
        } else {
            self.default.as_ref().map(|x: &HostSocket| SocketOrDefault::Ref(x))
        }
    }
}

enum SocketOrDefault<'a> {
    DashRef(Ref<'a, SocketAddr, HostSocket>),
    Ref(&'a HostSocket)
}

impl<'a> SocketOrDefault<'a> {
    fn socket(&'a self) -> &'a HostSocket {
        match self {
            SocketOrDefault::DashRef(r) => r.value(),
            SocketOrDefault::Ref(r) => r,
        }
    }
}

pub const SOCK_BUFFER_SIZE: usize = 4096;

fn main() -> io::Result<()> {
    let args = Cli::parse();

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads)
        .enable_all()
        .build()?
        .block_on(async_main(args))
}

async fn async_main(args: Cli) -> io::Result<()> {
    let config = Config::load_file(args.config_file.clone()).await
        .map_err(|e| io::Error::new(e.kind(), format!("{}: {}", args.config_file, e)))?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.log_level)),
        )
        .init();

    info!(version = env!("CARGO_PKG_VERSION"), "rapport");
    info!("starting server");

    let _ = init_resolver(None);
    let _ = get_connection_pool();

    if config.buffers.use_pool {
        init_buffer_pool(config.buffers.pool_count, config.buffers.pool_buffer_size);
        info!(
            count = config.buffers.pool_count,
            buffer_size = config.buffers.pool_buffer_size,
            "buffer pool initialized"
        );
    }

    let listen_routes = config.to_listen_routes();

    if listen_routes.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "no routes configured"));
    }

    let socket_buffer_size = config.buffers.socket_buffer_size;
    let connection_timeout = config.connection_timeout;

    let mut handles = Vec::with_capacity(listen_routes.len());
    for (listen_addr, forward_to) in listen_routes {
        let router = StreamRouter::recv(listen_addr).default(forward_to);
        let stream = Stream::bind(router, socket_buffer_size, connection_timeout).await?;
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

    let host = "www.winux.com";

    let mut times = vec![];
    for _ in (0..10) {
        let resolve1 = Instant::now();
        let s = resolver.lookup_ip(host).await.unwrap();
        times.push(resolve1.elapsed())
    }
    println!("{times:?}");

    let s = resolver.lookup_ip(host).await.unwrap().iter().next().unwrap();
    println!("{host:?}->{s}");
}

use rlimit::{increase_nofile_limit, setrlimit, Resource};

mod args;
mod configure;
mod port_range;
mod router;

use crate::args::Cli;
use crate::configure::Config;
use bytes::BytesMut;
use clap::Parser;
use dashmap::DashMap;
use futures::future::join_all;
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::mem::MaybeUninit;
use std::net::{AddrParseError, SocketAddr, UdpSocket as _DontUseUdpSocket};
use std::ops::Deref;
use std::sync::Arc;
use tokio::net::{ToSocketAddrs, UdpSocket};
use tokio::sync::Semaphore;
use tokio::{io, task};

use dashmap::mapref::one::Ref;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::task::JoinHandle;
use crate::router::Router;

/// bind to all required sockets concurrently
async fn bind_sockets(socket_addrs: HashSet<SocketAddr>) -> Vec<Arc<UdpSocket>> {
    let tasks: Vec<_> = socket_addrs
        .into_iter()
        .map(|addr| {
            let addr_clone = addr.clone();
            task::spawn(async move {
                debug!("attempting to bind to socket: {}", addr_clone);
                match UdpSocket::bind(addr_clone).await {
                    Ok(socket) => Ok(Arc::new(socket)),
                    Err(error) => Err((addr_clone, error)),
                }
            })
        })
        .collect();

    let results = join_all(tasks).await;
    let mut bound_sockets = Vec::new();

    for task_result in results {
        match task_result {
            Ok(Ok(socket)) => {
                bound_sockets.push(socket);
            }
            Ok(Err((addr, error))) => {
                error!("failed to bind to socket: {} > {}", addr, error);
            }
            Err(join_error) => {
                error!("bind task failed with error > {}", join_error);
            }
        }
    }

    bound_sockets
}

#[derive(Clone)]
struct Connection {
    send_socket: Arc<UdpSocket>,
    send_to: SocketAddr,

    recv_socket: Arc<UdpSocket>, // mark unused
    recv_in: SocketAddr,


    origin_addr: SocketAddr,
    recv_handle: Option<Arc<JoinHandle<io::Result<()>>>>
    // maybe store a ref to the buffer pool
}

// maybe will need a list of valid return addresses
impl Connection {
    async fn new(origin_addr: SocketAddr, recv_socket: Arc<UdpSocket>, send_to: SocketAddr) -> io::Result<Connection> {
        // get the address of the local socket
        // tiny bit of unnecessary overhead here
        let recv_in = recv_socket.local_addr()?;

        // todo: maybe specify a way in the config to send from particular socket
        // bind the output socket; we dont care where it comes from
        let send_socket = UdpSocket::bind("0.0.0.0:0").await?;
        debug!("BOUND TO SOCKET {:?}", send_socket.local_addr()?);

        let send_socket = Arc::new(send_socket);
        
        let mut connection = Connection {
            send_socket,
            send_to,

            recv_in,
            recv_socket,

            origin_addr,
            recv_handle: None,
        };
        
        debug!("INIT; SEND CONNECTION: {:?} -> {:?}", connection.send_to, connection.recv_in);
        // todo: add this handle to the error watcher to await
        // begin receiving
        connection.recv_handle = Some(Arc::new(connection.recv()));
        
        Ok(connection)
    }
   
    fn recv(&self) -> JoinHandle<io::Result<()>> {
        let connection = self.clone();
        
        task::spawn(async move {
            debug!("INIT; CONNECTION: {:?} -> {:?}", connection.send_to, connection.origin_addr);
            // todo: buf pool
            let mut buf = BytesMut::with_capacity(SOCK_BUFFER_SIZE);
            
            loop {
                connection.send_socket.readable().await?;
                
                let (length, from) = match connection.send_socket.try_recv_buf_from(&mut buf) {
                    Ok((length, from)) => (length, from),
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(e),
                };
                
                // drop the packet if it is not from the client
                if from != connection.send_to {
                    debug!("DROP; FROM {:?}", connection.send_to);
                    continue;
                }
                debug!("RECV; LOCATION {:?}", connection.send_to);
                
                let bytes = &buf[..length];
                let sent_size = connection.recv_socket.send_to(bytes, connection.origin_addr).await?;
                debug!("SENT; LOCATION: {:?}, LEN: {sent_size}", connection.recv_in);
                
                buf.resize(SOCK_BUFFER_SIZE, 0x0);
            }
        })
    }
    
    pub fn recv_handle(&self) -> Arc<JoinHandle<io::Result<()>>> {
        self.recv_handle.clone().unwrap()
    }

    async fn send(&self, bytes: &[u8]) -> io::Result<usize> {
        self.send_socket.send_to(bytes, self.send_to).await
    }
    
}

struct Stream {
    // Arc<T> because we need to share to timeout thread
    active: Arc<DashMap<SocketAddr, Arc<Connection>>>, // active connections to the socket
    // routes: DashMap<SocketAddr, SocketAddr>,           // C:x -> S:y
    router: StreamRouter,
    socket: Arc<UdpSocket>,
}

impl Stream {
    pub async fn bind(router: StreamRouter) -> io::Result<Stream> {
        let socket = UdpSocket::bind(router.recv).await?;
        let socket = Arc::new(socket);

        Ok(Self {
            socket,
            active: Arc::new(DashMap::new()),
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
            // drop the packet
            debug!("DROP; FROM {:?}", sent_from);
            return Ok(());
        };

        // get a handle on the connection
        let connection: Arc<Connection> = if let Some(active) = self.active.get(&send_to) {
            active.value().clone()
        } else {
            let connection = Connection::new(sent_from, Arc::clone(&self.socket), *send_to).await?;
            let connection: Arc<Connection> = Arc::new(connection);

            self.active.insert(*send_to, connection.clone());
            connection
        };

        // send the bytes to the server
        let sent_size = connection.send(bytes).await?;
        debug!("SENT; LOCATION: {:?}, LEN: {sent_size}", connection.recv_in);

        Ok(())
    }

    fn listen(self) -> JoinHandle<io::Result<()>> {
        
        task::spawn( async move {
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
}

impl StreamRouter {
    fn recv(addr: SocketAddr) -> Self {
        Self {
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

    pub fn solve_route(&self, addr: &SocketAddr) -> Option<Ref<'_, SocketAddr, SocketAddr>> {
        self.routes.get(addr)
    }
}

const SOCK_BUFFER_SIZE: usize = 4096;

#[tokio::main]
async fn main() -> io::Result<()> {
    // start logging
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .init();
    
    let router = StreamRouter::recv("127.0.0.1:5000".parse().unwrap())
        .route("127.0.0.1:7000".parse().unwrap(), "127.0.0.1:6000".parse().unwrap());
    
    let stream = Stream::bind(router).await?;
    
    stream.listen().await??;

    Ok(())
}
/*

 */

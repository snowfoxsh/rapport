mod args;
mod configure;
mod connection;
mod pool;
mod port_range;
mod router;
mod timer;

use bytes::BytesMut;
use dashmap::DashMap;
use log::{debug, info};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::{io, task};

use crate::pool::get_connection_pool;
use connection::Connection;
use dashmap::mapref::one::Ref;
use tokio::task::JoinHandle;

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

            // let connection_pool = get_connection_pool();
            // connection_pool.add_connection(connection.clone());

            self.active.insert(*send_to, connection.clone());
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

pub const SOCK_BUFFER_SIZE: usize = 4096;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // start logging
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .init();

    info!("SERVER STARTING");

    // let connection_pool = Arc::new(ConnectionPool::new());

    let router = StreamRouter::recv("127.0.0.1:5000".parse().unwrap()).route(
        "127.0.0.1:7000".parse().unwrap(),
        "127.0.0.1:6000".parse().unwrap(),
    );

    let stream = Stream::bind(router).await?;

    stream.listen().await??;

    Ok(())
}

use crate::pool::{get_connection_pool, ConnectionPool};
use crate::SOCK_BUFFER_SIZE;
use bytes::BytesMut;
use std::hash::{Hash, Hasher};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;
use tracing::{debug, debug_span, instrument, trace, warn, Span};
use tracing_futures::{Instrument, Instrumented};

#[derive(Clone, Debug)]
pub struct Connection {
    send_socket: Arc<UdpSocket>,
    send_to: SocketAddr,

    recv_socket: Arc<UdpSocket>,
    recv_in: SocketAddr,

    origin_addr: SocketAddr,

    recv_handle: Option<Arc<Instrumented<JoinHandle<io::Result<()>>>>>,

    last_used: Arc<AtomicU32>,
    connection_pool: Arc<ConnectionPool>,
    
    // _span: Span,
    pub _span: Span,
}

impl Connection {
    pub fn send_socket_addr(&self) -> SocketAddr { self.send_to }
    
    pub fn recv_socket_addr(&self) -> SocketAddr { self.recv_in }
    
    pub fn origin_socket_addr(&self) -> SocketAddr { self.origin_addr }
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.send_to == other.send_to
            && self.recv_in == other.recv_in
            && self.origin_addr == other.origin_addr
    }
}

impl Eq for Connection {}

impl Hash for Connection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.send_to.hash(state);
        self.recv_in.hash(state);
        self.origin_addr.hash(state);
    }
}

// maybe will need a list of valid return addresses
impl Connection {
    #[instrument("connection", skip(socket), fields(origin=%origin_addr, upstream=%send_to))]
    pub(crate) async fn new(
        origin_addr: SocketAddr,
        socket: (Arc<UdpSocket>, SocketAddr),
        send_to: SocketAddr,
    ) -> io::Result<Arc<Connection>> {
        let (recv_socket, recv_in) = socket;

        let send_socket = UdpSocket::bind("0.0.0.0:0").await?;
        trace!(send_socket=%send_socket.local_addr()?, "bound outbound socket");

        let send_socket = Arc::new(send_socket);
        let connection_pool = get_connection_pool();
        let last_used = Arc::new(AtomicU32::new(connection_pool.time()));

        let mut connection = Connection {
            send_socket,
            send_to,
            recv_in,
            recv_socket,
            origin_addr,
            recv_handle: None,
            last_used,
            connection_pool,
            _span: Span::current(),
        };

        let inst_recv_handle = connection.recv();

        if inst_recv_handle.inner().is_finished() {
            warn!(upstream=%connection.send_to, "recv task finished immediately after spawn");
        }

        connection.recv_handle = Some(Arc::new(inst_recv_handle));

        debug!(origin=%origin_addr, upstream=%send_to, "connection established");
        Ok(Arc::new(connection))
    }

    fn recv(&self) -> Instrumented<JoinHandle<io::Result<()>>> {
        let connection = self.clone();
        let span = debug_span!("recv", upstream=%connection.send_to, origin=%connection.origin_addr);
        tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(SOCK_BUFFER_SIZE);
            loop {
                connection.send_socket.readable().await?;

                let (length, from) = match connection.send_socket.try_recv_buf_from(&mut buf) {
                    Ok((length, from)) => (length, from),
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(e) => {
                        warn!(error=%e, "recv socket error");
                        return Err(e);
                    }
                };

                if from != connection.send_to {
                    trace!(from=%from, expected=%connection.send_to, "dropped packet from unexpected source");
                    continue;
                }

                connection.last_used.store(connection.connection_pool.time(), Ordering::Relaxed);

                trace!(from=%from, bytes=length, "received upstream packet");

                let sent_size = connection
                    .recv_socket
                    .send_to(&buf[..length], connection.origin_addr)
                    .await?;

                trace!(to=%connection.origin_addr, bytes=sent_size, "forwarded to origin");

                buf.resize(SOCK_BUFFER_SIZE, 0x0);
            }
        }).instrument(span)
    }

    pub(crate) async fn send(&self, bytes: &[u8]) -> io::Result<usize> {
        self.send_socket.send_to(bytes, self.send_to).await
    }

    pub(crate) fn recv_in(&self) -> SocketAddr {
        self.recv_in
    }

    pub(crate) fn timeout(&self) -> u32 {
        4
    }

    pub(crate) fn last_active(&self) -> u32 {
        self.last_used.load(Ordering::Relaxed)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(handle) = &self.recv_handle {
            let handle = handle.inner();
            if !handle.is_finished() {
                let _enter = self._span.enter();
                handle.abort();
                debug!(origin=%self.origin_addr, upstream=%self.send_to, "connection dropped");
            }
        }
    }
}



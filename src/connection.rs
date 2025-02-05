use crate::pool::{get_buffer_pool, get_connection_pool, ConnectionPool};
use crate::SOCK_BUFFER_SIZE;
use bytes::BytesMut;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::Error;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;
use tracing::{debug, trace, span, Level, Span, warn, debug_span, instrument};
use tracing::field::debug;
use tracing_futures::{Instrument, Instrumented};
use crate::dns::{get_resolver, HostSocket};

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
    #[instrument("connection", skip(socket))]
    pub(crate) async fn new(
        origin_addr: SocketAddr,
        socket: (Arc<UdpSocket>, SocketAddr),
        send_to: SocketAddr,
    ) -> io::Result<Arc<Connection>> {
        // let _span = span!(Level::INFO, "initiating connection");
        // let _enter_span = _span.clone();
        // let _enter = _enter_span.enter();
        
        let (recv_socket, recv_in) = socket;

        // todo: maybe specify a way in the config to send from particular socket
        // bind the output socket; we dont care where it comes from
        let send_socket = UdpSocket::bind("0.0.0.0:0").await?;
        
        // todo: ask larry about this sys call
        trace!(socket=%send_socket.local_addr()?, "connection sending data from socket");
        
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
            
            _span: Span::current()
        };
        
        // begin receiving
        // connection.recv_handle = Some(Arc::new(connection.recv()));
        let inst_recv_handle = connection.recv();
        let recv_handle = inst_recv_handle.inner();
        
        
        if recv_handle.is_finished() {
            warn!(
                a=%connection.send_to,
                b=%connection.recv_in,
                "failed to spawn connection recv task",
            );
        } else {
            debug!(
                a=%connection.send_to,
                b=%connection.recv_in,
                "successfully started connection recv task",
            );
        }

        connection.recv_handle = Some(Arc::new(inst_recv_handle));
        
        // todo: add this connection to the error watcher to await the future
        
        Ok(Arc::new(connection))
    }

    fn recv(&self) -> Instrumented<JoinHandle<io::Result<()>>> {
        let connection = self.clone();
        
        // let span = Span::current();
        let span = debug_span!("recv connection", outward_addr=?connection.send_to, interior_addr=?connection.origin_addr);
        tokio::spawn(async move {
            debug!("creating connection");
            
            // todo: buf pool
            let mut buf = BytesMut::with_capacity(SOCK_BUFFER_SIZE);
            loop {
                // todo: add span here
                connection.send_socket.readable().await?;

                let (length, from) = match connection.send_socket.try_recv_buf_from(&mut buf) {
                    Ok((length, from)) => (length, from),
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(e) => {
                        warn!(error=%e,"connection ended unexpectedly");
                        return Err(e)
                    },
                };
                
                // drop the packet if it is not from the client
                if from != connection.send_to {
                    debug!(from=%from,"dropping packet");
                    continue;
                }
                connection
                    .last_used
                    .store(connection.connection_pool.time(), Ordering::Relaxed);
                
                // debug!(
                //     "LAST USED; {} | {}",
                //     connection.last_used.load(Ordering::Relaxed),
                //     connection.connection_pool.time()
                // );

                // debug!("RECV; LOCATION {:?}", connection.send_to);
                trace!(
                    location=%connection.send_to, 
                    last_used_time=connection.last_used.load(Ordering::SeqCst), 
                    current_time=connection.connection_pool.time(),
                    "received packet"
                );
                let bytes = &buf[..length];
                let sent_size = connection
                    .recv_socket
                    .send_to(bytes, connection.origin_addr)
                    .await?;
                debug!("SENT; LOCATION: {:?}, LEN: {sent_size}", connection.recv_in);

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
    // kill the recv connection when the connection is dropped
    fn drop(&mut self) {
        // im not quite sure why drop sometimes gets called twice
        if let Some(handle) = &self.recv_handle {
            let _enter = self._span.enter();
            let handle = handle.inner();
            if !handle.is_finished() {
                handle.abort();
                debug!("connection dropped");
            }
        }
    }
}



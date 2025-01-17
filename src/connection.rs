use crate::pool::{get_connection_pool, ConnectionPool};
use crate::SOCK_BUFFER_SIZE;
use bytes::BytesMut;
use log::{debug, error};
use std::hash::{Hash, Hasher};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;
use crate::dns::{get_resolver, HostSocket};

#[derive(Clone)]
pub struct Connection {
    send_socket: Arc<UdpSocket>,
    send_to: SocketAddr,

    recv_socket: Arc<UdpSocket>,
    recv_in: SocketAddr,

    origin_addr: SocketAddr,

    recv_handle: Option<Arc<JoinHandle<io::Result<()>>>>,

    last_used: Arc<AtomicU32>,
    connection_pool: Arc<ConnectionPool>,
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
    pub(crate) async fn new(
        origin_addr: SocketAddr,
        recv_socket: Arc<UdpSocket>,
        send_to: SocketAddr,
    ) -> io::Result<Connection> {
        // get the address of the local socket
        // tiny bit of unnecessary overhead here
        let recv_in = recv_socket.local_addr()?;

        // todo: maybe specify a way in the config to send from particular socket
        // bind the output socket; we dont care where it comes from
        let send_socket = UdpSocket::bind("0.0.0.0:0").await?;
        debug!("BOUND TO SOCKET {:?}", send_socket.local_addr()?);

        let send_socket = Arc::new(send_socket);
        let connection_pool = get_connection_pool();
        let last_used = Arc::new(AtomicU32::new(connection_pool.time()));
        
        // when we create the connection resolve the ip that the connection sends to
        // let send_to = send_to.resolve_socket(&get_resolver()).await
        //     .expect("TODO: if the domain name is invalid this wont work");

        let mut connection = Connection {
            send_socket,
            send_to,

            recv_in,
            recv_socket,

            origin_addr,
            recv_handle: None,

            last_used,
            connection_pool,
        };

        debug!(
            "INIT; SEND CONNECTION: {:?} -> {:?}",
            connection.send_to, connection.recv_in
        );
        // todo: add this handle to the error watcher to await
        // begin receiving
        connection.recv_handle = Some(Arc::new(connection.recv()));

        Ok(connection)
    }

    fn recv(&self) -> JoinHandle<io::Result<()>> {
        let connection = self.clone();

        tokio::spawn(async move {
            debug!(
                "INIT; CONNECTION: {:?} -> {:?}",
                connection.send_to, connection.origin_addr
            );
            
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
                connection
                    .last_used
                    .store(connection.connection_pool.time(), Ordering::Relaxed);
                debug!(
                    "LAST USED; {} | {}",
                    connection.last_used.load(Ordering::Relaxed),
                    connection.connection_pool.time()
                );

                debug!("RECV; LOCATION {:?}", connection.send_to);
                let bytes = &buf[..length];
                let sent_size = connection
                    .recv_socket
                    .send_to(bytes, connection.origin_addr)
                    .await?;
                debug!("SENT; LOCATION: {:?}, LEN: {sent_size}", connection.recv_in);

                buf.resize(SOCK_BUFFER_SIZE, 0x0);
            }
        })
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
        // im not quite sure why drop sometimes gets called twice
        if let Some(handle) = &self.recv_handle {
            if !handle.is_finished() {
                handle.abort();
                debug!("CONNECTION DROPPED");
            }
        }
    }
}

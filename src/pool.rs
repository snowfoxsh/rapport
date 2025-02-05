use crate::connection::Connection;
use crate::timer::Timer;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use bytes::BytesMut;
use lendpool::LendPool;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{debug, span, Level};
use tracing_futures::Instrument;
use crate::dns::HostSocket;

static CONNECTION_POOL: OnceLock<Arc<ConnectionPool>> = OnceLock::new();

pub(crate) fn get_connection_pool() -> Arc<ConnectionPool> {
    CONNECTION_POOL.get_or_init(ConnectionPool::new).clone()
}

#[derive(Debug)]
pub struct ConnectionPool {
    active: Mutex<Vec<Arc<DashMap<SocketAddr, Arc<Connection>>>>>,
    timer: Timer,
}

impl ConnectionPool {
    pub(crate) fn new() -> Arc<Self> {
        let pool = Arc::new(Self {
            active: Mutex::new(vec![]),
            timer: Timer::start(),
        });

        let pool_ref = Arc::clone(&pool);

        let span = span!(Level::DEBUG, "started connection pool");
        tokio::spawn(async move {
            // let span = span!(Level::DEBUG, "started connection pool");
            // let _enter = span.enter();
            
            let timer = &pool_ref.timer;
            let active = &pool_ref.active;

            let mut interval = time::interval(Duration::from_secs(1));
            loop {
                // clean up about every second
                interval.tick().await;

                // start the cleanup
                // debug!("STARTING CLEANUP;");
                let time = timer.time();

                // we lock for a long time we only add in the beginning so it is okay
                let mut active_connections = 0;
                active.lock().await.iter().for_each(|sub_pool| {
                    sub_pool.retain(|_, con| {
                        (time - con.last_active() <= con.timeout())
                            .then(|| active_connections += 1)
                            .is_some()
                    })
                });

                debug!(
                    active_connections=active_connections, "finished cleanup"
                )
                // tracing::debug!(
                //     "FINISHED CLEANUP; ACTIVE CONNECTIONS: {}",
                //     active_connections
                // );
            }
        }).instrument(span);

        pool
    }

    #[inline(always)]
    pub fn time(&self) -> u32 {
        self.timer.time()
    }

    pub async fn add(&self, sub_pool: Arc<DashMap<SocketAddr, Arc<Connection>>>) {
        self.active.lock().await.push(sub_pool)
    }
}

static BUFFER_POOL: OnceLock<LendPool<BytesMut>> = OnceLock::new();


pub struct BufferPoolConfig {
    buffer_count: usize,
    initial_buffer_size: usize,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            buffer_count: 100,
            initial_buffer_size: 1024
        }
    }
}

pub fn init_buffer_pool<'a>(config: BufferPoolConfig) -> &'a LendPool<BytesMut> {
    assert!(BUFFER_POOL.get().is_none(), "BUFFER_POOL has already been initialized");
    
    let init_with = || {
        let pool = LendPool::new();

        for _ in 0..config.buffer_count {
            pool.add(BytesMut::with_capacity(config.initial_buffer_size))
        }
        
        pool
    };

    BUFFER_POOL.get_or_init(init_with)
}

pub fn get_buffer_pool<'a>() -> &'a LendPool<BytesMut> {
    BUFFER_POOL.get_or_init(|| {
        panic!("BUFFER_POOL not initialized, create it with pool::init_buffer_pool")
    })
}


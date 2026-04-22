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
use tracing::{debug_span, trace};
use tracing_futures::Instrument;

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

        let span = debug_span!("connection_pool");
        tokio::spawn(async move {
            let timer = &pool_ref.timer;
            let active = &pool_ref.active;

            let mut interval = time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;

                let time = timer.time();
                let mut active_connections = 0;
                active.lock().await.iter().for_each(|sub_pool| {
                    sub_pool.retain(|_, con| {
                        (time - con.last_active() <= con.timeout())
                            .then(|| active_connections += 1)
                            .is_some()
                    })
                });

                trace!(active_connections, "pool sweep");
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

pub fn init_buffer_pool(count: usize, buffer_size: usize) {
    assert!(BUFFER_POOL.get().is_none(), "BUFFER_POOL already initialized");
    BUFFER_POOL.get_or_init(|| {
        let pool = LendPool::new();
        for _ in 0..count {
            pool.add(BytesMut::with_capacity(buffer_size));
        }
        pool
    });
}

pub fn try_get_buffer_pool() -> Option<&'static LendPool<BytesMut>> {
    BUFFER_POOL.get()
}

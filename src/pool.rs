use std::io;
use log::{debug, error, info};
use std::sync::{Arc, OnceLock};
use std::collections::HashSet;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use futures::future::join_all;
use dashmap::{DashMap, DashSet};
use tokio::time;
use std::time::Duration;
use tokio::time::Instant;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use crate::{Connection, Stream, StreamRouter};
use crate::timer::Timer;



static CONNECTION_POOL: OnceLock<Arc<ConnectionPool>> = OnceLock::new();

pub(crate) fn get_connection_pool() -> Arc<ConnectionPool> {
    CONNECTION_POOL.get_or_init(ConnectionPool::new).clone()
}

pub struct ConnectionPool {
    active: Mutex<Vec<Arc<DashMap<SocketAddr, Arc<Connection>>>>>,
    // active: DashSet<Arc<Connection>>,
    timer: Timer
}

impl ConnectionPool {
    pub(crate) fn new() -> Arc<Self> {
        let pool = Arc::new(Self {
            active: Mutex::new(vec![]),
            timer: Timer::start()
        });

        let pool_ref = Arc::clone(&pool);
        
        tokio::spawn(async move {
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
                active.lock().await.iter().for_each(|sub_pool|
                    sub_pool.retain(|_, con| {
                        (time - con.last_active() <= con.timeout())
                            // .then(|| con.terminate())
                            // .is_some()
                            .then(|| active_connections += 1)
                            .is_some()
                    })
                );

                debug!("FINISHED CLEANUP; ACTIVE CONNECTIONS: {}", active_connections);
            }
        });

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

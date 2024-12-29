use std::io;
use log::{debug, error, info};
use std::sync::{Arc, OnceLock};
use std::collections::HashSet;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use futures::future::join_all;
use dashmap::DashSet;
use tokio::time;
use std::time::Duration;
use tokio::time::Instant;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::task::JoinHandle;
use crate::{Connection, Stream, StreamRouter};
use crate::timer::Timer;



static CONNECTION_POOL: OnceLock<Arc<ConnectionPool>> = OnceLock::new();

pub(crate) fn get_connection_pool() -> Arc<ConnectionPool> {
    CONNECTION_POOL.get_or_init(ConnectionPool::new).clone()
}

pub struct ConnectionPool {
    active: DashSet<Arc<Connection>>,
    timer: Timer
}

impl ConnectionPool {
    pub(crate) fn new() -> Arc<Self> {
        // put the AtomicU32 in an Arc
        let pool = Arc::new(Self {
            active: DashSet::new(),
            timer: Timer::start()
        });

        let pool_ref = Arc::clone(&pool);

        // spawn local here
        tokio::spawn(async move {
            let timer = &pool_ref.timer;
            let active   = &pool_ref.active;

            let mut interval = time::interval(Duration::from_secs(1));

            loop {
                // clean up about every second
                interval.tick().await;

                // start the cleanup

                // calling len here is fine since with no debug it will not run
                debug!("STARTING CLEANUP; ACTIVE CONNECTIONS: {}", active.len());

                let time = timer.time();

                let mut shutdown_count = 0;
                pool_ref.active.iter().for_each(|connection| {
                    // if the connection has been dead for longer than the timeout
                    active.remove_if(connection.key(), |con| { time - con.last_active() > con.timeout() })
                        .and_then(|c| {shutdown_count += 1; Some(c)});
                });

                debug!("FINISHED CLEANUP; SHUTDOWN CONNECTIONS: {}", shutdown_count);
            }

        });

        pool
    }
    
    #[inline(always)]
    pub fn time(&self) -> u32 {
        self.timer.time()
    }
    
    pub fn add_connection(&self, connection: Arc<Connection>) -> bool {
        self.active.insert(connection)
    }
}

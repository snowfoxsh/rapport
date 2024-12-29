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
        let outer_map: DashMap<String, DashMap<String, i32>> = DashMap::new();

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
                let mut to_remove = vec![];
                pool_ref.active.iter().for_each(|connection| {
                    // if the connection has been dead for longer than the timeout
                    // active.remove_if(connection.key(), |con| { !time - con.last_active() > con.timeout() })
                    //     .and_then(|c| {shutdown_count += 1; Some(c)});
                    // debug!("LAST ACTIVE: {}", connection.last_active())

                    // kill the connection if it has not been used recently
                    if time - connection.last_active() > connection.timeout() {
                        // first stop the recv task
                        connection.recv_handle.as_ref().unwrap().abort();
                        
                        // then remove it from the parent active pool
                        connection.parent_active_pool.remove(&connection.send_to);
                        
                        // we have to remove it later because of deadlock i think
                        to_remove.push(connection.key().clone());

                        shutdown_count += 1;
                    }
                });

                for connection in to_remove {
                    active.remove(&connection);
                }


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

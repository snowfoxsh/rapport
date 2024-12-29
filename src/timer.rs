use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::time;
use std::time::Duration;
use tokio::time::Instant;
use tokio::task::JoinHandle;

pub struct Timer {
    start: Instant,
    elapsed: Arc<AtomicU32>,
    timer_handle: Option<JoinHandle<()>>
}

impl Timer {
    pub fn start() -> Self {
        // put the AtomicU32 in an Arc
        let elapsed = Arc::new(AtomicU32::new(0));

        // clone the Arc so we can move it into the async block
        let elapsed_clone = Arc::clone(&elapsed);

        // spawn the timer task
        let timer_handle = Some(tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                // update the AtomicU32 every second
                elapsed_clone.fetch_add(1, Ordering::Relaxed);
            }
        }));

        Self {
            elapsed,
            start: Instant::now(),
            timer_handle,
        }
    }

    #[inline(always)]
    pub fn time(&self) -> u32 {
        self.elapsed.load(Ordering::Relaxed)
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(handle) = self.timer_handle.take() {
            handle.abort();
        }
    }
}
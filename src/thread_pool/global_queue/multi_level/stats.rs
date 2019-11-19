// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crossbeam_epoch::{self, Atomic};
use dashmap::DashMap;
use futures_timer::Delay;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const CLEANUP_INTERVAL: Duration = Duration::from_secs(20);

#[derive(Clone, Default)]
pub struct TaskElapsed(Arc<AtomicU64>);

impl TaskElapsed {
    pub fn get(&self) -> Duration {
        Duration::from_micros(self.0.load(Ordering::SeqCst))
    }

    pub fn inc_by(&self, t: Duration) {
        self.0.fetch_add(t.as_micros() as u64, Ordering::SeqCst);
    }
}

#[derive(Clone)]
pub struct TaskElapsedMap {
    new: Atomic<DashMap<u64, TaskElapsed>>,
    old: Atomic<DashMap<u64, TaskElapsed>>,
}

impl TaskElapsedMap {
    pub fn new() -> Self {
        let new = Atomic::new(DashMap::default());
        let old = Atomic::new(DashMap::default());
        TaskElapsedMap { new, old }
    }

    pub fn get_elapsed(&self, key: u64) -> TaskElapsed {
        unsafe {
            let guard = &crossbeam_epoch::pin();
            let new = self.new.load(Ordering::SeqCst, guard);
            if let Some(v) = new.deref().get(&key) {
                return v.clone();
            }
            let old = self.old.load(Ordering::SeqCst, guard);
            if let Some((_, v)) = old.deref().remove(&key) {
                new.deref().insert(key, v.clone());
                return v;
            }
            let stats = new.deref().get_or_insert(&key, TaskElapsed::default());
            stats.clone()
        }
    }

    /// Creates a future which continuously cleans stale statistics.
    ///
    /// It needs tokio-timer to have been initialized.
    pub fn async_cleanup(&self) -> impl Future<Output = ()> {
        let new = self.new.clone();
        let old = self.old.clone();
        async move {
            loop {
                Delay::new(CLEANUP_INTERVAL).await;
                let guard = &crossbeam_epoch::pin();
                let new_ptr = new.load(Ordering::SeqCst, guard);
                let old_ptr = old.swap(new_ptr, Ordering::SeqCst, guard);
                unsafe {
                    old_ptr.deref().clear();
                }
                new.store(old_ptr, Ordering::SeqCst);
            }
        }
    }
}

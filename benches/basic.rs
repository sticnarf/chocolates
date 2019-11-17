#![deny(warnings, rust_2018_idioms)]

use criterion::*;

const NUM_SPAWN: usize = 10_000;
const NUM_YIELD: usize = 1_000;
const TASKS_PER_CPU: usize = 50;

mod tokio_threadpool {
    use futures::{future, task, Async};
    use num_cpus;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::{mpsc, Arc};
    use tokio_threadpool::*;

    pub fn spawn_many(b: &mut criterion::Bencher) {
        let threadpool = ThreadPool::new();

        let (tx, rx) = mpsc::sync_channel(10);
        let rem = Arc::new(AtomicUsize::new(0));

        b.iter(move || {
            rem.store(super::NUM_SPAWN, SeqCst);

            for _ in 0..super::NUM_SPAWN {
                let tx = tx.clone();
                let rem = rem.clone();

                threadpool.spawn(future::lazy(move || {
                    if 1 == rem.fetch_sub(1, SeqCst) {
                        tx.send(()).unwrap();
                    }

                    Ok(())
                }));
            }

            let _ = rx.recv().unwrap();
        });
    }

    pub fn yield_many(b: &mut criterion::Bencher) {
        let threadpool = ThreadPool::new();
        let tasks = super::TASKS_PER_CPU * num_cpus::get();

        let (tx, rx) = mpsc::sync_channel(tasks);

        b.iter(move || {
            for _ in 0..tasks {
                let mut rem = super::NUM_YIELD;
                let tx = tx.clone();

                threadpool.spawn(future::poll_fn(move || {
                    rem -= 1;

                    if rem == 0 {
                        tx.send(()).unwrap();
                        Ok(Async::Ready(()))
                    } else {
                        // Notify the current task
                        task::current().notify();

                        // Not ready
                        Ok(Async::NotReady)
                    }
                }));
            }

            for _ in 0..tasks {
                let _ = rx.recv().unwrap();
            }
        });
    }
}

// In this case, CPU pool completes the benchmark faster, but this is due to how
// CpuPool currently behaves, starving other futures. This completes the
// benchmark quickly but results in poor runtime characteristics for a thread
// pool.
//
// See rust-lang-nursery/futures-rs#617
//
mod cpupool {
    use futures::future::{self, Executor};
    use futures::{task, Async};
    use futures_cpupool::*;
    use num_cpus;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::{mpsc, Arc};

    pub fn spawn_many(b: &mut criterion::Bencher) {
        let pool = CpuPool::new(num_cpus::get());

        let (tx, rx) = mpsc::sync_channel(10);
        let rem = Arc::new(AtomicUsize::new(0));

        b.iter(move || {
            rem.store(super::NUM_SPAWN, SeqCst);

            for _ in 0..super::NUM_SPAWN {
                let tx = tx.clone();
                let rem = rem.clone();

                pool.execute(future::lazy(move || {
                    if 1 == rem.fetch_sub(1, SeqCst) {
                        tx.send(()).unwrap();
                    }

                    Ok(())
                }))
                .ok()
                .unwrap();
            }

            let _ = rx.recv().unwrap();
        });
    }

    pub fn yield_many(b: &mut criterion::Bencher) {
        let pool = CpuPool::new(num_cpus::get());
        let tasks = super::TASKS_PER_CPU * num_cpus::get();

        let (tx, rx) = mpsc::sync_channel(tasks);

        b.iter(move || {
            for _ in 0..tasks {
                let mut rem = super::NUM_YIELD;
                let tx = tx.clone();

                pool.execute(future::poll_fn(move || {
                    rem -= 1;

                    if rem == 0 {
                        tx.send(()).unwrap();
                        Ok(Async::Ready(()))
                    } else {
                        // Notify the current task
                        task::current().notify();

                        // Not ready
                        Ok(Async::NotReady)
                    }
                }))
                .ok()
                .unwrap();
            }

            for _ in 0..tasks {
                let _ = rx.recv().unwrap();
            }
        });
    }
}

mod thread_pool_callback {
    use chocolates::thread_pool::callback::{Handle, RunnerFactory, TypeErasedTask};
    use chocolates::thread_pool::{Config, TaskProvider};
    use num_cpus;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::{mpsc, Arc};

    pub fn spawn_many(b: &mut criterion::Bencher) {
        let pool = Config::new("test-pool")
            .spawn(RunnerFactory::new(), || crossbeam_deque::Injector::new());

        let (tx, rx) = mpsc::sync_channel(10);
        let rem = Arc::new(AtomicUsize::new(0));

        b.iter(move || {
            rem.store(super::NUM_SPAWN, SeqCst);

            for _ in 0..super::NUM_SPAWN {
                let tx = tx.clone();
                let rem = rem.clone();

                pool.spawn_once(move |_| {
                    if 1 == rem.fetch_sub(1, SeqCst) {
                        tx.send(()).unwrap();
                    }
                });
            }

            let _ = rx.recv().unwrap();
        });
    }

    pub fn yield_many(b: &mut criterion::Bencher) {
        let pool = Config::new("yield many")
            .spawn(RunnerFactory::new(), || crossbeam_deque::Injector::new());
        let tasks = super::TASKS_PER_CPU * num_cpus::get();

        let (tx, rx) = mpsc::sync_channel(tasks);

        b.iter(move || {
            for _ in 0..tasks {
                let mut rem = super::NUM_YIELD;
                let tx = tx.clone();

                fn sub_rem<P>(c: &mut Handle<'_, P>, rem: &mut usize, tx: &mpsc::SyncSender<()>)
                where
                    P: TaskProvider<Task = TypeErasedTask, RawTask = TypeErasedTask>,
                {
                    *rem -= 1;
                    if *rem == 0 {
                        tx.send(()).unwrap();
                    } else {
                        c.rerun();
                    }
                }

                pool.spawn_mut(move |c| sub_rem(c, &mut rem, &tx))
            }

            for _ in 0..tasks {
                let _ = rx.recv().unwrap();
            }
        });
    }
}

mod thread_pool_future {
    use chocolates::thread_pool::future::RunnerFactory;
    use chocolates::thread_pool::Config;
    use futures::{future, task, Async};
    use num_cpus;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::{mpsc, Arc};

    pub fn spawn_many(b: &mut criterion::Bencher) {
        let threadpool = Config::new("test-pool")
            .spawn(RunnerFactory::new(4), || crossbeam_deque::Injector::new());

        let (tx, rx) = mpsc::sync_channel(10);
        let rem = Arc::new(AtomicUsize::new(0));

        b.iter(move || {
            rem.store(super::NUM_SPAWN, SeqCst);

            for _ in 0..super::NUM_SPAWN {
                let tx = tx.clone();
                let rem = rem.clone();

                threadpool.spawn_future(future::lazy(move || {
                    if 1 == rem.fetch_sub(1, SeqCst) {
                        tx.send(()).unwrap();
                    }

                    Ok(())
                }));
            }

            let _ = rx.recv().unwrap();
        });
    }

    pub fn yield_many(b: &mut criterion::Bencher) {
        let threadpool = Config::new("test-pool")
            .spawn(RunnerFactory::new(4), || crossbeam_deque::Injector::new());
        let tasks = super::TASKS_PER_CPU * num_cpus::get();

        let (tx, rx) = mpsc::sync_channel(tasks);

        b.iter(move || {
            for _ in 0..tasks {
                let mut rem = super::NUM_YIELD;
                let tx = tx.clone();

                threadpool.spawn_future(future::poll_fn(move || {
                    rem -= 1;

                    if rem == 0 {
                        tx.send(()).unwrap();
                        Ok(Async::Ready(()))
                    } else {
                        // Notify the current task
                        task::current().notify();

                        // Not ready
                        Ok(Async::NotReady)
                    }
                }));
            }

            for _ in 0..tasks {
                let _ = rx.recv().unwrap();
            }
        });
    }
}

fn spawn_many(b: &mut Criterion) {
    b.bench(
        "spawn_many",
        ParameterizedBenchmark::new(
            "thread_pool_future",
            |b, _| thread_pool_future::spawn_many(b),
            &[()],
        )
        .with_function("thread_pool_callback", |b, _| {
            thread_pool_callback::spawn_many(b)
        })
        .with_function("cpupool", |b, _| cpupool::spawn_many(b))
        .with_function("tokio_threadpool", |b, _| tokio_threadpool::spawn_many(b)),
    );
}

fn yield_many(b: &mut Criterion) {
    b.bench(
        "yield_many",
        ParameterizedBenchmark::new(
            "thread_pool_future",
            |b, _| thread_pool_future::yield_many(b),
            &[()],
        )
        .with_function("thread_pool_callback", |b, _| {
            thread_pool_callback::yield_many(b)
        })
        .with_function("cpupool", |b, _| cpupool::yield_many(b))
        .with_function("tokio_threadpool", |b, _| tokio_threadpool::yield_many(b)),
    );
}

criterion_group!(benches, spawn_many, yield_many);

criterion_main!(benches);

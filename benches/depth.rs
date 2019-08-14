#![deny(warnings, rust_2018_idioms)]

use criterion::*;

const ITER: usize = 20_000;

mod tokio_threadpool {
    use criterion::Bencher;
    use futures::future;
    use std::sync::mpsc;
    use tokio_threadpool::*;

    pub fn chained_spawn(b: &mut Bencher) {
        let threadpool = ThreadPool::new();

        fn spawn(pool_tx: Sender, res_tx: mpsc::Sender<()>, n: usize) {
            if n == 0 {
                res_tx.send(()).unwrap();
            } else {
                let pool_tx2 = pool_tx.clone();
                pool_tx
                    .spawn(future::lazy(move || {
                        spawn(pool_tx2, res_tx, n - 1);
                        Ok(())
                    }))
                    .unwrap();
            }
        }

        b.iter(move || {
            let (res_tx, res_rx) = mpsc::channel();

            spawn(threadpool.sender().clone(), res_tx, super::ITER);
            res_rx.recv().unwrap();
        });
    }
}

mod cpupool {
    use criterion::Bencher;
    use futures::future::{self, Executor};
    use futures_cpupool::*;
    use num_cpus;
    use std::sync::mpsc;

    pub fn chained_spawn(b: &mut Bencher) {
        let pool = CpuPool::new(num_cpus::get());

        fn spawn(pool: CpuPool, res_tx: mpsc::Sender<()>, n: usize) {
            if n == 0 {
                res_tx.send(()).unwrap();
            } else {
                let pool2 = pool.clone();
                pool.execute(future::lazy(move || {
                    spawn(pool2, res_tx, n - 1);
                    Ok(())
                }))
                .ok()
                .unwrap();
            }
        }

        b.iter(move || {
            let (res_tx, res_rx) = mpsc::channel();

            spawn(pool.clone(), res_tx, super::ITER);
            res_rx.recv().unwrap();
        });
    }
}

mod thread_pool_callback {
    use chocolates::thread_pool::callback::{Handle, RunnerFactory};
    use chocolates::thread_pool::Config;
    use criterion::Bencher;
    use std::sync::mpsc;

    pub fn chained_spawn(b: &mut Bencher) {
        let pool = Config::new("chain-spawn").spawn(RunnerFactory::new());

        fn spawn(c: &mut Handle<'_>, res_tx: mpsc::Sender<()>, n: usize) {
            if n == 0 {
                res_tx.send(()).unwrap();
            } else {
                c.spawn_once(move |c| {
                    spawn(c, res_tx, n - 1);
                })
            }
        }

        b.iter(move || {
            let (res_tx, res_rx) = mpsc::channel();
            pool.spawn_once(move |c| spawn(c, res_tx, super::ITER));
            res_rx.recv().unwrap();
        });
    }
}

mod thread_pool_future {
    use chocolates::thread_pool::future::{RunnerFactory, Sender};
    use chocolates::thread_pool::Config;
    use criterion::Bencher;
    use futures::future;
    use std::sync::mpsc;

    pub fn chained_spawn(b: &mut Bencher) {
        let threadpool = Config::new("chained_spawn")
            .max_idle_time(std::time::Duration::from_secs(3))
            .spawn(RunnerFactory::new(4));

        fn spawn(pool_tx: Sender, res_tx: mpsc::Sender<()>, n: usize) {
            if n == 0 {
                res_tx.send(()).unwrap();
            } else {
                let pool_tx2 = pool_tx.clone();
                pool_tx.spawn(future::lazy(move || {
                    spawn(pool_tx2, res_tx, n - 1);
                    Ok(())
                }));
            }
        }

        b.iter(move || {
            let (res_tx, res_rx) = mpsc::channel();

            spawn(threadpool.sender(), res_tx, super::ITER);
            res_rx.recv().unwrap();
        });
    }

}

fn chained_spawn(b: &mut Criterion) {
    b.bench(
        "chained_spawn",
        ParameterizedBenchmark::new(
            "thread_pool_future",
            |b, _| thread_pool_future::chained_spawn(b),
            &[()],
        )
        .with_function("thread_pool_callback", |b, _| {
            thread_pool_callback::chained_spawn(b)
        })
        .with_function("cpupool", |b, _| cpupool::chained_spawn(b))
        .with_function("tokio_threadpool", |b, _| {
            tokio_threadpool::chained_spawn(b)
        }),
    );
}

criterion_group!(benches, chained_spawn);

criterion_main!(benches);

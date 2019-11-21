use super::{Config, GlobalQueue, PoolContext, SchedUnit};
use crate::thread_pool::global_queue::multi_level;

use crossbeam_deque::Steal;
use std::cell::UnsafeCell;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering::SeqCst};
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::{fmt, mem, ptr};

pub struct Runner<G>
where
    G: GlobalQueue,
{
    max_inplace_spin: usize,
    _phantom: PhantomData<G>,
}

pub struct RunnerFactory<G>
where
    G: GlobalQueue,
{
    max_inplace_spin: usize,
    _phantom: PhantomData<G>,
}

impl<G> RunnerFactory<G>
where
    G: GlobalQueue,
{
    pub fn new(max_inplace_spin: usize) -> Self {
        RunnerFactory {
            max_inplace_spin,
            _phantom: PhantomData,
        }
    }
}

impl<G> Default for RunnerFactory<G>
where
    G: GlobalQueue,
{
    fn default() -> Self {
        Self {
            max_inplace_spin: 4,
            _phantom: PhantomData,
        }
    }
}

impl<G, T> super::RunnerFactory for RunnerFactory<G>
where
    G: GlobalQueue<Task = Arc<T>> + Send + Sync + 'static,
    T: FutureTask<G>,
{
    type Runner = Runner<G>;

    fn produce(&mut self) -> Runner<G> {
        Runner {
            max_inplace_spin: self.max_inplace_spin,
            _phantom: PhantomData,
        }
    }
}

// #[derive(Clone, Debug)]
// #[repr(transparent)]
// struct Task(Arc<AtomicFuture>);

pub struct AtomicFuture<G: GlobalQueue> {
    sender: super::Remote<G>,
    status: AtomicU8,
    future: UnsafeCell<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
}

impl<G: GlobalQueue> fmt::Debug for AtomicFuture<G> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "AtomicFuture".fmt(f)
    }
}

unsafe impl<G: GlobalQueue + Send + Sync> Send for AtomicFuture<G> {}
unsafe impl<G: GlobalQueue + Sync + Sync> Sync for AtomicFuture<G> {}

const WAITING: u8 = 0; // --> POLLING
const POLLING: u8 = 1; // --> WAITING, REPOLL, or COMPLETE
const REPOLL: u8 = 2; // --> POLLING
const COMPLETE: u8 = 3; // No transitions out

impl<G: GlobalQueue> AtomicFuture<G> {
    fn new<F: Future<Output = ()> + Send + 'static>(future: F, sender: super::Remote<G>) -> Self {
        AtomicFuture {
            sender,
            status: AtomicU8::new(WAITING),
            future: UnsafeCell::new(Box::pin(future)),
        }
    }
}

// impl Task {
//     #[inline]
//     fn new<F: Future<Output = ()> + Send + 'static>(future: F) -> Task {
//         let future: Arc<AtomicFuture> = Arc::new(AtomicFuture {
//             status: AtomicU8::new(WAITING),
//             future: UnsafeCell::new(future.boxed()),
//         });
//         let future: *const AtomicFuture = Arc::into_raw(future) as *const AtomicFuture;
//         unsafe { task(future) }
//     }

//     #[inline]
//     fn new_boxed(future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> Task {
//         let future: Arc<AtomicFuture> = Arc::new(AtomicFuture {
//             status: AtomicU8::new(WAITING),
//             future: UnsafeCell::new(future),
//         });
//         let future: *const AtomicFuture = Arc::into_raw(future) as *const AtomicFuture;
//         unsafe { task(future) }
//     }

//     #[inline]
//     unsafe fn poll(self) {
//         self.0.status.store(POLLING, SeqCst);
//         let waker = ManuallyDrop::new(waker(&*self.0));
//         let mut cx = Context::from_waker(&waker);
//         loop {
//             if let Poll::Ready(_) = (&mut *self.0.future.get()).poll_unpin(&mut cx) {
//                 break self.0.status.store(COMPLETE, SeqCst);
//             }
//             match self
//                 .0
//                 .status
//                 .compare_exchange(POLLING, WAITING, SeqCst, SeqCst)
//             {
//                 Ok(_) => break,
//                 Err(_) => self.0.status.store(POLLING, SeqCst),
//             }
//         }
//     }
// }

#[inline]
unsafe fn waker<G, T>(task: *const T) -> Waker
where
    G: GlobalQueue<Task = Arc<T>> + Send + Sync + 'static,
    T: FutureTask<G>,
{
    Waker::from_raw(RawWaker::new(
        task as *const (),
        &RawWakerVTable::new(
            clone_raw::<G, T>,
            wake_raw::<G, T>,
            wake_ref_raw::<G, T>,
            drop_raw::<G, T>,
        ),
    ))
}

#[inline]
unsafe fn clone_raw<G, T>(this: *const ()) -> RawWaker
where
    G: GlobalQueue<Task = Arc<T>> + Send + Sync + 'static,
    T: FutureTask<G>,
{
    let task = clone_task(this as *const T);
    RawWaker::new(
        Arc::into_raw(task) as *const (),
        &RawWakerVTable::new(
            clone_raw::<G, T>,
            wake_raw::<G, T>,
            wake_ref_raw::<G, T>,
            drop_raw::<G, T>,
        ),
    )
}

#[inline]
unsafe fn drop_raw<G, T>(this: *const ())
where
    G: GlobalQueue<Task = Arc<T>> + Send + Sync + 'static,
    T: FutureTask<G>,
{
    drop(task(this as *const T))
}

#[inline]
unsafe fn wake_raw<G, T>(this: *const ())
where
    G: GlobalQueue<Task = Arc<T>> + Send + Sync + 'static,
    T: FutureTask<G>,
{
    let task = task(this as *const T);
    let atomic_future = task.as_atomic_future();
    let mut status = atomic_future.status.load(SeqCst);
    loop {
        match status {
            WAITING => {
                match atomic_future
                    .status
                    .compare_exchange(WAITING, POLLING, SeqCst, SeqCst)
                {
                    Ok(_) => {
                        atomic_future.sender.spawn(clone_task(&*task));
                        break;
                    }
                    Err(cur) => status = cur,
                }
            }
            POLLING => {
                match atomic_future
                    .status
                    .compare_exchange(POLLING, REPOLL, SeqCst, SeqCst)
                {
                    Ok(_) => break,
                    Err(cur) => status = cur,
                }
            }
            _ => break,
        }
    }
}

#[inline]
unsafe fn wake_ref_raw<G, T>(this: *const ())
where
    G: GlobalQueue<Task = Arc<T>> + Send + Sync + 'static,
    T: FutureTask<G>,
{
    let task = ManuallyDrop::new(task(this as *const T));
    let atomic_future = task.as_atomic_future();
    let mut status = atomic_future.status.load(SeqCst);
    loop {
        match status {
            WAITING => {
                match atomic_future
                    .status
                    .compare_exchange(WAITING, POLLING, SeqCst, SeqCst)
                {
                    Ok(_) => {
                        atomic_future.sender.spawn(clone_task(&**task));
                        break;
                    }
                    Err(cur) => status = cur,
                }
            }
            POLLING => {
                match atomic_future
                    .status
                    .compare_exchange(POLLING, REPOLL, SeqCst, SeqCst)
                {
                    Ok(_) => break,
                    Err(cur) => status = cur,
                }
            }
            _ => break,
        }
    }
}

#[inline]
unsafe fn task<G, T>(future: *const T) -> Arc<T>
where
    G: GlobalQueue<Task = Arc<T>> + Send + Sync + 'static,
    T: FutureTask<G>,
{
    Arc::from_raw(future)
}

#[inline]
unsafe fn clone_task<G, T>(future: *const T) -> Arc<T>
where
    G: GlobalQueue<Task = Arc<T>> + Send + Sync + 'static,
    T: FutureTask<G>,
{
    let task = task(future);
    std::mem::forget(task.clone());
    task
}

pub trait FutureTask<G: GlobalQueue>: Send + Sync {
    fn as_atomic_future(&self) -> &AtomicFuture<G>;
}

impl<G: GlobalQueue + Send + Sync> FutureTask<G> for AtomicFuture<G> {
    fn as_atomic_future(&self) -> &AtomicFuture<G> {
        self
    }
}

impl<G: GlobalQueue + Send + Sync> FutureTask<G> for multi_level::MultiLevelTask<AtomicFuture<G>> {
    fn as_atomic_future(&self) -> &AtomicFuture<G> {
        &self.task
    }
}

impl<G, T> super::Runner for Runner<G>
where
    G: GlobalQueue<Task = Arc<T>> + Send + Sync + 'static,
    T: FutureTask<G>,
{
    type GlobalQueue = G;

    fn handle(&mut self, ctx: &mut PoolContext<G>, task: Arc<T>) -> bool {
        let mut tried_times = 1;
        let atomic_future = task.as_atomic_future();

        unsafe {
            atomic_future.status.store(POLLING, SeqCst);
            let waker = ManuallyDrop::new(waker(&*task));
            let mut cx = Context::from_waker(&waker);
            loop {
                if let Poll::Ready(_) = Pin::new(&mut *atomic_future.future.get()).poll(&mut cx) {
                    atomic_future.status.store(COMPLETE, SeqCst);
                    return true;
                }
                match atomic_future
                    .status
                    .compare_exchange(POLLING, WAITING, SeqCst, SeqCst)
                {
                    Ok(_) => return false,
                    Err(_) => atomic_future.status.store(POLLING, SeqCst),
                }
            }
        }
    }
}

pub struct SingleQueue(crossbeam_deque::Injector<SchedUnit<Arc<AtomicFuture<SingleQueue>>>>);

impl GlobalQueue for SingleQueue {
    type Task = Arc<AtomicFuture<SingleQueue>>;

    fn steal_batch_and_pop(
        &self,
        local_queue: &crossbeam_deque::Worker<SchedUnit<Self::Task>>,
    ) -> Steal<SchedUnit<Self::Task>> {
        crossbeam_deque::Injector::steal_batch_and_pop(&self.0, local_queue)
    }
    fn push(&self, task: SchedUnit<Self::Task>) {
        self.0.push(task);
    }
}

pub struct SimpleThreadPool(super::ThreadPool<SingleQueue>);

impl SimpleThreadPool {
    pub fn from_config(config: Config) -> Self {
        let pool = config.spawn(RunnerFactory::new(4), || {
            SingleQueue(crossbeam_deque::Injector::new())
        });
        Self(pool)
    }

    pub fn spawn_future<F: Future<Output = ()> + Send + 'static>(&self, f: F) {
        let task = Arc::new(AtomicFuture::new(f, self.0.remote()));
        self.0.spawn(task);
    }
}

// pub struct MultiLevelThreadPool(
//     super::ThreadPool<
//         multi_level::MultiLevelQueue<TaskUnit, Arc<multi_level::MultiLevelTask<TaskUnit>>>,
//     >,
// );

// impl MultiLevelThreadPool {
//     pub fn from_config(config: Config) -> Self {
//         let pool = config.spawn(RunnerFactory::new(4), || {
//             multi_level::MultiLevelQueue::new()
//         });
//         Self(pool)
//     }

//     pub fn spawn_future<F: Future<Item = (), Error = ()> + Send + 'static>(
//         &self,
//         f: F,
//         task_id: u64,
//         fixed_level: Option<u8>,
//     ) {
//         let task = TaskUnit::new(f);
//         let t = Arc::new(
//             self.0
//                 .global_queue()
//                 .create_task(task, task_id, fixed_level),
//         );
//         self.0.spawn(t);
//     }

//     pub fn async_adjust_level_ratio(&self) -> impl std::future::Future<Output = ()> {
//         self.0.global_queue().async_adjust_level_ratio()
//     }

//     pub fn async_cleanup_stats(&self) -> impl std::future::Future<Output = ()> {
//         self.0.global_queue().async_cleanup_stats()
//     }
// }

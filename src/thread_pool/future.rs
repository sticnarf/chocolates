use super::{Config, GlobalQueue, PoolContext, SchedUnit};
use crate::thread_pool::global_queue::multi_level;
use futures::executor::{self, Notify, Spawn};
use futures::future::{ExecuteError, Executor};
use futures::sync::oneshot;
use futures::{Async, Future};
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::{mem, ptr};

pub use futures::sync::oneshot::SpawnHandle;

pub struct Runner<G>
where
    G: GlobalQueue,
{
    max_inplace_spin: usize,
    notifier: Option<Arc<ThreadPoolNotify<G>>>,
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
    T: FutureTask,
{
    type Runner = Runner<G>;

    fn produce(&mut self) -> Runner<G> {
        Runner {
            max_inplace_spin: self.max_inplace_spin,
            notifier: None,
            _phantom: PhantomData,
        }
    }
}

thread_local! {
    static LOCAL_WAKER: UnsafeCell<*mut ()> = UnsafeCell::new(ptr::null_mut());
}

pub struct Sender<G>
where
    G: GlobalQueue,
{
    remote: super::Remote<G>,
}

impl<G, T> Sender<G>
where
    G: GlobalQueue<Task = Arc<T>> + Send + Sync + 'static,
    T: FutureTask,
{
    fn spawn_task(&self, task: Arc<T>) {
        LOCAL_WAKER.with(|w| {
            let ptr = unsafe { *w.get() };
            if ptr.is_null() {
                self.remote.spawn(task)
            } else {
                unsafe { (*(ptr as *mut PoolContext<G>)).spawn(task) }
            }
        })
    }
}

impl<G> Clone for Sender<G>
where
    G: GlobalQueue,
{
    fn clone(&self) -> Self {
        Sender {
            remote: self.remote.clone(),
        }
    }
}

pub struct ThreadPoolNotify<G>
where
    G: GlobalQueue,
{
    sender: Sender<G>,
}

impl<G, T> Notify for ThreadPoolNotify<G>
where
    G: GlobalQueue<Task = Arc<T>> + Send + Sync + 'static,
    T: FutureTask,
{
    fn notify(&self, id: usize) {
        let task = unsafe { Arc::from_raw(id as *mut T) };
        if !task.as_task_unit().mark_scheduled() {
            mem::forget(task);
            return;
        }
        let t = task.clone();
        mem::forget(task);
        self.sender.spawn_task(t);
    }

    fn clone_id(&self, id: usize) -> usize {
        let task = unsafe { Arc::from_raw(id as *mut T) };
        let t = task.clone();
        mem::forget(task);
        Arc::into_raw(t) as usize
    }

    fn drop_id(&self, id: usize) {
        unsafe { Arc::from_raw(id as *mut T) };
    }
}

const IDLE: u8 = 0;
const SCHEDULED: u8 = 1;
const POLLING: u8 = 2;
const COMPLETED: u8 = 3;
const RESCHEDULED: u8 = 4;

pub struct TaskUnit {
    state: AtomicU8,
    task: UnsafeCell<Option<Spawn<Box<dyn Future<Item = (), Error = ()> + Send>>>>,
}

impl TaskUnit {
    pub fn new(f: impl Future<Item = (), Error = ()> + Send + 'static) -> TaskUnit {
        TaskUnit {
            state: AtomicU8::new(SCHEDULED),
            task: UnsafeCell::new(Some(executor::spawn(Box::new(f)))),
        }
    }
}

unsafe impl Sync for TaskUnit {}

impl TaskUnit {
    fn mark_scheduled(&self) -> bool {
        loop {
            let state = self
                .state
                .compare_and_swap(IDLE, SCHEDULED, Ordering::SeqCst);
            match state {
                IDLE => return true,
                POLLING => {
                    match self
                        .state
                        .compare_and_swap(POLLING, RESCHEDULED, Ordering::SeqCst)
                    {
                        IDLE => (),
                        POLLING | SCHEDULED | COMPLETED | RESCHEDULED | _ => return false,
                    }
                }
                SCHEDULED | COMPLETED | RESCHEDULED | _ => return false,
            }
        }
    }

    fn mark_idle(&self) -> bool {
        let state = self.state.compare_and_swap(POLLING, IDLE, Ordering::SeqCst);
        match state {
            POLLING => return true,
            RESCHEDULED => return false,
            IDLE | SCHEDULED | COMPLETED | _ => unreachable!(),
        }
    }

    fn mark_polling(&self) {
        let state = self.state.swap(POLLING, Ordering::SeqCst);
        if state != SCHEDULED && state != RESCHEDULED {
            panic!("unexpected state transition: {} -> POLLING", state);
        }
    }

    fn on_completed(&self) {
        let state = self.state.swap(COMPLETED, Ordering::SeqCst);
        match state {
            POLLING | RESCHEDULED => (),
            IDLE | SCHEDULED | COMPLETED | _ => unreachable!(),
        }
        unsafe { &mut *self.task.get() }.take();
    }
}

pub trait FutureTask: Send + Sync {
    fn as_task_unit(&self) -> &TaskUnit;
}

impl FutureTask for TaskUnit {
    fn as_task_unit(&self) -> &TaskUnit {
        self
    }
}

impl FutureTask for multi_level::MultiLevelTask<TaskUnit> {
    fn as_task_unit(&self) -> &TaskUnit {
        &self.task
    }
}

impl<G, T> super::Runner for Runner<G>
where
    G: GlobalQueue<Task = Arc<T>> + Send + Sync + 'static,
    T: FutureTask,
{
    type GlobalQueue = G;

    fn start(&mut self, ctx: &mut PoolContext<G>) {
        LOCAL_WAKER.with(|w| {
            let waker = unsafe { &mut *w.get() };
            assert!((*waker).is_null());
            *waker = ctx as *mut _ as *mut ();
        });
        self.notifier = Some(Arc::new(ThreadPoolNotify {
            sender: Sender {
                remote: ctx.remote(),
            },
        }));
    }

    fn handle(&mut self, ctx: &mut PoolContext<G>, task: Arc<T>) -> bool {
        let mut tried_times = 1;
        let id = &*task as *const T as usize;
        let task_unit = task.as_task_unit();
        let spawn = unsafe { &mut *task_unit.task.get() }.as_mut().unwrap();
        let notifier = self.notifier.as_ref().unwrap();
        loop {
            task_unit.mark_polling();
            let res = spawn.poll_future_notify(notifier, id);
            match res {
                Ok(Async::NotReady) => {
                    if task_unit.mark_idle() {
                        return false;
                    } else {
                        if tried_times >= self.max_inplace_spin {
                            ctx.spawn(task);
                            return false;
                        } else {
                            tried_times += 1;
                        }
                    }
                }
                Ok(Async::Ready(())) | Err(()) => {
                    task_unit.on_completed();
                    return true;
                }
            }
        }
    }

    fn end(&mut self, _: &PoolContext<G>) {
        LOCAL_WAKER.with(|w| {
            let waker = unsafe { &mut *w.get() };
            assert!(!(*waker).is_null());
            *waker = ptr::null_mut();
        });
        self.notifier.take();
    }
}

pub struct SimpleThreadPool(super::ThreadPool<crossbeam_deque::Injector<SchedUnit<Arc<TaskUnit>>>>);

impl SimpleThreadPool {
    pub fn from_config(config: Config) -> Self {
        let pool = config.spawn(RunnerFactory::new(4), || crossbeam_deque::Injector::new());
        Self(pool)
    }

    pub fn spawn_future<F: Future<Item = (), Error = ()> + Send + 'static>(&self, f: F) {
        let t = Arc::new(TaskUnit::new(f));
        self.0.spawn(t);
    }

    pub fn spawn_future_handle<F>(&self, f: F) -> SpawnHandle<F::Item, F::Error>
    where
        F: Future + Send + 'static,
        F::Item: Send,
        F::Error: Send,
    {
        oneshot::spawn(f, self)
    }

    pub fn sender(&self) -> Sender<crossbeam_deque::Injector<SchedUnit<Arc<TaskUnit>>>> {
        Sender {
            remote: self.0.remote(),
        }
    }
}

impl Sender<crossbeam_deque::Injector<SchedUnit<Arc<TaskUnit>>>> {
    pub fn spawn(&self, f: impl Future<Item = (), Error = ()> + Send + 'static) {
        self.spawn_task(Arc::new(TaskUnit::new(f)))
    }

    pub fn spawn_handle<F>(&self, f: F) -> SpawnHandle<F::Item, F::Error>
    where
        F: Future + Send + 'static,
        F::Item: Send,
        F::Error: Send,
    {
        oneshot::spawn(f, self)
    }
}

impl<F> Executor<F> for SimpleThreadPool
where
    F: Future<Item = (), Error = ()> + Send + 'static,
{
    fn execute(&self, future: F) -> Result<(), ExecuteError<F>> {
        self.spawn_future(future);
        // TODO: handle shutdown here.
        Ok(())
    }
}

impl<F> Executor<F> for Sender<crossbeam_deque::Injector<SchedUnit<Arc<TaskUnit>>>>
where
    F: Future<Item = (), Error = ()> + Send + 'static,
{
    fn execute(&self, future: F) -> Result<(), ExecuteError<F>> {
        self.spawn(future);
        // TODO: handle shutdown here.
        Ok(())
    }
}

pub struct MultiLevelThreadPool(
    super::ThreadPool<
        multi_level::MultiLevelQueue<TaskUnit, Arc<multi_level::MultiLevelTask<TaskUnit>>>,
    >,
);

impl MultiLevelThreadPool {
    pub fn from_config(config: Config) -> Self {
        let pool = config.spawn(RunnerFactory::new(4), || {
            multi_level::MultiLevelQueue::new()
        });
        Self(pool)
    }

    pub fn spawn_future<F: Future<Item = (), Error = ()> + Send + 'static>(
        &self,
        f: F,
        task_id: u64,
        fixed_level: Option<u8>,
    ) {
        let task = TaskUnit::new(f);
        let t = Arc::new(
            self.0
                .global_queue()
                .create_task(task, task_id, fixed_level),
        );
        self.0.spawn(t);
    }
}

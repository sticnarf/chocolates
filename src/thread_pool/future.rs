use super::{PoolContext, TaskProvider};
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

pub struct Runner<P>
where
    P: TaskProvider<Task = Arc<TaskUnit>, RawTask = Arc<TaskUnit>> + Send + Sync + 'static,
{
    max_inplace_spin: usize,
    notifier: Option<Arc<ThreadPoolNotify<P>>>,
}

pub struct RunnerFactory<P>
where
    P: TaskProvider<Task = Arc<TaskUnit>, RawTask = Arc<TaskUnit>> + Send + Sync + 'static,
{
    max_inplace_spin: usize,
    _phantom: PhantomData<P>,
}

impl<P> RunnerFactory<P>
where
    P: TaskProvider<Task = Arc<TaskUnit>, RawTask = Arc<TaskUnit>> + Send + Sync + 'static,
{
    pub fn new(max_inplace_spin: usize) -> RunnerFactory<P> {
        RunnerFactory {
            max_inplace_spin,
            _phantom: PhantomData,
        }
    }
}

impl<P> Default for RunnerFactory<P>
where
    P: TaskProvider<Task = Arc<TaskUnit>, RawTask = Arc<TaskUnit>> + Send + Sync + 'static,
{
    fn default() -> RunnerFactory<P> {
        RunnerFactory {
            max_inplace_spin: 4,
            _phantom: PhantomData,
        }
    }
}

impl<P> super::RunnerFactory for RunnerFactory<P>
where
    P: TaskProvider<Task = Arc<TaskUnit>, RawTask = Arc<TaskUnit>> + Send + Sync + 'static,
{
    type Runner = Runner<P>;

    fn produce(&mut self) -> Runner<P> {
        Runner {
            max_inplace_spin: self.max_inplace_spin,
            notifier: None,
        }
    }
}

thread_local! {
    // static LOCAL_WAKER: UnsafeCell<*mut PoolContext<Arc<TaskUnit>>> = UnsafeCell::new(ptr::null_mut());
    static LOCAL_WAKER: UnsafeCell<*mut ()> = UnsafeCell::new(ptr::null_mut());
}

pub struct Sender<P>
where
    P: TaskProvider<Task = Arc<TaskUnit>, RawTask = Arc<TaskUnit>> + Send + Sync + 'static,
{
    remote: super::Remote<P>,
}

impl<P> Sender<P>
where
    P: TaskProvider<Task = Arc<TaskUnit>, RawTask = Arc<TaskUnit>> + Send + Sync + 'static,
{
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

    fn spawn_task(&self, task: Arc<TaskUnit>) {
        LOCAL_WAKER.with(|w| {
            let ptr = unsafe { *w.get() };
            if ptr.is_null() {
                self.remote.spawn(task)
            } else {
                unsafe { (*(ptr as *mut PoolContext<P>)).spawn(task) }
            }
        })
    }
}

impl<P> Clone for Sender<P>
where
    P: TaskProvider<Task = Arc<TaskUnit>, RawTask = Arc<TaskUnit>> + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            remote: self.remote.clone(),
        }
    }
}

pub struct ThreadPoolNotify<P>
where
    P: TaskProvider<Task = Arc<TaskUnit>, RawTask = Arc<TaskUnit>> + Send + Sync + 'static,
{
    sender: Sender<P>,
}

impl<P> Notify for ThreadPoolNotify<P>
where
    P: TaskProvider<Task = Arc<TaskUnit>, RawTask = Arc<TaskUnit>> + Send + Sync + 'static,
{
    fn notify(&self, id: usize) {
        let task = unsafe { Arc::from_raw(id as *mut TaskUnit) };
        if !task.mark_scheduled() {
            mem::forget(task);
            return;
        }
        let t = task.clone();
        mem::forget(task);
        self.sender.spawn_task(t);
    }

    fn clone_id(&self, id: usize) -> usize {
        let task = unsafe { Arc::from_raw(id as *mut TaskUnit) };
        let t = task.clone();
        mem::forget(task);
        Arc::into_raw(t) as usize
    }

    fn drop_id(&self, id: usize) {
        unsafe { Arc::from_raw(id as *mut TaskUnit) };
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

impl<P> super::ThreadPool<P>
where
    P: TaskProvider<Task = Arc<TaskUnit>, RawTask = Arc<TaskUnit>> + Send + Sync + 'static,
{
    pub fn spawn_future<F: Future<Item = (), Error = ()> + Send + 'static>(&self, f: F) {
        let t = Arc::new(TaskUnit::new(f));
        self.spawn(t);
    }

    pub fn spawn_future_handle<F>(&self, f: F) -> SpawnHandle<F::Item, F::Error>
    where
        F: Future + Send + 'static,
        F::Item: Send,
        F::Error: Send,
    {
        oneshot::spawn(f, self)
    }

    pub fn sender(&self) -> Sender<P> {
        Sender {
            remote: self.remote(),
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

impl<P> super::Runner for Runner<P>
where
    P: TaskProvider<Task = Arc<TaskUnit>, RawTask = Arc<TaskUnit>> + Send + Sync + 'static,
{
    type TaskProvider = P;

    fn start(&mut self, ctx: &mut PoolContext<P>) {
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

    fn handle(&mut self, ctx: &mut PoolContext<P>, task: Arc<TaskUnit>) -> bool {
        let mut tried_times = 1;
        let id = &*task as *const TaskUnit as usize;
        let spawn = unsafe { &mut *task.task.get() }.as_mut().unwrap();
        let notifier = self.notifier.as_ref().unwrap();
        loop {
            task.mark_polling();
            let res = spawn.poll_future_notify(notifier, id);
            match res {
                Ok(Async::NotReady) => {
                    if task.mark_idle() {
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
                    task.on_completed();
                    return true;
                }
            }
        }
    }

    fn end(&mut self, _: &PoolContext<P>) {
        LOCAL_WAKER.with(|w| {
            let waker = unsafe { &mut *w.get() };
            assert!(!(*waker).is_null());
            *waker = ptr::null_mut();
        });
        self.notifier.take();
    }
}

impl<F, P> Executor<F> for Sender<P>
where
    F: Future<Item = (), Error = ()> + Send + 'static,
    P: TaskProvider<Task = Arc<TaskUnit>, RawTask = Arc<TaskUnit>> + Send + Sync + 'static,
{
    fn execute(&self, future: F) -> Result<(), ExecuteError<F>> {
        self.spawn(future);
        // TODO: handle shutdown here.
        Ok(())
    }
}

impl<F, P> Executor<F> for super::ThreadPool<P>
where
    F: Future<Item = (), Error = ()> + Send + 'static,
    P: TaskProvider<Task = Arc<TaskUnit>, RawTask = Arc<TaskUnit>> + Send + Sync + 'static,
{
    fn execute(&self, future: F) -> Result<(), ExecuteError<F>> {
        self.spawn_future(future);
        // TODO: handle shutdown here.
        Ok(())
    }
}

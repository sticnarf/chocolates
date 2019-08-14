use super::PoolContext;
use futures::executor::{self, Notify, Spawn};
use futures::future::{ExecuteError, Executor};
use futures::{Async, Future};
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::{mem, ptr};

pub struct Runner {
    max_inplace_spin: usize,
    notifier: Option<Arc<ThreadPoolNotify>>,
}

pub struct RunnerFactory {
    max_inplace_spin: usize,
}

impl RunnerFactory {
    pub fn new(max_inplace_spin: usize) -> RunnerFactory {
        RunnerFactory { max_inplace_spin }
    }
}

impl super::RunnerFactory for RunnerFactory {
    type Runner = Runner;

    fn produce(&mut self) -> Runner {
        Runner {
            max_inplace_spin: self.max_inplace_spin,
            notifier: None,
        }
    }
}

thread_local! {
    static LOCAL_WAKER: UnsafeCell<*mut PoolContext<Arc<TaskUnit>>> = UnsafeCell::new(ptr::null_mut());
}

#[derive(Clone)]
pub struct Sender {
    remote: super::Remote<Arc<TaskUnit>>,
}

impl Sender {
    pub fn spawn(&self, f: impl Future<Item = (), Error = ()> + Send + 'static) {
        self.spawn_task(Arc::new(TaskUnit::new(f)))
    }

    fn spawn_task(&self, task: Arc<TaskUnit>) {
        LOCAL_WAKER.with(|w| {
            let ptr = unsafe { *w.get() };
            if ptr.is_null() {
                self.remote.spawn(task)
            } else {
                unsafe { &mut *ptr }.spawn(task)
            }
        })
    }
}

pub struct ThreadPoolNotify {
    sender: Sender,
}

impl Notify for ThreadPoolNotify {
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

pub type FutureThreadPool = super::ThreadPool<Arc<TaskUnit>>;

impl FutureThreadPool {
    pub fn spawn_future<F: Future<Item = (), Error = ()> + Send + 'static>(&self, f: F) {
        let t = Arc::new(TaskUnit::new(f));
        self.spawn(t);
    }

    pub fn sender(&self) -> Sender {
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

impl super::Runner for Runner {
    type Task = Arc<TaskUnit>;

    fn start(&mut self, ctx: &mut PoolContext<Self::Task>) {
        LOCAL_WAKER.with(|w| {
            let waker = unsafe { &mut *w.get() };
            assert!((*waker).is_null());
            *waker = ctx;
        });
        self.notifier = Some(Arc::new(ThreadPoolNotify {
            sender: Sender {
                remote: ctx.remote(),
            },
        }));
    }

    fn handle(&mut self, ctx: &mut PoolContext<Self::Task>, task: Self::Task) {
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
                        break;
                    } else {
                        if tried_times >= self.max_inplace_spin {
                            ctx.spawn(task);
                            break;
                        } else {
                            tried_times += 1;
                        }
                    }
                }
                Ok(Async::Ready(())) | Err(()) => {
                    task.on_completed();
                    break;
                }
            }
        }
    }

    fn end(&mut self, _: &PoolContext<Self::Task>) {
        LOCAL_WAKER.with(|w| {
            let waker = unsafe { &mut *w.get() };
            assert!(!(*waker).is_null());
            *waker = ptr::null_mut();
        });
        self.notifier.take();
    }
}

impl<F> Executor<F> for Sender
where
    F: Future<Item = (), Error = ()> + Send + 'static,
{
    fn execute(&self, future: F) -> Result<(), ExecuteError<F>> {
        self.spawn(future);
        // TODO: handle shutdown here.
        Ok(())
    }
}

impl<F> Executor<F> for FutureThreadPool
where
    F: Future<Item = (), Error = ()> + Send + 'static,
{
    fn execute(&self, future: F) -> Result<(), ExecuteError<F>> {
        self.spawn_future(future);
        // TODO: handle shutdown here.
        Ok(())
    }
}

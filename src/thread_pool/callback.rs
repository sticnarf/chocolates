use super::{Config, GlobalQueue, PoolContext, SchedUnit};
use crossbeam_deque::Steal;
use std::marker::PhantomData;

pub enum Task<G>
where
    G: GlobalQueue,
{
    Once(Box<dyn FnOnce(&mut Handle<'_, G>) + Send>),
    Mut(Box<dyn FnMut(&mut Handle<'_, G>) + Send>),
}

pub trait CallbackTask<G>: Sized
where
    G: GlobalQueue<Task = Self>,
{
    fn as_mut_task(&mut self) -> &mut Task<G>;

    fn into_task(self) -> Task<G>;
}

impl<G> CallbackTask<G> for Task<G>
where
    G: GlobalQueue<Task = Self>,
{
    fn as_mut_task(&mut self) -> &mut Task<G> {
        self
    }

    fn into_task(self) -> Task<G> {
        self
    }
}

pub struct Runner<G>
where
    G: GlobalQueue,
{
    max_inplace_spin: usize,
    _phantom: PhantomData<G>,
}

impl<G, T> super::Runner for Runner<G>
where
    G: GlobalQueue<Task = T>,
    T: CallbackTask<G>,
{
    type GlobalQueue = G;

    fn handle(&mut self, ctx: &mut PoolContext<G>, mut task: G::Task) -> bool {
        let mut handle = Handle { ctx, rerun: false };
        match task.as_mut_task() {
            Task::Mut(ref mut r) => {
                let mut tried_times = 0;
                loop {
                    r(&mut handle);
                    if !handle.rerun {
                        return true;
                    }
                    // TODO: fix the bug here when set to true.
                    handle.rerun = false;
                    tried_times += 1;
                    if tried_times == self.max_inplace_spin {
                        break;
                    }
                }
            }
            Task::Once(_) => {
                if let Task::Once(r) = task.into_task() {
                    r(&mut handle);
                    return true;
                }
                unreachable!()
            }
        }
        ctx.spawn(task);
        false
    }
}

pub struct Handle<'a, G>
where
    G: GlobalQueue,
{
    ctx: &'a mut PoolContext<G>,
    rerun: bool,
}

impl<'a, G> Handle<'a, G>
where
    G: GlobalQueue,
{
    pub fn rerun(&mut self) {
        self.rerun = true;
    }

    pub fn to_owned(&self) -> Remote<G> {
        Remote {
            remote: self.ctx.remote(),
        }
    }
}

impl<'a, G> Handle<'a, G>
where
    G: GlobalQueue<Task = Task<G>>,
{
    pub fn spawn_once(&mut self, t: impl FnOnce(&mut Handle<'_, G>) + Send + 'static) {
        self.ctx.spawn(Task::Once(Box::new(t)));
    }

    pub fn spawn_mut(&mut self, t: impl FnMut(&mut Handle<'_, G>) + Send + 'static) {
        self.ctx.spawn(Task::Mut(Box::new(t)));
    }
}

pub struct Remote<G>
where
    G: GlobalQueue,
{
    remote: super::Remote<G>,
}

impl<G> Remote<G>
where
    G: GlobalQueue<Task = Task<G>>,
{
    pub fn spawn_once(&self, t: impl FnOnce(&mut Handle<'_, G>) + Send + 'static) {
        self.remote.spawn(Task::Once(Box::new(t)));
    }

    pub fn spawn_mut(&self, t: impl FnMut(&mut Handle<'_, G>) + Send + 'static) {
        self.remote.spawn(Task::Mut(Box::new(t)))
    }
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
    pub fn new() -> Self {
        RunnerFactory {
            max_inplace_spin: 4,
            _phantom: PhantomData,
        }
    }

    pub fn set_max_inplace_spin(&mut self, count: usize) {
        self.max_inplace_spin = count;
    }
}

impl<G, T> super::RunnerFactory for RunnerFactory<G>
where
    G: GlobalQueue<Task = T>,
    T: CallbackTask<G>,
{
    type Runner = Runner<G>;

    fn produce(&mut self) -> Runner<G> {
        Runner {
            max_inplace_spin: self.max_inplace_spin,
            _phantom: PhantomData,
        }
    }
}

// Thread pool with only one global queue.
//
// For lack of lazy normalization, a wrapper type is needed to avoid cyclic type error.

pub struct SingleQueue(crossbeam_deque::Injector<SchedUnit<Task<SingleQueue>>>);

impl GlobalQueue for SingleQueue {
    type Task = Task<SingleQueue>;

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
        let pool = config.spawn(RunnerFactory::new(), || {
            SingleQueue(crossbeam_deque::Injector::new())
        });
        Self(pool)
    }

    pub fn spawn_once(&self, t: impl FnOnce(&mut Handle<'_, SingleQueue>) + Send + 'static) {
        self.0.spawn(Task::Once(Box::new(t)))
    }

    pub fn spawn_mut(&self, t: impl FnMut(&mut Handle<'_, SingleQueue>) + Send + 'static) {
        self.0.spawn(Task::Mut(Box::new(t)))
    }
}

// Thread pool with multi-level queues.
//
// For lack of lazy normalization, a wrapper type is needed to avoid cyclic type error.

use crate::thread_pool::global_queue::multi_level;

pub struct MultiLevelQueue(multi_level::MultiLevelQueue<Task<MultiLevelQueue>>);

impl GlobalQueue for MultiLevelQueue {
    type Task = multi_level::MultiLevelTask<Task<MultiLevelQueue>>;

    fn steal_batch_and_pop(
        &self,
        local_queue: &crossbeam_deque::Worker<SchedUnit<Self::Task>>,
    ) -> Steal<SchedUnit<Self::Task>> {
        self.0.steal_batch_and_pop(local_queue)
    }
    fn push(&self, task: SchedUnit<Self::Task>) {
        self.0.push(task);
    }
}

impl<G> CallbackTask<G> for multi_level::MultiLevelTask<Task<G>>
where
    G: GlobalQueue<Task = Self>,
{
    fn as_mut_task(&mut self) -> &mut Task<G> {
        &mut self.task
    }

    fn into_task(self) -> Task<G> {
        self.task
    }
}

pub struct MultiLevelThreadPool(super::ThreadPool<MultiLevelQueue>);

impl MultiLevelThreadPool {
    pub fn from_config(config: Config) -> Self {
        let pool = config.spawn(RunnerFactory::new(), || {
            MultiLevelQueue(multi_level::MultiLevelQueue::new())
        });
        Self(pool)
    }

    pub fn spawn_once(
        &self,
        t: impl FnOnce(&mut Handle<'_, MultiLevelQueue>) + Send + 'static,
        task_id: u64,
        fixed_level: Option<u8>,
    ) {
        let global = &self.0.global_queue().0;
        let task = global.create_task(Task::Once(Box::new(t)), task_id, fixed_level);
        self.0.spawn(task)
    }

    pub fn spawn_mut(
        &self,
        t: impl FnMut(&mut Handle<'_, MultiLevelQueue>) + Send + 'static,
        task_id: u64,
        fixed_level: Option<u8>,
    ) {
        let global = &self.0.global_queue().0;
        let task = global.create_task(Task::Mut(Box::new(t)), task_id, fixed_level);
        self.0.spawn(task);
    }
}

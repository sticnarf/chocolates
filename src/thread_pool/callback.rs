use super::{GlobalQueue, PoolContext};
use std::marker::PhantomData;

pub enum Task<G>
where
    G: GlobalQueue<Task = Task<G>, RawTask = Task<G>>,
{
    Once(Box<dyn FnOnce(&mut Handle<'_, G>) + Send>),
    Mut(Box<dyn FnMut(&mut Handle<'_, G>) + Send>),
}

impl<G> Task<G>
where
    G: GlobalQueue<Task = Task<G>, RawTask = Task<G>>,
{
    fn erase_type(self) -> TypeErasedTask {
        unsafe { std::mem::transmute(self) }
    }
}

/// Same as `Task<G>` but the type is erased to avoid cyclic type error.
pub struct TypeErasedTask([u8; 24]);

pub struct Runner<G>
where
    G: GlobalQueue<Task = Task<G>, RawTask = Task<G>>,
{
    max_inplace_spin: usize,
    _phantom: PhantomData<G>,
}

impl<G> super::Runner for Runner<G>
where
    G: GlobalQueue<Task = Task<G>, RawTask = Task<G>>,
{
    type GlobalQueue = G;

    fn handle(&mut self, ctx: &mut PoolContext<G>, mut task: G::Task) -> bool {
        let mut handle = Handle { ctx, rerun: false };
        match task {
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
            Task::Once(r) => {
                r(&mut handle);
                return true;
            }
        }
        ctx.spawn(task);
        false
    }
}

pub struct Handle<'a, G>
where
    G: GlobalQueue<Task = Task<G>, RawTask = Task<G>>,
{
    ctx: &'a mut PoolContext<G>,
    rerun: bool,
}

impl<'a, G> Handle<'a, G>
where
    G: GlobalQueue<Task = Task<G>, RawTask = Task<G>>,
{
    pub fn spawn_once(&mut self, t: impl FnOnce(&mut Handle<'_, G>) + Send + 'static) {
        self.ctx.spawn(Task::Once(Box::new(t)));
    }

    pub fn spawn_mut(&mut self, t: impl FnMut(&mut Handle<'_, G>) + Send + 'static) {
        self.ctx.spawn(Task::Mut(Box::new(t)));
    }

    pub fn rerun(&mut self) {
        self.rerun = true;
    }

    pub fn to_owned(&self) -> Remote<G> {
        Remote {
            remote: self.ctx.remote(),
        }
    }
}

pub struct Remote<G>
where
    G: GlobalQueue<Task = Task<G>, RawTask = Task<G>>,
{
    remote: super::Remote<G>,
}

impl<G> Remote<G>
where
    G: GlobalQueue<Task = Task<G>, RawTask = Task<G>>,
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
    G: GlobalQueue<Task = Task<G>, RawTask = Task<G>>,
{
    max_inplace_spin: usize,
    _phantom: PhantomData<G>,
}

impl<G> RunnerFactory<G>
where
    G: GlobalQueue<Task = Task<G>, RawTask = Task<G>>,
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

impl<G> super::RunnerFactory for RunnerFactory<G>
where
    G: GlobalQueue<Task = Task<G>, RawTask = Task<G>>,
{
    type Runner = Runner<G>;

    fn produce(&mut self) -> Runner<G> {
        Runner {
            max_inplace_spin: self.max_inplace_spin,
            _phantom: PhantomData,
        }
    }
}

impl<G> super::ThreadPool<G>
where
    G: GlobalQueue<Task = Task<G>, RawTask = Task<G>>,
{
    pub fn spawn_once(&self, t: impl FnOnce(&mut Handle<'_, G>) + Send + 'static) {
        self.spawn(Task::Once(Box::new(t)));
    }

    pub fn spawn_mut(&self, t: impl FnMut(&mut Handle<'_, G>) + Send + 'static) {
        self.spawn(Task::Mut(Box::new(t)))
    }
}

use super::{PoolContext, SchedUnit, TaskProvider};

use static_assertions::assert_eq_size;
use std::marker::PhantomData;

pub enum Task<P>
where
    P: TaskProvider<Task = TypeErasedTask, RawTask = TypeErasedTask>,
{
    Once(Box<dyn FnOnce(&mut Handle<'_, P>) + Send>),
    Mut(Box<dyn FnMut(&mut Handle<'_, P>) + Send>),
}

/// Same as `Task<P>` but erased its actual type. This type is created to avoid cyclic type.
pub struct TypeErasedTask([u8; 24]);

assert_eq_size!(
    TypeErasedTask,
    Task<crossbeam_deque::Injector<SchedUnit<TypeErasedTask>>>
);

impl<P> Task<P>
where
    P: TaskProvider<Task = TypeErasedTask, RawTask = TypeErasedTask>,
{
    fn erase_type(self) -> TypeErasedTask {
        unsafe { std::mem::transmute(self) }
    }
}

pub struct Runner<P>
where
    P: TaskProvider<Task = TypeErasedTask, RawTask = TypeErasedTask>,
{
    max_inplace_spin: usize,
    _phantom: PhantomData<P>,
}

impl<P> super::Runner for Runner<P>
where
    P: TaskProvider<Task = TypeErasedTask, RawTask = TypeErasedTask>,
{
    type TaskProvider = P;

    fn handle(&mut self, ctx: &mut PoolContext<P>, task: TypeErasedTask) -> bool {
        let mut handle = Handle { ctx, rerun: false };
        let mut task: Task<P> = unsafe { std::mem::transmute(task) };
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
        ctx.spawn(task.erase_type());
        false
    }
}

pub struct Handle<'a, P>
where
    P: TaskProvider<Task = TypeErasedTask, RawTask = TypeErasedTask>,
{
    ctx: &'a mut PoolContext<P>,
    rerun: bool,
}

impl<'a, P: TaskProvider> Handle<'a, P>
where
    P: TaskProvider<Task = TypeErasedTask, RawTask = TypeErasedTask>,
{
    pub fn spawn_once(&mut self, t: impl FnOnce(&mut Handle<'_, P>) + Send + 'static) {
        self.ctx.spawn(Task::Once(Box::new(t)).erase_type());
    }

    pub fn spawn_mut(&mut self, t: impl FnMut(&mut Handle<'_, P>) + Send + 'static) {
        self.ctx.spawn(Task::Mut(Box::new(t)).erase_type());
    }

    pub fn rerun(&mut self) {
        self.rerun = true;
    }

    pub fn to_owned(&self) -> Remote<P> {
        Remote {
            remote: self.ctx.remote(),
        }
    }
}

pub struct Remote<P>
where
    P: TaskProvider<Task = TypeErasedTask>,
{
    remote: super::Remote<P>,
}

impl<P> Remote<P>
where
    P: TaskProvider<Task = TypeErasedTask, RawTask = TypeErasedTask>,
{
    pub fn spawn_once(&self, t: impl FnOnce(&mut Handle<'_, P>) + Send + 'static) {
        self.remote.spawn(Task::Once(Box::new(t)).erase_type());
    }

    pub fn spawn_mut(&self, t: impl FnMut(&mut Handle<'_, P>) + Send + 'static) {
        self.remote.spawn(Task::Mut(Box::new(t)).erase_type())
    }
}

pub struct RunnerFactory<P>
where
    P: TaskProvider<Task = TypeErasedTask, RawTask = TypeErasedTask>,
{
    max_inplace_spin: usize,
    _phantom: PhantomData<P>,
}

impl<P> RunnerFactory<P>
where
    P: TaskProvider<Task = TypeErasedTask, RawTask = TypeErasedTask>,
{
    pub fn new() -> RunnerFactory<P> {
        RunnerFactory {
            max_inplace_spin: 4,
            _phantom: PhantomData,
        }
    }

    pub fn set_max_inplace_spin(&mut self, count: usize) {
        self.max_inplace_spin = count;
    }
}

impl<P> super::RunnerFactory for RunnerFactory<P>
where
    P: TaskProvider<Task = TypeErasedTask, RawTask = TypeErasedTask>,
{
    type Runner = Runner<P>;

    fn produce(&mut self) -> Runner<P> {
        Runner {
            max_inplace_spin: self.max_inplace_spin,
            _phantom: PhantomData,
        }
    }
}

impl<P> super::ThreadPool<P>
where
    P: TaskProvider<Task = TypeErasedTask, RawTask = TypeErasedTask>,
{
    pub fn spawn_once(&self, t: impl FnOnce(&mut Handle<'_, P>) + Send + 'static) {
        self.spawn(Task::Once(Box::new(t)).erase_type());
    }

    pub fn spawn_mut(&self, t: impl FnMut(&mut Handle<'_, P>) + Send + 'static) {
        self.spawn(Task::Mut(Box::new(t)).erase_type())
    }
}

use super::PoolContext;

pub enum Task {
    Once(Box<dyn FnOnce(&mut Handle<'_>) + Send>),
    Mut(Box<dyn FnMut(&mut Handle<'_>) + Send>),
}

pub struct Runner {
    max_inplace_spin: usize,
}

impl super::Runner for Runner {
    type Task = Task;

    fn handle(&mut self, ctx: &mut PoolContext<Task>, mut task: Task) {
        let mut handle = Handle { ctx, rerun: false };
        match task {
            Task::Mut(ref mut r) => {
                let mut tried_times = 0;
                loop {
                    r(&mut handle);
                    if !handle.rerun {
                        return;
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
                return;
            }
        }
        ctx.spawn(task);
    }
}

pub struct Handle<'a> {
    ctx: &'a mut PoolContext<Task>,
    rerun: bool,
}

impl<'a> Handle<'a> {
    pub fn spawn_once(&mut self, t: impl FnOnce(&mut Handle<'_>) + Send + 'static) {
        self.ctx.spawn(Task::Once(Box::new(t)));
    }

    pub fn spawn_mut(&mut self, t: impl FnMut(&mut Handle<'_>) + Send + 'static) {
        self.ctx.spawn(Task::Mut(Box::new(t)));
    }

    pub fn rerun(&mut self) {
        self.rerun = true;
    }

    pub fn to_owned(&self) -> Remote {
        Remote {
            remote: self.ctx.remote(),
        }
    }
}

pub struct Remote {
    remote: super::Remote<Task>,
}

impl Remote {
    pub fn spawn_once(&self, t: impl FnOnce(&mut Handle<'_>) + Send + 'static) {
        self.remote.spawn(Task::Once(Box::new(t)));
    }

    pub fn spawn_mut(&self, t: impl FnMut(&mut Handle<'_>) + Send + 'static) {
        self.remote.spawn(Task::Mut(Box::new(t)))
    }
}

pub struct RunnerFactory {
    max_inplace_spin: usize,
}

impl RunnerFactory {
    pub fn new() -> RunnerFactory {
        RunnerFactory {
            max_inplace_spin: 4,
        }
    }

    pub fn set_max_inplace_spin(&mut self, count: usize) {
        self.max_inplace_spin = count;
    }
}

impl super::RunnerFactory for RunnerFactory {
    type Runner = Runner;

    fn produce(&mut self) -> Runner {
        Runner {
            max_inplace_spin: self.max_inplace_spin,
        }
    }
}

impl super::ThreadPool<Task> {
    pub fn spawn_once(&self, t: impl FnOnce(&mut Handle<'_>) + Send + 'static) {
        self.spawn(Task::Once(Box::new(t)));
    }

    pub fn spawn_mut(&self, t: impl FnMut(&mut Handle<'_>) + Send + 'static) {
        self.spawn(Task::Mut(Box::new(t)))
    }
}

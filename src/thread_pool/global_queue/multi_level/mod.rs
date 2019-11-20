mod stats;

use crate::thread_pool::{GlobalQueue, PoolContext, SchedUnit};
use stats::{TaskElapsed, TaskElapsedMap};

use crossbeam_deque::{Injector, Steal};
use init_with::InitWith;
use rand::prelude::*;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering::SeqCst};
use std::sync::Arc;
use std::time::{Duration, Instant};

const LEVEL_NUM: usize = 3;

pub struct MultiLevelQueue<Task, T = MultiLevelTask<Task>> {
    injectors: Arc<[Injector<SchedUnit<T>>; LEVEL_NUM]>,
    level_elapsed: LevelElapsed,
    task_elapsed_map: TaskElapsedMap,
    level_ratio: LevelRatio,
    _phantom: PhantomData<Task>,
}

unsafe impl<Task, T: Send> Send for MultiLevelQueue<Task, T> {}

unsafe impl<Task, T: Send> Sync for MultiLevelQueue<Task, T> {}

pub struct MultiLevelTask<Task> {
    pub(crate) task: Task,
    elapsed: TaskElapsed,
    level: AtomicU8,
    fixed_level: Option<u8>,
}

impl<Task> AsRef<MultiLevelTask<Task>> for MultiLevelTask<Task> {
    fn as_ref(&self) -> &MultiLevelTask<Task> {
        self
    }
}

impl<Task, T: AsRef<MultiLevelTask<Task>>> MultiLevelQueue<Task, T> {
    pub fn new() -> Self {
        Self {
            injectors: Arc::new(<[Injector<SchedUnit<T>>; LEVEL_NUM]>::init_with(|| {
                Injector::new()
            })),
            level_elapsed: LevelElapsed::default(),
            task_elapsed_map: TaskElapsedMap::new(),
            level_ratio: LevelRatio::default(),
            _phantom: PhantomData,
        }
    }

    pub fn create_task(
        &self,
        task: Task,
        task_id: u64,
        fixed_level: Option<u8>,
    ) -> MultiLevelTask<Task> {
        let elapsed = self.task_elapsed_map.get_elapsed(task_id);
        let level = fixed_level.unwrap_or_else(|| self.expected_level(elapsed.get()));
        MultiLevelTask {
            task,
            elapsed,
            level: AtomicU8::new(level),
            fixed_level,
        }
    }

    fn expected_level(&self, elapsed: Duration) -> u8 {
        match elapsed.as_micros() {
            0..=999 => 0,
            1000..=29_999 => 1,
            _ => 2,
        }
    }
}

impl<Task, T: AsRef<MultiLevelTask<Task>>> Clone for MultiLevelQueue<Task, T> {
    fn clone(&self) -> Self {
        Self {
            injectors: self.injectors.clone(),
            level_elapsed: self.level_elapsed.clone(),
            task_elapsed_map: self.task_elapsed_map.clone(),
            level_ratio: self.level_ratio.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<Task, T: AsRef<MultiLevelTask<Task>>> GlobalQueue for MultiLevelQueue<Task, T> {
    type Task = T;

    fn steal_batch_and_pop(
        &self,
        local_queue: &crossbeam_deque::Worker<SchedUnit<T>>,
    ) -> Steal<SchedUnit<T>> {
        let level = self.level_ratio.rand_level();
        match self.injectors[level].steal_batch_and_pop(local_queue) {
            s @ Steal::Success(_) | s @ Steal::Retry => return s,
            _ => {}
        }
        for queue in self
            .injectors
            .iter()
            .skip(level + 1)
            .chain(&*self.injectors)
            .take(LEVEL_NUM)
        {
            match queue.steal_batch_and_pop(local_queue) {
                s @ Steal::Success(_) | s @ Steal::Retry => return s,
                _ => {}
            }
        }
        Steal::Empty
    }

    fn push(&self, mut task: SchedUnit<T>) {
        let multi_level_task = task.task.as_ref();
        let elapsed = multi_level_task.elapsed.get();
        let level = multi_level_task
            .fixed_level
            .unwrap_or_else(|| self.expected_level(elapsed));
        multi_level_task.level.store(level, SeqCst);
        self.injectors[level as usize].push(task);
    }
}

/// The i-th value represents the chance ratio of L_i and L_{i+1}.
#[derive(Clone)]
struct LevelRatio(Arc<[AtomicU32; LEVEL_NUM - 1]>);

impl Default for LevelRatio {
    fn default() -> Self {
        Self(Arc::new([AtomicU32::new(32), AtomicU32::new(4)]))
    }
}

impl LevelRatio {
    fn rand_level(&self) -> usize {
        let mut rng = thread_rng();
        for (level, ratio) in self.0.iter().enumerate() {
            let ratio = ratio.load(SeqCst);
            if rng.gen_ratio(ratio, ratio.saturating_add(1)) {
                return level;
            }
        }
        return LEVEL_NUM - 1;
    }
}

#[derive(Clone, Default)]
pub struct LevelElapsed(Arc<[AtomicU64; LEVEL_NUM]>);

impl LevelElapsed {
    pub fn inc_level_by(&self, level: u8, t: Duration) {
        self.0[level as usize].fetch_add(t.as_micros() as u64, SeqCst);
    }
}

pub struct RunnerFactory<R, Task, T>
where
    R: super::super::RunnerFactory,
    <R as super::super::RunnerFactory>::Runner:
        super::super::Runner<GlobalQueue = MultiLevelQueue<Task, T>>,
    T: AsRef<MultiLevelTask<Task>>,
{
    inner: R,
}

impl<R, Task, T> super::super::RunnerFactory for RunnerFactory<R, Task, T>
where
    R: super::super::RunnerFactory,
    <R as super::super::RunnerFactory>::Runner:
        super::super::Runner<GlobalQueue = MultiLevelQueue<Task, T>>,
    T: AsRef<MultiLevelTask<Task>>,
{
    type Runner = Runner<<R as super::super::RunnerFactory>::Runner, Task, T>;

    fn produce(&mut self) -> Self::Runner {
        Runner {
            inner: self.inner.produce(),
        }
    }
}

pub struct Runner<R, Task, T>
where
    R: super::super::Runner<GlobalQueue = MultiLevelQueue<Task, T>>,
    T: AsRef<MultiLevelTask<Task>>,
{
    inner: R,
}

impl<R, Task, T> super::super::Runner for Runner<R, Task, T>
where
    R: super::super::Runner<GlobalQueue = MultiLevelQueue<Task, T>>,
    T: AsRef<MultiLevelTask<Task>>,
{
    type GlobalQueue = MultiLevelQueue<Task, T>;

    fn handle(&mut self, ctx: &mut PoolContext<Self::GlobalQueue>, task: T) -> bool {
        let multi_level_task = task.as_ref();
        let level = multi_level_task.level.load(SeqCst);
        let task_elapsed = multi_level_task.elapsed.clone();

        let begin = Instant::now();
        let res = self.inner.handle(ctx, task);
        let duration = begin.elapsed();

        task_elapsed.inc_by(duration);
        ctx.global_queue()
            .level_elapsed
            .inc_level_by(level, duration);

        res
    }
}

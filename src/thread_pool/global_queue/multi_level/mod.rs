mod stats;

use crate::thread_pool::{GlobalQueue, PoolContext, SchedUnit};
use stats::{TaskElapsed, TaskElapsedMap};

use crossbeam_deque::{Injector, Steal};
use init_with::InitWith;
use rand::prelude::*;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::SeqCst};
use std::sync::Arc;
use std::time::{Duration, Instant};

const LEVEL_NUM: usize = 3;

pub struct MultiLevelQueue<Task> {
    injectors: Arc<[Injector<SchedUnit<MultiLevelTask<Task>>>; LEVEL_NUM]>,
    level_stats: LevelStats,
    task_elapsed_map: TaskElapsedMap,
    level_ratio: LevelRatio,
}

pub struct MultiLevelTask<Task> {
    task: Task,
    elapsed: TaskElapsed,
    level: u8,
    fixed_level: Option<u8>,
}

impl<Task> AsMut<Task> for MultiLevelTask<Task> {
    fn as_mut(&mut self) -> &mut Task {
        &mut self.task
    }
}

impl<Task> MultiLevelQueue<Task> {
    pub fn new() -> Self {
        Self {
            injectors: Arc::new(
                <[Injector<SchedUnit<MultiLevelTask<Task>>>; LEVEL_NUM]>::init_with(|| {
                    Injector::new()
                }),
            ),
            level_stats: LevelStats::default(),
            task_elapsed_map: TaskElapsedMap::new(),
            level_ratio: LevelRatio::default(),
        }
    }

    pub fn create_task(
        &self,
        task: Task,
        task_id: u64,
        fixed_level: Option<u8>,
    ) -> MultiLevelTask<Task> {
        let elapsed = self.task_elapsed_map.get_elapsed(task_id);
        let level = fixed_level.unwrap_or_else(|| self.expected_level(elapsed.load(SeqCst)));
        MultiLevelTask {
            task,
            elapsed,
            level,
            fixed_level,
        }
    }

    fn expected_level(&self, elapsed: u64) -> u8 {
        match elapsed {
            0..=999 => 0,
            1000..=29_999 => 1,
            _ => 2,
        }
    }
}

impl<Task> Clone for MultiLevelQueue<Task> {
    fn clone(&self) -> Self {
        Self {
            injectors: self.injectors.clone(),
            level_stats: self.level_stats.clone(),
            task_elapsed_map: self.task_elapsed_map.clone(),
            level_ratio: self.level_ratio.clone(),
        }
    }
}

impl<Task> GlobalQueue for MultiLevelQueue<Task> {
    type Task = MultiLevelTask<Task>;

    fn steal_batch_and_pop(
        &self,
        local_queue: &crossbeam_deque::Worker<SchedUnit<Self::Task>>,
    ) -> Steal<SchedUnit<Self::Task>> {
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

    fn push(&self, mut task: SchedUnit<Self::Task>) {
        let elapsed = task.task.elapsed.load(SeqCst);
        let level = task
            .task
            .fixed_level
            .unwrap_or_else(|| self.expected_level(elapsed));
        task.task.level = level;
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
pub struct LevelStats(Arc<[AtomicU64; LEVEL_NUM]>);

impl LevelStats {
    pub fn inc_level_by(&self, level: u8, inc: u64) {
        self.0[level as usize].fetch_add(inc, SeqCst);
    }
}

pub struct RunnerFactory<R, Task>
where
    R: super::super::RunnerFactory,
    <R as super::super::RunnerFactory>::Runner:
        super::super::Runner<GlobalQueue = MultiLevelQueue<Task>>,
{
    inner: R,
}

impl<R, Task> super::super::RunnerFactory for RunnerFactory<R, Task>
where
    R: super::super::RunnerFactory,
    <R as super::super::RunnerFactory>::Runner:
        super::super::Runner<GlobalQueue = MultiLevelQueue<Task>>,
{
    type Runner = Runner<<R as super::super::RunnerFactory>::Runner, Task>;

    fn produce(&mut self) -> Self::Runner {
        Runner {
            inner: self.inner.produce(),
        }
    }
}

pub struct Runner<R, Task>
where
    R: super::super::Runner<GlobalQueue = MultiLevelQueue<Task>>,
{
    inner: R,
}

impl<R, Task> super::super::Runner for Runner<R, Task>
where
    R: super::super::Runner<GlobalQueue = MultiLevelQueue<Task>>,
{
    type GlobalQueue = MultiLevelQueue<Task>;

    fn handle(
        &mut self,
        ctx: &mut PoolContext<Self::GlobalQueue>,
        task: <Self::GlobalQueue as GlobalQueue>::Task,
    ) -> bool {
        let begin = Instant::now();
        let res = self.inner.handle(ctx, task);
        let duration = begin.elapsed();
        // TODO: record duration
        drop(duration);
        res
    }
}

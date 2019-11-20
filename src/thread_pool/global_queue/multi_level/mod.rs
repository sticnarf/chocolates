mod stats;

use crate::thread_pool::{GlobalQueue, PoolContext, SchedUnit};
use stats::{TaskElapsed, TaskElapsedMap};

use crossbeam_deque::{Injector, Steal};
use futures_timer::Delay;
use init_with::InitWith;
use rand::prelude::*;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{
    AtomicU32, AtomicU64, AtomicU8,
    Ordering::{Relaxed, SeqCst},
};
use std::sync::Arc;
use std::time::{Duration, Instant};

const LEVEL_NUM: usize = 3;
const ADJUST_RATIO_INTERVAL: Duration = Duration::from_secs(1);

pub struct MultiLevelQueue<Task, T = MultiLevelTask<Task>> {
    injectors: [Injector<SchedUnit<T>>; LEVEL_NUM],
    level_elapsed: LevelElapsed,
    task_elapsed_map: TaskElapsedMap,
    level_chance_ratio: LevelChanceRatio,
    target_ratio: TargetRatio,
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
            injectors: <[Injector<SchedUnit<T>>; LEVEL_NUM]>::init_with(|| Injector::new()),
            level_elapsed: LevelElapsed::default(),
            task_elapsed_map: TaskElapsedMap::new(),
            level_chance_ratio: LevelChanceRatio::default(),
            target_ratio: TargetRatio::default(),
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

    pub fn target_ratio(&self) -> TargetRatio {
        self.target_ratio.clone()
    }

    pub fn async_adjust_level_ratio(&self) -> impl Future<Output = ()> {
        let level_elapsed = self.level_elapsed.clone();
        let level_chance_ratio = self.level_chance_ratio.clone();
        let target_ratio = self.target_ratio.clone();
        async move {
            let mut last_elapsed = level_elapsed.load_all();
            loop {
                Delay::new(ADJUST_RATIO_INTERVAL).await;
                let curr_elapsed = level_elapsed.load_all();
                let diff_elapsed = <[f32; LEVEL_NUM]>::init_with_indices(|i| {
                    (curr_elapsed[i] - last_elapsed[i]) as f32
                });
                last_elapsed = curr_elapsed;

                let sum: f32 = diff_elapsed.iter().sum();
                if sum == 0.0 {
                    continue;
                }
                let curr_l0_ratio = diff_elapsed[0] / sum;
                let target_l0_ratio = target_ratio.get_l0_target();
                if curr_l0_ratio > target_l0_ratio + 0.05 {
                    let ratio01 = level_chance_ratio.0[0].load(SeqCst) as f32;
                    let new_ratio01 = u32::min((ratio01 * 1.6).round() as u32, MAX_L0_CHANCE_RATIO);
                    level_chance_ratio.0[0].store(new_ratio01, SeqCst);
                } else if curr_l0_ratio < target_l0_ratio - 0.05 {
                    let ratio01 = level_chance_ratio.0[0].load(SeqCst) as f32;
                    let new_ratio01 = u32::max((ratio01 / 1.6).round() as u32, MIN_L0_CHANCE_RATIO);
                    level_chance_ratio.0[0].store(new_ratio01, SeqCst);
                }
            }
        }
    }

    pub fn async_cleanup_stats(&self) -> impl Future<Output = ()> {
        self.task_elapsed_map.async_cleanup()
    }
}

impl<Task, T: AsRef<MultiLevelTask<Task>>> GlobalQueue for MultiLevelQueue<Task, T> {
    type Task = T;

    fn steal_batch_and_pop(
        &self,
        local_queue: &crossbeam_deque::Worker<SchedUnit<T>>,
    ) -> Steal<SchedUnit<T>> {
        let level = self.level_chance_ratio.rand_level();
        match self.injectors[level].steal_batch_and_pop(local_queue) {
            s @ Steal::Success(_) | s @ Steal::Retry => return s,
            _ => {}
        }
        for queue in self
            .injectors
            .iter()
            .skip(level + 1)
            .chain(&self.injectors)
            .take(LEVEL_NUM)
        {
            match queue.steal_batch_and_pop(local_queue) {
                s @ Steal::Success(_) | s @ Steal::Retry => return s,
                _ => {}
            }
        }
        Steal::Empty
    }

    fn push(&self, task: SchedUnit<T>) {
        let multi_level_task = task.task.as_ref();
        let elapsed = multi_level_task.elapsed.get();
        let level = multi_level_task
            .fixed_level
            .unwrap_or_else(|| self.expected_level(elapsed));
        multi_level_task.level.store(level, SeqCst);
        self.injectors[level as usize].push(task);
    }
}

const MAX_L0_CHANCE_RATIO: u32 = 256;
const MIN_L0_CHANCE_RATIO: u32 = 1;

/// The i-th value represents the chance ratio of L_i and Sum[L_k] (k > i).
#[derive(Clone)]
struct LevelChanceRatio(Arc<[AtomicU32; LEVEL_NUM - 1]>);

impl Default for LevelChanceRatio {
    fn default() -> Self {
        Self(Arc::new([AtomicU32::new(32), AtomicU32::new(4)]))
    }
}

impl LevelChanceRatio {
    fn rand_level(&self) -> usize {
        let mut rng = thread_rng();
        for (level, ratio) in self.0.iter().enumerate() {
            let ratio = ratio.load(Relaxed);
            if rng.gen_ratio(ratio, ratio.saturating_add(1)) {
                return level;
            }
        }
        return LEVEL_NUM - 1;
    }
}

/// The expected time percentage used by L0 tasks.
#[derive(Clone)]
pub struct TargetRatio(Arc<AtomicU8>);

impl Default for TargetRatio {
    fn default() -> Self {
        Self(Arc::new(AtomicU8::new(80)))
    }
}

impl TargetRatio {
    pub fn set_l0_target(&self, ratio: f32) {
        assert!(ratio >= 0.0 && ratio <= 1.0);
        self.0.store((100.0 * ratio) as u8, Relaxed);
    }

    pub fn get_l0_target(&self) -> f32 {
        self.0.load(Relaxed) as f32 / 100.0
    }
}

#[derive(Clone, Default)]
pub struct LevelElapsed(Arc<[AtomicU64; LEVEL_NUM]>);

impl LevelElapsed {
    pub fn inc_level_by(&self, level: u8, t: Duration) {
        self.0[level as usize].fetch_add(t.as_micros() as u64, Relaxed);
    }

    fn load_all(&self) -> [u64; LEVEL_NUM] {
        <[u64; LEVEL_NUM]>::init_with_indices(|i| self.0[i].load(Relaxed))
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

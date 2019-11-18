pub mod multi_level;

use super::SchedUnit;
use crossbeam_deque::Steal;

pub trait GlobalQueue {
    type Task;

    fn steal_batch_and_pop(
        &self,
        local_queue: &crossbeam_deque::Worker<SchedUnit<Self::Task>>,
    ) -> Steal<SchedUnit<Self::Task>>;

    fn push(&self, task: SchedUnit<Self::Task>);
}

impl<Task> GlobalQueue for crossbeam_deque::Injector<SchedUnit<Task>> {
    type Task = Task;

    fn steal_batch_and_pop(
        &self,
        local_queue: &crossbeam_deque::Worker<SchedUnit<Self::Task>>,
    ) -> Steal<SchedUnit<Self::Task>> {
        crossbeam_deque::Injector::steal_batch_and_pop(&self, local_queue)
    }

    fn push(&self, task: SchedUnit<Self::Task>) {
        crossbeam_deque::Injector::push(&self, task)
    }
}

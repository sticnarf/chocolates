use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref MULTI_LEVEL_POOL_RUNNING_TASK_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_multi_level_pool_pending_task_total",
        "Current multi-level pool pending + running tasks.",
        &["name"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_HANDLED_TASK_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_multi_level_pool_handled_task_total",
        "Total number of multi-level pool handled tasks.",
        &["name"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_LEVEL_POLL_TIMES: IntCounterVec = register_int_counter_vec!(
        "tikv_multi_level_pool_level_poll_times",
        "Poll times of tasks in each level",
        &["name", "level"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_LEVEL_ELAPSED: IntCounterVec = register_int_counter_vec!(
        "tikv_multi_level_pool_level_elapsed",
        "Running time of each level",
        &["name", "level"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_PROPORTIONS: IntGaugeVec = register_int_gauge_vec!(
        "tikv_multi_level_pool_proportions",
        "Sum proportions above the specific level (with 2^32 as denominator)",
        &["name", "level"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_PROPORTION_TARGET: GaugeVec = register_gauge_vec!(
        "tikv_multi_level_pool_proportion_target",
        "Level 0 target proportion",
        &["name"]
    )
    .unwrap();
    pub static ref MULTI_LEVEL_POOL_TASK_SOURCE: IntCounterVec = register_int_counter_vec!(
        "tikv_multi_level_pool_task_source",
        "Count of task source of each polling",
        &["name", "source"]
    )
    .unwrap();
}

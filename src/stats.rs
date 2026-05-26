use std::io::Write;
use std::time::Duration;

use anyhow::Result;
use scx_stats::prelude::*;
use scx_stats_derive::stat_doc;
use scx_stats_derive::Stats;
use serde::Deserialize;
use serde::Serialize;

#[stat_doc]
#[derive(Clone, Debug, Default, Serialize, Deserialize, Stats)]
#[stat(top)]
pub struct Metrics {
    #[stat(desc = "Number of online CPUs")]
    pub nr_cpus: u64,
    #[stat(desc = "Amount of tasks currently running")]
    pub nr_running: u64,
    #[stat(desc = "Amount of tasks queued to the user-space scheduler")]
    pub nr_queued: u64,
    #[stat(desc = "Amount of tasks in the user-space scheduler waiting to be dispatched")]
    pub nr_scheduled: u64,
    #[stat(desc = "Amount of user-space scheduler's page faults (should be always 0)")]
    pub nr_page_faults: u64,
    #[stat(desc = "Number of tasks classified as cold-start invocations")]
    pub nr_cold_start_tasks: u64,
    #[stat(desc = "Number of tasks classified as hot invocations")]
    pub nr_hot_invocation_tasks: u64,
    #[stat(desc = "Number of tasks classified as background")]
    pub nr_background_tasks: u64,
    #[stat(desc = "Number of tasks that received SLO-oriented priority boost")]
    pub nr_slo_boosted: u64,
    #[stat(desc = "Maximum pending tasks observed in the user-space scheduler")]
    pub max_pending: u64,
    #[stat(desc = "Number of task dispatched by the user-space scheduler")]
    pub nr_user_dispatches: u64,
    #[stat(desc = "Number of task dispatched directly by the kernel")]
    pub nr_kernel_dispatches: u64,
    #[stat(desc = "Number of cancelled dispatches")]
    pub nr_cancel_dispatches: u64,
    #[stat(desc = "Number of dispatches bounced to another DSQ")]
    pub nr_bounce_dispatches: u64,
    #[stat(desc = "Number of failed dispatches")]
    pub nr_failed_dispatches: u64,
    #[stat(desc = "Number of scheduler congestion events")]
    pub nr_sched_congested: u64,
    #[stat(desc = "Number of tasks classified via invocation metadata")]
    pub nr_metadata_classified: u64,
    #[stat(desc = "Number of tasks classified via heuristic fallback")]
    pub nr_heuristic_classified: u64,
    #[stat(desc = "Number of queued tasks refreshed from invocation metadata after enqueue")]
    pub nr_metadata_refreshed: u64,
    #[stat(desc = "Number of task enqueues where BPF observed invocation metadata hint")]
    pub nr_has_invocation_enqueues: u64,
    #[stat(desc = "Number of tasks assigned to the latency pool")]
    pub nr_pool_latency: u64,
    #[stat(desc = "Number of tasks assigned to the batch pool")]
    pub nr_pool_batch: u64,
    #[stat(desc = "Number of tasks dispatched through the tail guard pool")]
    pub nr_tail_guard_dispatches: u64,
    #[stat(desc = "Number of metadata deadline misses observed by the policy")]
    pub nr_slo_violations: u64,
    #[stat(desc = "Number of CPU migrations between scheduler pools")]
    pub nr_pool_migrations: u64,
    #[stat(desc = "Number of latency-pool tasks borrowed onto batch-pool CPUs")]
    pub nr_latency_pool_borrows: u64,
    #[stat(desc = "Number of batch-pool tasks borrowed onto latency-pool CPUs")]
    pub nr_batch_pool_borrows: u64,
}

impl Metrics {
    fn format<W: Write>(&self, w: &mut W) -> Result<()> {
        writeln!(
            w,
            "[{}] tasks -> r: {:>2}/{:<2} w: {:<2}/{:<2} max: {:<3} | invoc -> cold: {:<5} hot: {:<5} bg: {:<5} boost: {:<5} | pf: {:<5} | dispatch -> u: {:<5} k: {:<5} c: {:<5} b: {:<5} f: {:<5} | cg: {:<5}",
            crate::SCHEDULER_NAME,
            self.nr_running,
            self.nr_cpus,
            self.nr_queued,
            self.nr_scheduled,
            self.max_pending,
            self.nr_cold_start_tasks,
            self.nr_hot_invocation_tasks,
            self.nr_background_tasks,
            self.nr_slo_boosted,
            self.nr_page_faults,
            self.nr_user_dispatches,
            self.nr_kernel_dispatches,
            self.nr_cancel_dispatches,
            self.nr_bounce_dispatches,
            self.nr_failed_dispatches,
            self.nr_sched_congested,
        )?;
        if self.nr_metadata_classified > 0
            || self.nr_heuristic_classified > 0
            || self.nr_metadata_refreshed > 0
            || self.nr_has_invocation_enqueues > 0
        {
            writeln!(
                w,
                "  [classify] meta: {:<5} heur: {:<5} refresh: {:<5} bpf_meta: {:<5}",
                self.nr_metadata_classified,
                self.nr_heuristic_classified,
                self.nr_metadata_refreshed,
                self.nr_has_invocation_enqueues,
            )?;
        }
        if self.nr_pool_latency > 0
            || self.nr_pool_batch > 0
            || self.nr_tail_guard_dispatches > 0
            || self.nr_pool_migrations > 0
            || self.nr_latency_pool_borrows > 0
            || self.nr_batch_pool_borrows > 0
        {
            writeln!(
                w,
                "  [pools] lat: {:<5} batch: {:<5} tg: {:<5} mig: {:<5} borrow_l2b: {:<5} borrow_b2l: {:<5} slo_miss: {:<5}",
                self.nr_pool_latency,
                self.nr_pool_batch,
                self.nr_tail_guard_dispatches,
                self.nr_pool_migrations,
                self.nr_latency_pool_borrows,
                self.nr_batch_pool_borrows,
                self.nr_slo_violations,
            )?;
        }
        Ok(())
    }

    fn delta(&self, rhs: &Self) -> Self {
        Self {
            nr_cold_start_tasks: self.nr_cold_start_tasks - rhs.nr_cold_start_tasks,
            nr_hot_invocation_tasks: self.nr_hot_invocation_tasks - rhs.nr_hot_invocation_tasks,
            nr_background_tasks: self.nr_background_tasks - rhs.nr_background_tasks,
            nr_slo_boosted: self.nr_slo_boosted - rhs.nr_slo_boosted,
            nr_user_dispatches: self.nr_user_dispatches - rhs.nr_user_dispatches,
            nr_kernel_dispatches: self.nr_kernel_dispatches - rhs.nr_kernel_dispatches,
            nr_cancel_dispatches: self.nr_cancel_dispatches - rhs.nr_cancel_dispatches,
            nr_bounce_dispatches: self.nr_bounce_dispatches - rhs.nr_bounce_dispatches,
            nr_failed_dispatches: self.nr_failed_dispatches - rhs.nr_failed_dispatches,
            nr_sched_congested: self.nr_sched_congested - rhs.nr_sched_congested,
            nr_metadata_classified: self.nr_metadata_classified - rhs.nr_metadata_classified,
            nr_heuristic_classified: self.nr_heuristic_classified - rhs.nr_heuristic_classified,
            nr_metadata_refreshed: self.nr_metadata_refreshed - rhs.nr_metadata_refreshed,
            nr_has_invocation_enqueues: self.nr_has_invocation_enqueues
                - rhs.nr_has_invocation_enqueues,
            nr_pool_latency: self.nr_pool_latency - rhs.nr_pool_latency,
            nr_pool_batch: self.nr_pool_batch - rhs.nr_pool_batch,
            nr_tail_guard_dispatches: self.nr_tail_guard_dispatches - rhs.nr_tail_guard_dispatches,
            nr_slo_violations: self.nr_slo_violations - rhs.nr_slo_violations,
            nr_pool_migrations: self.nr_pool_migrations - rhs.nr_pool_migrations,
            nr_latency_pool_borrows: self.nr_latency_pool_borrows - rhs.nr_latency_pool_borrows,
            nr_batch_pool_borrows: self.nr_batch_pool_borrows - rhs.nr_batch_pool_borrows,
            ..self.clone()
        }
    }
}

pub fn server_data() -> StatsServerData<(), Metrics> {
    let open: Box<dyn StatsOpener<(), Metrics>> = Box::new(move |(req_ch, res_ch)| {
        req_ch.send(())?;
        let mut prev = res_ch.recv()?;

        let read: Box<dyn StatsReader<(), Metrics>> = Box::new(move |_args, (req_ch, res_ch)| {
            req_ch.send(())?;
            let cur = res_ch.recv()?;
            let delta = cur.delta(&prev);
            prev = cur;
            delta.to_json()
        });

        Ok(read)
    });

    StatsServerData::new()
        .add_meta(Metrics::meta())
        .add_ops("top", StatsOps { open, close: None })
}

pub fn monitor(intv: Duration) -> Result<()> {
    scx_utils::monitor_stats::<Metrics>(
        &[],
        intv,
        || false,
        |metrics| metrics.format(&mut std::io::stdout()),
    )
}

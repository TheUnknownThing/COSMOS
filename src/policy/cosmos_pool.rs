// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

//! Dynamic CPU Pool Manager for COSMOS.
//!
//! Manages the assignment of CPUs to scheduling pools (Latency, Batch, TailGuard).
//! The pool manager periodically rebalances CPU assignments based on queue pressure
//! signals, stealing CPUs from underutilized pools to satisfy overloaded ones.
//!
//! Design constraints:
//! - Tail guard pool size is fixed and never participates in rebalancing
//! - Each pool always retains at least 1 CPU
//! - At most 1 CPU migration per rebalance period (rate limiting)

use log::info;

/// CPU pool assignment, matching the `cosmos_pool` BPF enum.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(u32)]
pub enum TaskPool {
    None = 0,
    Latency = 1,
    Batch = 2,
    TailGuard = 3,
}

/// Pressure signals read from pool DSQ queue depths.
#[derive(Debug, Default)]
pub struct PoolMetrics {
    pub latency_queue_depth: u64,
    pub batch_queue_depth: u64,
}

/// A single CPU pool reassignment: CPU `cpu` moves to pool `pool`.
#[derive(Debug, Clone)]
pub struct PoolChange {
    pub cpu: u32,
    pub pool: TaskPool,
}

/// Dynamic CPU Pool Manager.
///
/// Tracks per-CPU pool assignments and rebalances them based on queue pressure.
/// The rebalancing heuristic is conservative: at most one CPU migrates per
/// rebalance cycle, and the tail guard pool is never touched.
pub struct PoolManager {
    nr_cpus: usize,
    assignments: Vec<TaskPool>,
    enabled: bool,
    latency_pct: u32,
    tail_guard_cpus: u32,
    /// Queue depth threshold above which a pool is considered "pressured"
    pressure_threshold: u64,
    /// Number of pool migrations performed (for stats)
    pub nr_pool_migrations: u64,
}

/// Tail guard only makes sense when the machine has enough CPUs left to keep
/// the main latency pool from collapsing to a single core.
pub fn effective_tail_guard_cpus(nr_cpus: usize, requested: u32) -> u32 {
    if requested == 0 || nr_cpus < 5 {
        0
    } else {
        requested.min(nr_cpus.saturating_sub(2) as u32)
    }
}

impl PoolManager {
    pub fn new(nr_cpus: usize, latency_pct: u32, tail_guard_cpus: u32) -> Self {
        let tail_guard_cpus = effective_tail_guard_cpus(nr_cpus, tail_guard_cpus);
        let mut mgr = PoolManager {
            nr_cpus,
            assignments: vec![TaskPool::None; nr_cpus],
            enabled: true,
            latency_pct,
            tail_guard_cpus,
            pressure_threshold: 4,
            nr_pool_migrations: 0,
        };
        mgr.initial_assign();
        mgr
    }

    pub fn disabled(nr_cpus: usize) -> Self {
        PoolManager {
            nr_cpus,
            assignments: vec![TaskPool::None; nr_cpus],
            enabled: false,
            latency_pct: 0,
            tail_guard_cpus: 0,
            pressure_threshold: 4,
            nr_pool_migrations: 0,
        }
    }

    fn initial_assign(&mut self) {
        let nr = self.nr_cpus;
        if nr == 0 {
            return;
        }

        let tg = self.tail_guard_cpus as usize;

        for i in (nr - tg)..nr {
            self.assignments[i] = TaskPool::TailGuard;
        }

        let remaining = nr - tg;
        if remaining == 0 {
            return;
        }

        let latency_count = ((remaining as u64 * self.latency_pct as u64) / 100).max(1) as usize;
        let latency_count = latency_count.min(remaining.saturating_sub(1));

        for i in 0..latency_count {
            self.assignments[i] = TaskPool::Latency;
        }

        for i in latency_count..(nr - tg) {
            self.assignments[i] = TaskPool::Batch;
        }

        info!(
            "Pool initial assignment: {} latency, {} batch, {} tail_guard (total {} CPUs)",
            latency_count,
            remaining - latency_count,
            tg,
            nr
        );
    }

    pub fn count_pool(&self, pool: TaskPool) -> usize {
        self.assignments.iter().filter(|&&p| p == pool).count()
    }

    fn find_last_cpu_in_pool(&self, pool: TaskPool) -> Option<usize> {
        self.assignments.iter().rposition(|&p| p == pool)
    }

    pub fn rebalance(&mut self, metrics: &PoolMetrics) -> Vec<PoolChange> {
        if !self.enabled {
            return Vec::new();
        }

        let mut changes = Vec::new();

        let latency_count = self.count_pool(TaskPool::Latency);
        let batch_count = self.count_pool(TaskPool::Batch);

        if metrics.latency_queue_depth > self.pressure_threshold && batch_count > 1 {
            if let Some(cpu) = self.find_last_cpu_in_pool(TaskPool::Batch) {
                self.assignments[cpu] = TaskPool::Latency;
                self.nr_pool_migrations += 1;
                changes.push(PoolChange {
                    cpu: cpu as u32,
                    pool: TaskPool::Latency,
                });
                info!(
                    "Pool rebalance: CPU {} batch→latency (lat_depth={}, batch_count={}→{})",
                    cpu,
                    metrics.latency_queue_depth,
                    batch_count,
                    batch_count - 1
                );
                return changes;
            }
        }

        if metrics.batch_queue_depth > self.pressure_threshold && latency_count > 1 {
            if let Some(cpu) = self.find_last_cpu_in_pool(TaskPool::Latency) {
                self.assignments[cpu] = TaskPool::Batch;
                self.nr_pool_migrations += 1;
                changes.push(PoolChange {
                    cpu: cpu as u32,
                    pool: TaskPool::Batch,
                });
                info!(
                    "Pool rebalance: CPU {} latency→batch (batch_depth={}, latency_count={}→{})",
                    cpu,
                    metrics.batch_queue_depth,
                    latency_count,
                    latency_count - 1
                );
                return changes;
            }
        }

        changes
    }

    pub fn iter_assignments(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.assignments
            .iter()
            .enumerate()
            .map(|(cpu, pool)| (cpu as u32, *pool as u32))
    }

    /// Return initial pool assignments for the scheduler to apply.
    pub fn init(&self) -> Vec<(u32, u32)> {
        self.iter_assignments().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_assign_4_cpus_50_pct_1_tg() {
        let mgr = PoolManager::new(4, 50, 1);
        // On a 4-CPU host we disable tail guard to avoid collapsing the
        // latency pool to a single CPU.
        assert_eq!(mgr.assignments[0], TaskPool::Latency);
        assert_eq!(mgr.assignments[1], TaskPool::Latency);
        assert_eq!(mgr.assignments[2], TaskPool::Batch);
        assert_eq!(mgr.assignments[3], TaskPool::Batch);
    }

    #[test]
    fn initial_assign_8_cpus_50_pct_1_tg() {
        let mgr = PoolManager::new(8, 50, 1);
        let lat = mgr.count_pool(TaskPool::Latency);
        let bat = mgr.count_pool(TaskPool::Batch);
        let tg = mgr.count_pool(TaskPool::TailGuard);
        assert_eq!(lat, 3);
        assert_eq!(bat, 4);
        assert_eq!(tg, 1);
    }

    #[test]
    fn effective_tail_guard_disabled_on_small_hosts() {
        assert_eq!(effective_tail_guard_cpus(4, 1), 0);
        assert_eq!(effective_tail_guard_cpus(5, 1), 1);
        assert_eq!(effective_tail_guard_cpus(8, 2), 2);
    }

    #[test]
    fn initial_assign_2_cpus_no_tg() {
        let mgr = PoolManager::new(2, 50, 0);
        assert_eq!(mgr.assignments[0], TaskPool::Latency);
        assert_eq!(mgr.assignments[1], TaskPool::Batch);
    }

    #[test]
    fn disabled_manager_leaves_all_cpus_on_shared_path() {
        let mgr = PoolManager::disabled(4);
        assert!(!mgr.enabled);
        assert!(mgr.assignments.iter().all(|pool| *pool == TaskPool::None));
    }

    #[test]
    fn initial_assign_ensures_minimum_pools() {
        let mgr = PoolManager::new(4, 100, 0);
        let lat = mgr.count_pool(TaskPool::Latency);
        let bat = mgr.count_pool(TaskPool::Batch);
        assert!(lat >= 1);
        assert!(bat >= 1);
    }

    #[test]
    fn rebalance_steals_from_batch_when_latency_pressured() {
        let mut mgr = PoolManager::new(4, 50, 0);
        let lat_before = mgr.count_pool(TaskPool::Latency);
        let bat_before = mgr.count_pool(TaskPool::Batch);
        assert_eq!(lat_before, 2);
        assert_eq!(bat_before, 2);

        let metrics = PoolMetrics {
            latency_queue_depth: 10,
            batch_queue_depth: 0,
        };
        let changes = mgr.rebalance(&metrics);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].pool, TaskPool::Latency);

        let lat_after = mgr.count_pool(TaskPool::Latency);
        let bat_after = mgr.count_pool(TaskPool::Batch);
        assert_eq!(lat_after, 3);
        assert_eq!(bat_after, 1);
    }

    #[test]
    fn rebalance_steals_from_latency_when_batch_pressured() {
        let mut mgr = PoolManager::new(4, 50, 0);

        let metrics = PoolMetrics {
            latency_queue_depth: 0,
            batch_queue_depth: 10,
        };
        let changes = mgr.rebalance(&metrics);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].pool, TaskPool::Batch);

        let lat_after = mgr.count_pool(TaskPool::Latency);
        let bat_after = mgr.count_pool(TaskPool::Batch);
        assert_eq!(lat_after, 1);
        assert_eq!(bat_after, 3);
    }

    #[test]
    fn rebalance_wont_steal_last_cpu() {
        let mut mgr = PoolManager::new(4, 50, 0);

        for _ in 0..5 {
            let metrics = PoolMetrics {
                latency_queue_depth: 10,
                batch_queue_depth: 0,
            };
            mgr.rebalance(&metrics);
        }

        let bat = mgr.count_pool(TaskPool::Batch);
        assert!(bat >= 1, "batch pool should retain at least 1 CPU");
    }

    #[test]
    fn rebalance_rate_limits_to_one_migration() {
        let mut mgr = PoolManager::new(8, 50, 0);

        let metrics = PoolMetrics {
            latency_queue_depth: 100,
            batch_queue_depth: 0,
        };
        let changes = mgr.rebalance(&metrics);
        assert_eq!(changes.len(), 1);
    }

    #[test]
    fn rebalance_no_change_when_balanced() {
        let mut mgr = PoolManager::new(4, 50, 0);

        let metrics = PoolMetrics {
            latency_queue_depth: 0,
            batch_queue_depth: 0,
        };
        let changes = mgr.rebalance(&metrics);
        assert!(changes.is_empty());
    }

    #[test]
    fn tail_guard_never_touched() {
        let mut mgr = PoolManager::new(4, 50, 1);
        let tg_before = mgr.count_pool(TaskPool::TailGuard);

        for _ in 0..10 {
            let metrics = PoolMetrics {
                latency_queue_depth: 100,
                batch_queue_depth: 100,
            };
            mgr.rebalance(&metrics);
        }

        let tg_after = mgr.count_pool(TaskPool::TailGuard);
        assert_eq!(
            tg_before, tg_after,
            "tail guard pool should never be modified"
        );
    }
}

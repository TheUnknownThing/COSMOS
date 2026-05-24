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
    pub tail_guard_queue_depth: u64,
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
    latency_pct: u32,
    tail_guard_cpus: u32,
    /// Queue depth threshold above which a pool is considered "pressured"
    pressure_threshold: u64,
    /// Number of pool migrations performed (for stats)
    pub nr_pool_migrations: u64,
}

impl PoolManager {
    /// Create a new PoolManager.
    ///
    /// # Arguments
    /// * `nr_cpus` — total number of online CPUs
    /// * `latency_pct` — target percentage of CPUs in the latency pool
    /// * `tail_guard_cpus` — fixed number of CPUs reserved for tail guard (0 = disabled)
    pub fn new(nr_cpus: usize, latency_pct: u32, tail_guard_cpus: u32) -> Self {
        let mut mgr = PoolManager {
            nr_cpus,
            assignments: vec![TaskPool::None; nr_cpus],
            latency_pct,
            tail_guard_cpus,
            pressure_threshold: 4,
            nr_pool_migrations: 0,
        };
        mgr.initial_assign();
        mgr
    }

    /// Perform initial CPU pool assignment.
    ///
    /// Layout strategy:
    /// - Last N CPUs → TailGuard (fixed)
    /// - First M CPUs → Latency (based on latency_pct of remaining)
    /// - Remaining CPUs → Batch
    ///
    /// Ensures each active pool has at least 1 CPU.
    fn initial_assign(&mut self) {
        let nr = self.nr_cpus;
        if nr == 0 {
            return;
        }

        // Clamp tail guard CPUs to leave at least 2 CPUs for latency+batch
        let tg = (self.tail_guard_cpus as usize).min(nr.saturating_sub(2));

        // Assign tail guard CPUs at the end
        for i in (nr - tg)..nr {
            self.assignments[i] = TaskPool::TailGuard;
        }

        let remaining = nr - tg;
        if remaining == 0 {
            return;
        }

        // Calculate latency pool size from percentage of non-tail-guard CPUs
        let latency_count = ((remaining as u64 * self.latency_pct as u64) / 100).max(1) as usize;
        let latency_count = latency_count.min(remaining.saturating_sub(1)); // leave at least 1 for batch

        // Assign latency CPUs at the start
        for i in 0..latency_count {
            self.assignments[i] = TaskPool::Latency;
        }

        // Assign remaining as batch
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

    /// Return the current pool assignments as a slice.
    pub fn assignments(&self) -> &[TaskPool] {
        &self.assignments
    }

    /// Count CPUs in a given pool.
    fn count_pool(&self, pool: TaskPool) -> usize {
        self.assignments.iter().filter(|&&p| p == pool).count()
    }

    /// Find the last CPU assigned to a given pool (for stealing).
    fn find_last_cpu_in_pool(&self, pool: TaskPool) -> Option<usize> {
        self.assignments
            .iter()
            .rposition(|&p| p == pool)
    }

    /// Find the first CPU assigned to a given pool (for stealing).
    fn find_first_cpu_in_pool(&self, pool: TaskPool) -> Option<usize> {
        self.assignments
            .iter()
            .position(|&p| p == pool)
    }

    /// Rebalance CPU pool assignments based on queue pressure signals.
    ///
    /// Heuristic:
    /// - If latency queue depth > threshold and batch pool has >1 CPU: steal one from batch → latency
    /// - If batch queue depth > threshold and latency pool has >1 CPU: steal one from latency → batch
    /// - Tail guard pool is never modified
    /// - At most 1 CPU migration per call (rate limiting)
    ///
    /// Returns a list of changes made (empty if no rebalancing needed).
    pub fn rebalance(&mut self, metrics: &PoolMetrics) -> Vec<PoolChange> {
        let mut changes = Vec::new();

        let latency_count = self.count_pool(TaskPool::Latency);
        let batch_count = self.count_pool(TaskPool::Batch);

        // Case 1: Latency pool is pressured, steal from batch
        if metrics.latency_queue_depth > self.pressure_threshold && batch_count > 1 {
            // Steal the last batch CPU → move to latency
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
                return changes; // rate limit: 1 migration per cycle
            }
        }

        // Case 2: Batch pool is pressured, steal from latency
        if metrics.batch_queue_depth > self.pressure_threshold && latency_count > 1 {
            // Steal the first latency CPU (from the end) → move to batch
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

        // Case 3: Both pools are balanced or there's nothing to steal
        changes
    }

    /// Apply all current pool assignments to the BPF cpu_pool_map.
    ///
    /// This writes every CPU's pool assignment. Call this after `initial_assign()`
    /// or after `rebalance()` returns changes.
    pub fn apply_all<F>(&self, mut update_fn: F)
    where
        F: FnMut(u32, u32),
    {
        for (cpu, &pool) in self.assignments.iter().enumerate() {
            update_fn(cpu as u32, pool as u32);
        }
    }

    /// Apply only the changed assignments to the BPF cpu_pool_map.
    pub fn apply_changes<F>(changes: &[PoolChange], mut update_fn: F)
    where
        F: FnMut(u32, u32),
    {
        for change in changes {
            update_fn(change.cpu, change.pool as u32);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_assign_4_cpus_50_pct_1_tg() {
        let mgr = PoolManager::new(4, 50, 1);
        // 4 CPUs, 1 tail guard → 3 remaining
        // 50% of 3 = 1.5 → 1 latency (min 1), 2 batch
        assert_eq!(mgr.assignments[0], TaskPool::Latency);
        assert_eq!(mgr.assignments[1], TaskPool::Batch);
        assert_eq!(mgr.assignments[2], TaskPool::Batch);
        assert_eq!(mgr.assignments[3], TaskPool::TailGuard);
    }

    #[test]
    fn initial_assign_8_cpus_50_pct_1_tg() {
        let mgr = PoolManager::new(8, 50, 1);
        // 8 CPUs, 1 tail guard → 7 remaining
        // 50% of 7 = 3.5 → 3 latency, 4 batch
        let lat = mgr.count_pool(TaskPool::Latency);
        let bat = mgr.count_pool(TaskPool::Batch);
        let tg = mgr.count_pool(TaskPool::TailGuard);
        assert_eq!(lat, 3);
        assert_eq!(bat, 4);
        assert_eq!(tg, 1);
    }

    #[test]
    fn initial_assign_2_cpus_no_tg() {
        let mgr = PoolManager::new(2, 50, 0);
        // 2 CPUs, 0 tail guard → 2 remaining
        // 50% of 2 = 1 latency, 1 batch
        assert_eq!(mgr.assignments[0], TaskPool::Latency);
        assert_eq!(mgr.assignments[1], TaskPool::Batch);
    }

    #[test]
    fn initial_assign_ensures_minimum_pools() {
        // With 100% latency, batch should still get 1 CPU
        let mgr = PoolManager::new(4, 100, 0);
        let lat = mgr.count_pool(TaskPool::Latency);
        let bat = mgr.count_pool(TaskPool::Batch);
        assert!(lat >= 1);
        assert!(bat >= 1);
    }

    #[test]
    fn rebalance_steals_from_batch_when_latency_pressured() {
        let mut mgr = PoolManager::new(4, 50, 0);
        // Initial: 2 latency, 2 batch
        let lat_before = mgr.count_pool(TaskPool::Latency);
        let bat_before = mgr.count_pool(TaskPool::Batch);
        assert_eq!(lat_before, 2);
        assert_eq!(bat_before, 2);

        // Pressure on latency
        let metrics = PoolMetrics {
            latency_queue_depth: 10,
            batch_queue_depth: 0,
            tail_guard_queue_depth: 0,
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
        // Initial: 2 latency, 2 batch

        // Pressure on batch
        let metrics = PoolMetrics {
            latency_queue_depth: 0,
            batch_queue_depth: 10,
            tail_guard_queue_depth: 0,
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

        // Drain batch down to 1 CPU by pressuring latency repeatedly
        for _ in 0..5 {
            let metrics = PoolMetrics {
                latency_queue_depth: 10,
                batch_queue_depth: 0,
                tail_guard_queue_depth: 0,
            };
            mgr.rebalance(&metrics);
        }

        // Batch should never go below 1
        let bat = mgr.count_pool(TaskPool::Batch);
        assert!(bat >= 1, "batch pool should retain at least 1 CPU");
    }

    #[test]
    fn rebalance_rate_limits_to_one_migration() {
        let mut mgr = PoolManager::new(8, 50, 0);

        let metrics = PoolMetrics {
            latency_queue_depth: 100,
            batch_queue_depth: 0,
            tail_guard_queue_depth: 0,
        };
        let changes = mgr.rebalance(&metrics);
        // Even with extreme pressure, only 1 CPU migrates per cycle
        assert_eq!(changes.len(), 1);
    }

    #[test]
    fn rebalance_no_change_when_balanced() {
        let mut mgr = PoolManager::new(4, 50, 0);

        let metrics = PoolMetrics {
            latency_queue_depth: 0,
            batch_queue_depth: 0,
            tail_guard_queue_depth: 0,
        };
        let changes = mgr.rebalance(&metrics);
        assert!(changes.is_empty());
    }

    #[test]
    fn tail_guard_never_touched() {
        let mut mgr = PoolManager::new(4, 50, 1);
        let tg_before = mgr.count_pool(TaskPool::TailGuard);

        // Apply extreme pressure on both pools
        for _ in 0..10 {
            let metrics = PoolMetrics {
                latency_queue_depth: 100,
                batch_queue_depth: 100,
                tail_guard_queue_depth: 0,
            };
            mgr.rebalance(&metrics);
        }

        let tg_after = mgr.count_pool(TaskPool::TailGuard);
        assert_eq!(tg_before, tg_after, "tail guard pool should never be modified");
    }

    #[test]
    fn apply_all_writes_every_cpu() {
        let mgr = PoolManager::new(4, 50, 1);
        let mut written = Vec::new();
        mgr.apply_all(|cpu, pool| {
            written.push((cpu, pool));
        });
        assert_eq!(written.len(), 4);
    }
}

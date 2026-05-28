// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

//! Dynamic CPU Pool Manager for COSMOS.

use log::info;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(u32)]
pub enum TaskPool {
    None = 0,
    Latency = 1,
    Batch = 2,
    TailGuard = 3,
}

#[derive(Debug, Default)]
pub struct PoolMetrics {
    pub latency_queue_depth: u64,
    pub batch_queue_depth: u64,
    pub latency_cpu_count: u64,
    pub batch_cpu_count: u64,
    pub latency_home_depth: u64,
    pub batch_home_depth: u64,
    pub latency_empty_samples: u64,
    pub batch_empty_samples: u64,
    pub samples: u64,
}

#[derive(Debug, Clone)]
pub struct PoolChange {
    pub cpu: u32,
    pub pool: TaskPool,
}

pub struct PoolManager {
    nr_cpus: usize,
    pub(crate) assignments: Vec<TaskPool>,
    enabled: bool,
    latency_pct: u32,
    tail_guard_cpus: u32,
    pressure_per_cpu: u64,
    min_main_pool_cpus: usize,
    pub nr_pool_migrations: u64,
}

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
            pressure_per_cpu: 1,
            min_main_pool_cpus: 1,
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
            pressure_per_cpu: 1,
            min_main_pool_cpus: 1,
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
        self.min_main_pool_cpus = (remaining / 8).max(1);
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

    fn pool_is_pressured(&self, queue_depth: u64, cpu_count: u64) -> bool {
        if cpu_count == 0 {
            return false;
        }
        queue_depth > cpu_count.saturating_mul(self.pressure_per_cpu)
    }

    fn migrate_one(&mut self, from: TaskPool, to: TaskPool) -> Vec<PoolChange> {
        let mut changes = Vec::new();
        if let Some(cpu) = self.find_last_cpu_in_pool(from) {
            self.assignments[cpu] = to;
            self.nr_pool_migrations += 1;
            changes.push(PoolChange {
                cpu: cpu as u32,
                pool: to,
            });
        }
        changes
    }

    pub fn rebalance(&mut self, metrics: &PoolMetrics) -> Vec<PoolChange> {
        if !self.enabled {
            return Vec::new();
        }
        let latency_count = self.count_pool(TaskPool::Latency);
        let batch_count = self.count_pool(TaskPool::Batch);
        let latency_pressured =
            self.pool_is_pressured(metrics.latency_queue_depth, metrics.latency_cpu_count);
        let batch_pressured =
            self.pool_is_pressured(metrics.batch_queue_depth, metrics.batch_cpu_count);
        if latency_pressured && !batch_pressured && batch_count > self.min_main_pool_cpus {
            let changes = self.migrate_one(TaskPool::Batch, TaskPool::Latency);
            info!(
                "Pool rebalance: CPU {} batch->latency (lat_depth={}, batch_count={}->{})",
                self.find_last_cpu_in_pool(TaskPool::Latency).unwrap_or(0),
                metrics.latency_queue_depth,
                batch_count,
                batch_count - 1
            );
            return changes;
        }
        if batch_pressured && !latency_pressured && latency_count > self.min_main_pool_cpus {
            let changes = self.migrate_one(TaskPool::Latency, TaskPool::Batch);
            info!(
                "Pool rebalance: CPU {} latency->batch (batch_depth={}, latency_count={}->{})",
                self.find_last_cpu_in_pool(TaskPool::Batch).unwrap_or(0),
                metrics.batch_queue_depth,
                latency_count,
                latency_count - 1
            );
            return changes;
        }
        Vec::new()
    }

    pub fn iter_assignments(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.assignments
            .iter()
            .enumerate()
            .map(|(cpu, pool)| (cpu as u32, *pool as u32))
    }

    pub fn init(&self) -> Vec<(u32, u32)> {
        self.iter_assignments().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn metrics(l: u64, b: u64, lc: u64, bc: u64) -> PoolMetrics {
        PoolMetrics {
            latency_queue_depth: l,
            batch_queue_depth: b,
            latency_cpu_count: lc,
            batch_cpu_count: bc,
            latency_home_depth: l,
            batch_home_depth: b,
            latency_empty_samples: if l == 0 { 1 } else { 0 },
            batch_empty_samples: if b == 0 { 1 } else { 0 },
            samples: 1,
        }
    }
    #[test]
    fn initial_assign_4() {
        let m = PoolManager::new(4, 50, 1);
        assert_eq!(m.assignments[0], TaskPool::Latency);
        assert_eq!(m.assignments[1], TaskPool::Latency);
        assert_eq!(m.assignments[2], TaskPool::Batch);
        assert_eq!(m.assignments[3], TaskPool::Batch);
    }
    #[test]
    fn initial_assign_8() {
        let m = PoolManager::new(8, 50, 1);
        assert_eq!(m.count_pool(TaskPool::Latency), 3);
        assert_eq!(m.count_pool(TaskPool::Batch), 4);
        assert_eq!(m.count_pool(TaskPool::TailGuard), 1);
    }
    #[test]
    fn effective_tg() {
        assert_eq!(effective_tail_guard_cpus(4, 1), 0);
        assert_eq!(effective_tail_guard_cpus(5, 1), 1);
        assert_eq!(effective_tail_guard_cpus(8, 2), 2);
    }
    #[test]
    fn assign_2() {
        let m = PoolManager::new(2, 50, 0);
        assert_eq!(m.assignments[0], TaskPool::Latency);
        assert_eq!(m.assignments[1], TaskPool::Batch);
    }
    #[test]
    fn disabled() {
        let m = PoolManager::disabled(4);
        assert!(!m.enabled);
        assert!(m.assignments.iter().all(|p| *p == TaskPool::None));
    }
    #[test]
    fn min_pools() {
        let m = PoolManager::new(4, 100, 0);
        assert!(m.count_pool(TaskPool::Latency) >= 1);
        assert!(m.count_pool(TaskPool::Batch) >= 1);
    }
    #[test]
    fn steal_batch() {
        let mut m = PoolManager::new(4, 50, 0);
        let ch = m.rebalance(&metrics(3, 0, 2, 2));
        assert_eq!(ch.len(), 1);
        assert_eq!(ch[0].pool, TaskPool::Latency);
        assert_eq!(m.count_pool(TaskPool::Latency), 3);
        assert_eq!(m.count_pool(TaskPool::Batch), 1);
    }
    #[test]
    fn steal_latency() {
        let mut m = PoolManager::new(4, 50, 0);
        let ch = m.rebalance(&metrics(0, 3, 2, 2));
        assert_eq!(ch.len(), 1);
        assert_eq!(ch[0].pool, TaskPool::Batch);
        assert_eq!(m.count_pool(TaskPool::Latency), 1);
        assert_eq!(m.count_pool(TaskPool::Batch), 3);
    }
    #[test]
    fn wont_steal_last() {
        let mut m = PoolManager::new(4, 50, 0);
        m.rebalance(&metrics(3, 0, 2, 2));
        assert!(m.rebalance(&metrics(5, 0, 3, 1)).is_empty());
        assert!(m.count_pool(TaskPool::Batch) >= 1);
    }
    #[test]
    fn both_pressured() {
        let mut m = PoolManager::new(4, 50, 0);
        assert!(m.rebalance(&metrics(3, 3, 2, 2)).is_empty());
    }
    #[test]
    fn balanced() {
        let mut m = PoolManager::new(4, 50, 0);
        assert!(m.rebalance(&metrics(0, 0, 2, 2)).is_empty());
    }
    #[test]
    fn tg_untouched() {
        let mut m = PoolManager::new(8, 50, 1);
        let tg = m.count_pool(TaskPool::TailGuard);
        for _ in 0..10 {
            m.rebalance(&metrics(100, 100, 3, 4));
        }
        assert_eq!(m.count_pool(TaskPool::TailGuard), tg);
    }
}

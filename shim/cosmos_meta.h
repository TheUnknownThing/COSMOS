/*
 * COSMOS Invocation Metadata Shim — Public C API
 *
 * This library allows FaaS runtimes to annotate their process with
 * invocation-level metadata (deadline, SLO class, cold-start flag)
 * that the COSMOS scheduler reads via a pinned BPF hash map.
 *
 * Usage:
 *   1. Link against libcosmos_meta.so or LD_PRELOAD it.
 *   2. Call cosmos_invocation_start() at invocation begin.
 *   3. Call cosmos_invocation_end() when the invocation completes.
 *
 * The LD_PRELOAD wrapper (cosmos_preload.c) automates this via
 * constructor/destructor using environment variables.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

#ifndef COSMOS_META_H
#define COSMOS_META_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Register invocation metadata for the calling process (tgid).
 *
 * @deadline_ns   Absolute CLOCK_MONOTONIC deadline in nanoseconds.
 *                Pass 0 for "no deadline" (still benefits from SLO class).
 * @slo_class     0 = latency-critical, 1 = standard, 2 = batch.
 * @is_cold_start 1 if this is a cold start invocation, 0 otherwise.
 * @invocation_id Opaque correlation ID for tracing.
 *
 * Returns 0 on success, -errno on failure.
 */
int cosmos_invocation_start(uint64_t deadline_ns, uint32_t slo_class,
                            uint32_t is_cold_start, uint64_t invocation_id);

/*
 * Remove invocation metadata for the calling process (tgid).
 *
 * Should be called when the invocation completes so the scheduler
 * falls back to heuristic classification for this process.
 *
 * Returns 0 on success, -errno on failure.
 */
int cosmos_invocation_end(void);

#ifdef __cplusplus
}
#endif

#endif /* COSMOS_META_H */

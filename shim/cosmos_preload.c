/*
 * COSMOS LD_PRELOAD Wrapper
 *
 * When LD_PRELOAD'd, this library automatically registers invocation
 * metadata on process startup (constructor) and cleans it up on exit
 * (destructor).
 *
 * Configuration via environment variables:
 *   COSMOS_DEADLINE_NS   — absolute CLOCK_MONOTONIC deadline (default: 0)
 *   COSMOS_SLO_CLASS     — 0=latency-critical, 1=standard, 2=batch (default: 0)
 *   COSMOS_COLD_START    — 1 if cold start (default: 0)
 *   COSMOS_INVOCATION_ID — opaque correlation ID (default: getpid())
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>

#include "cosmos_meta.h"

static void __attribute__((constructor)) cosmos_preload_init(void)
{
    const char *env;
    uint64_t deadline_ns = 0;
    uint32_t slo_class = 0;
    uint32_t is_cold_start = 0;
    uint64_t invocation_id = (uint64_t)getpid();
    int ret;

    env = getenv("COSMOS_DEADLINE_NS");
    if (env)
        deadline_ns = (uint64_t)strtoull(env, NULL, 10);

    env = getenv("COSMOS_SLO_CLASS");
    if (env)
        slo_class = (uint32_t)strtoul(env, NULL, 10);

    env = getenv("COSMOS_COLD_START");
    if (env)
        is_cold_start = (uint32_t)strtoul(env, NULL, 10);

    env = getenv("COSMOS_INVOCATION_ID");
    if (env)
        invocation_id = (uint64_t)strtoull(env, NULL, 10);

    ret = cosmos_invocation_start(deadline_ns, slo_class,
                                  is_cold_start, invocation_id);
    if (ret < 0) {
        /* Non-fatal: scheduler might not be running */
        fprintf(stderr, "cosmos_preload: invocation_start failed: %d "
                        "(scheduler may not be running)\n", ret);
    }
}

static void __attribute__((destructor)) cosmos_preload_fini(void)
{
    cosmos_invocation_end();
}

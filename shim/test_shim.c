/*
 * COSMOS Shim Standalone Test
 *
 * Verifies that the shim library can write to and delete from the
 * pinned invocation_meta BPF map.
 *
 * Prerequisites:
 *   - COSMOS scheduler must be running (so the map exists)
 *   - Must be run as root (BPF map access requires CAP_BPF)
 *
 * Usage:
 *   cd shim && make test_shim
 *   sudo ./test_shim
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "cosmos_meta.h"

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

int main(void)
{
    int ret;
    uint64_t deadline;

    printf("COSMOS shim test (pid=%d)\n", getpid());

    /* Test 1: Write metadata with a 100ms deadline */
    deadline = now_ns() + 100000000ULL;  /* 100ms from now */
    ret = cosmos_invocation_start(deadline, 0, 1, 42);
    if (ret < 0) {
        fprintf(stderr, "FAIL: cosmos_invocation_start returned %d\n", ret);
        fprintf(stderr, "  Is the COSMOS scheduler running?\n");
        fprintf(stderr, "  Does /sys/fs/bpf/cosmos/invocation_meta exist?\n");
        return 1;
    }
    printf("PASS: cosmos_invocation_start (deadline=%lu, slo=0, cold=1, id=42)\n",
           (unsigned long)deadline);

    /* Give the scheduler a moment to read it */
    usleep(10000);  /* 10ms */

    /* Test 2: Remove metadata */
    ret = cosmos_invocation_end();
    if (ret < 0) {
        fprintf(stderr, "FAIL: cosmos_invocation_end returned %d\n", ret);
        return 1;
    }
    printf("PASS: cosmos_invocation_end\n");

    /* Test 3: Double-remove should fail gracefully (key not found) */
    ret = cosmos_invocation_end();
    if (ret == 0) {
        printf("NOTE: cosmos_invocation_end on missing key returned 0 (may vary by kernel)\n");
    } else {
        printf("PASS: cosmos_invocation_end on missing key returned %d (expected)\n", ret);
    }

    printf("\nAll tests passed.\n");
    return 0;
}

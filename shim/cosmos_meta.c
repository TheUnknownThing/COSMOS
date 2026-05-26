/*
 * COSMOS Invocation Metadata Shim — Implementation
 *
 * Writes/deletes entries in the pinned BPF hash map at
 * /sys/fs/bpf/cosmos/invocation_events using raw bpf() syscalls.
 *
 * This is the drain-queue: the scheduler reads and deletes entries,
 * populates the Registry, and sets the 1-bit has_invocation hint
 * that BPF reads to route tasks to userspace.
 *
 * No dependency on libbpf — this is a lightweight shim that can be
 * LD_PRELOAD'd into any process with minimal overhead (~1us).
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <linux/bpf.h>

#include "cosmos_meta.h"

/* Must match struct invocation_meta_val in intf.h */
struct invocation_meta_val {
    uint64_t deadline_ns;
    uint32_t slo_class;
    uint32_t is_cold_start;
    uint64_t invocation_id;
};

/* BPF syscall wrapper */
static inline int sys_bpf(int cmd, union bpf_attr *attr, unsigned int size)
{
    return (int)syscall(__NR_bpf, cmd, attr, size);
}

/* Get a file descriptor for the pinned BPF map */
static int open_pinned_map(void)
{
    union bpf_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.pathname = (uint64_t)(unsigned long)"/sys/fs/bpf/cosmos/invocation_events";
    attr.bpf_fd = 0;
    attr.file_flags = 0;

    return sys_bpf(BPF_OBJ_GET, &attr, sizeof(attr));
}

int cosmos_invocation_start(uint64_t deadline_ns, uint32_t slo_class,
                            uint32_t is_cold_start, uint64_t invocation_id)
{
    int map_fd, ret;
    uint32_t key;
    struct invocation_meta_val val;
    union bpf_attr attr;

    map_fd = open_pinned_map();
    if (map_fd < 0)
        return -errno;

    /* Key is tgid — on Linux, getpid() returns the tgid */
    key = (uint32_t)getpid();

    val.deadline_ns = deadline_ns;
    val.slo_class = slo_class;
    val.is_cold_start = is_cold_start;
    val.invocation_id = invocation_id;

    memset(&attr, 0, sizeof(attr));
    attr.map_fd = map_fd;
    attr.key = (uint64_t)(unsigned long)&key;
    attr.value = (uint64_t)(unsigned long)&val;
    attr.flags = BPF_ANY;  /* insert or update */

    ret = sys_bpf(BPF_MAP_UPDATE_ELEM, &attr, sizeof(attr));
    if (ret < 0)
        ret = -errno;

    close(map_fd);
    return ret;
}

int cosmos_invocation_end(void)
{
    int map_fd, ret;
    uint32_t key;
    union bpf_attr attr;

    map_fd = open_pinned_map();
    if (map_fd < 0)
        return -errno;

    key = (uint32_t)getpid();

    memset(&attr, 0, sizeof(attr));
    attr.map_fd = map_fd;
    attr.key = (uint64_t)(unsigned long)&key;

    ret = sys_bpf(BPF_MAP_DELETE_ELEM, &attr, sizeof(attr));
    if (ret < 0)
        ret = -errno;

    close(map_fd);
    return ret;
}

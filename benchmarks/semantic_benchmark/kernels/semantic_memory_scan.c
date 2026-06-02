#include "semantic_kernel_common.h"

static void usage(const char *argv0) {
    fprintf(stderr, "usage: %s --target-us N [--working-set BYTES]\n", argv0);
}

int main(int argc, char **argv) {
    uint64_t target_us = 0;
    size_t working_set = 1024 * 1024;
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--target-us") == 0 && i + 1 < argc) {
            target_us = parse_u64(argv[++i], "--target-us");
        } else if (strcmp(argv[i], "--working-set") == 0 && i + 1 < argc) {
            working_set = (size_t)parse_u64(argv[++i], "--working-set");
        } else {
            usage(argv[0]);
            return 2;
        }
    }
    if (target_us == 0 || working_set == 0) {
        usage(argv[0]);
        return 2;
    }
    uint8_t *memory = malloc(working_set);
    if (memory == NULL) {
        perror("malloc");
        return 2;
    }
    memset(memory, 1, working_set);
    uint64_t state = 0x123456789abcdefULL;
    uint64_t start = now_us();
    uint64_t deadline = start + target_us;
    uint64_t iterations = 0;
    while (now_us() < deadline) {
        memory_step(memory, working_set, &state);
        ++iterations;
    }
    uint64_t elapsed = now_us() - start;
    printf(
        "{\"mode\":\"memory_scan\",\"target_us\":%llu,\"elapsed_us\":%llu,\"iterations\":%llu,\"sink\":%llu}\n",
        (unsigned long long)target_us,
        (unsigned long long)elapsed,
        (unsigned long long)iterations,
        (unsigned long long)sink_u64
    );
    free(memory);
    return 0;
}


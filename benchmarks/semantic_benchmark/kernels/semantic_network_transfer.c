#include "semantic_kernel_common.h"

static void usage(const char *argv0) {
    fprintf(stderr, "usage: %s --target-us N [--transfer-size BYTES]\n", argv0);
}

int main(int argc, char **argv) {
    uint64_t target_us = 0;
    size_t transfer_size = 4096;
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--target-us") == 0 && i + 1 < argc) {
            target_us = parse_u64(argv[++i], "--target-us");
        } else if (strcmp(argv[i], "--transfer-size") == 0 && i + 1 < argc) {
            transfer_size = (size_t)parse_u64(argv[++i], "--transfer-size");
        } else {
            usage(argv[0]);
            return 2;
        }
    }
    if (target_us == 0 || transfer_size == 0) {
        usage(argv[0]);
        return 2;
    }
    int sockets[2] = {-1, -1};
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0) {
        perror("socketpair");
        return 2;
    }
    uint8_t *transfer = malloc(transfer_size);
    if (transfer == NULL) {
        perror("malloc");
        return 2;
    }
    memset(transfer, 7, transfer_size);
    uint64_t state = 0x123456789abcdefULL;
    uint64_t start = now_us();
    uint64_t deadline = start + target_us;
    uint64_t iterations = 0;
    while (now_us() < deadline) {
        network_step(sockets, transfer, transfer_size, &state);
        ++iterations;
    }
    uint64_t elapsed = now_us() - start;
    printf(
        "{\"mode\":\"network_transfer\",\"target_us\":%llu,\"elapsed_us\":%llu,\"iterations\":%llu,\"sink\":%llu}\n",
        (unsigned long long)target_us,
        (unsigned long long)elapsed,
        (unsigned long long)iterations,
        (unsigned long long)sink_u64
    );
    close(sockets[0]);
    close(sockets[1]);
    free(transfer);
    return 0;
}


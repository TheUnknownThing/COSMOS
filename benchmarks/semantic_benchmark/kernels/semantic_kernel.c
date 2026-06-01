#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

static volatile uint64_t sink_u64;

static uint64_t now_us(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        perror("clock_gettime");
        exit(2);
    }
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

static uint64_t parse_u64(const char *text, const char *name) {
    char *end = NULL;
    errno = 0;
    unsigned long long value = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        fprintf(stderr, "invalid %s: %s\n", name, text);
        exit(2);
    }
    return (uint64_t)value;
}

static uint64_t next_rand(uint64_t x) {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    return x;
}

static void cpu_step(uint64_t *state) {
    uint64_t x = *state;
    for (int i = 0; i < 2048; ++i) {
        x = next_rand(x + (uint64_t)i + 0x9e3779b97f4a7c15ULL);
    }
    *state = x;
    sink_u64 ^= x;
}

static void memory_step(uint8_t *buffer, size_t bytes, uint64_t *state) {
    if (bytes == 0) {
        cpu_step(state);
        return;
    }
    uint64_t x = *state;
    for (size_t offset = 0; offset < bytes; offset += 64) {
        x = next_rand(x + buffer[offset]);
        buffer[offset] = (uint8_t)x;
    }
    *state = x;
    sink_u64 ^= x;
}

static void write_all(int fd, const uint8_t *buffer, size_t bytes) {
    size_t done = 0;
    while (done < bytes) {
        ssize_t wrote = write(fd, buffer + done, bytes - done);
        if (wrote < 0) {
            perror("write");
            exit(2);
        }
        done += (size_t)wrote;
    }
}

static void read_all(int fd, uint8_t *buffer, size_t bytes) {
    size_t done = 0;
    while (done < bytes) {
        ssize_t got = read(fd, buffer + done, bytes - done);
        if (got < 0) {
            perror("read");
            exit(2);
        }
        if (got == 0) {
            fprintf(stderr, "short read\n");
            exit(2);
        }
        done += (size_t)got;
    }
}

static int make_temp_fd(void) {
    char path[] = "/tmp/cosmos-semantic-kernel.XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) {
        perror("mkstemp");
        exit(2);
    }
    unlink(path);
    return fd;
}

static void io_step(int fd, uint8_t *buffer, size_t chunk, uint64_t *state) {
    uint64_t x = *state;
    for (size_t i = 0; i < chunk; ++i) {
        x = next_rand(x + i);
        buffer[i] = (uint8_t)x;
    }
    if (lseek(fd, 0, SEEK_SET) < 0) {
        perror("lseek");
        exit(2);
    }
    write_all(fd, buffer, chunk);
    if (lseek(fd, 0, SEEK_SET) < 0) {
        perror("lseek");
        exit(2);
    }
    read_all(fd, buffer, chunk);
    *state = x + buffer[chunk / 2];
    sink_u64 ^= *state;
}

static void network_step(int sockets[2], uint8_t *buffer, size_t chunk, uint64_t *state) {
    uint64_t x = *state;
    for (size_t i = 0; i < chunk; ++i) {
        x = next_rand(x + i);
        buffer[i] = (uint8_t)x;
    }
    for (size_t offset = 0; offset < chunk;) {
        size_t piece = chunk - offset;
        if (piece > 4096) {
            piece = 4096;
        }
        write_all(sockets[0], buffer + offset, piece);
        read_all(sockets[1], buffer + offset, piece);
        offset += piece;
    }
    *state = x + buffer[chunk / 2];
    sink_u64 ^= *state;
}

static void usage(const char *argv0) {
    fprintf(
        stderr,
        "usage: %s --mode cpu|memory|io|network|balanced --target-us N "
        "[--working-set BYTES] [--transfer-size BYTES]\n",
        argv0
    );
}

int main(int argc, char **argv) {
    const char *mode = NULL;
    uint64_t target_us = 0;
    size_t working_set = 1024 * 1024;
    size_t transfer_size = 4096;

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            mode = argv[++i];
        } else if (strcmp(argv[i], "--target-us") == 0 && i + 1 < argc) {
            target_us = parse_u64(argv[++i], "--target-us");
        } else if (strcmp(argv[i], "--working-set") == 0 && i + 1 < argc) {
            working_set = (size_t)parse_u64(argv[++i], "--working-set");
        } else if (strcmp(argv[i], "--transfer-size") == 0 && i + 1 < argc) {
            transfer_size = (size_t)parse_u64(argv[++i], "--transfer-size");
        } else {
            usage(argv[0]);
            return 2;
        }
    }

    if (mode == NULL || target_us == 0 || transfer_size == 0) {
        usage(argv[0]);
        return 2;
    }

    uint8_t *memory = NULL;
    if (strcmp(mode, "memory") == 0 || strcmp(mode, "balanced") == 0) {
        memory = malloc(working_set);
        if (memory == NULL) {
            perror("malloc");
            return 2;
        }
        memset(memory, 1, working_set);
    }

    uint8_t *transfer = malloc(transfer_size);
    if (transfer == NULL) {
        perror("malloc");
        return 2;
    }
    memset(transfer, 7, transfer_size);

    int fd = -1;
    if (strcmp(mode, "io") == 0 || strcmp(mode, "balanced") == 0) {
        fd = make_temp_fd();
    }

    int sockets[2] = {-1, -1};
    if (strcmp(mode, "network") == 0 || strcmp(mode, "balanced") == 0) {
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0) {
            perror("socketpair");
            return 2;
        }
    }

    uint64_t state = 0x123456789abcdefULL;
    uint64_t start = now_us();
    uint64_t deadline = start + target_us;
    uint64_t iterations = 0;

    while (now_us() < deadline) {
        if (strcmp(mode, "cpu") == 0) {
            cpu_step(&state);
        } else if (strcmp(mode, "memory") == 0) {
            memory_step(memory, working_set, &state);
        } else if (strcmp(mode, "io") == 0) {
            io_step(fd, transfer, transfer_size, &state);
        } else if (strcmp(mode, "network") == 0) {
            network_step(sockets, transfer, transfer_size, &state);
        } else if (strcmp(mode, "balanced") == 0) {
            cpu_step(&state);
            memory_step(memory, working_set, &state);
            io_step(fd, transfer, transfer_size, &state);
            network_step(sockets, transfer, transfer_size, &state);
        } else {
            fprintf(stderr, "unsupported mode: %s\n", mode);
            return 2;
        }
        ++iterations;
    }

    uint64_t elapsed = now_us() - start;
    printf(
        "{\"mode\":\"%s\",\"target_us\":%llu,\"elapsed_us\":%llu,"
        "\"iterations\":%llu,\"sink\":%llu}\n",
        mode,
        (unsigned long long)target_us,
        (unsigned long long)elapsed,
        (unsigned long long)iterations,
        (unsigned long long)sink_u64
    );

    if (sockets[0] >= 0) {
        close(sockets[0]);
    }
    if (sockets[1] >= 0) {
        close(sockets[1]);
    }
    if (fd >= 0) {
        close(fd);
    }
    free(transfer);
    free(memory);
    return 0;
}

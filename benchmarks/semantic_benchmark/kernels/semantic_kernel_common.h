#pragma once

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

static void control_step(uint64_t *state) {
    uint64_t x = *state;
    for (int i = 0; i < 128; ++i) {
        x = next_rand(x + (uint64_t)i + 0x632be59bd9b4e019ULL);
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

static void sleep_us(uint64_t us) {
    struct timespec req;
    req.tv_sec = (time_t)(us / 1000000ULL);
    req.tv_nsec = (long)((us % 1000000ULL) * 1000ULL);
    while (nanosleep(&req, &req) != 0) {
        if (errno != EINTR) {
            perror("nanosleep");
            exit(2);
        }
    }
}

static void passive_wait_step(uint64_t deadline_us, uint64_t *state) {
    uint64_t now = now_us();
    if (now >= deadline_us) {
        return;
    }
    uint64_t remaining = deadline_us - now;
    if (remaining > 4000ULL) {
        uint64_t sleep_for = remaining - 1000ULL;
        sleep_us(sleep_for);
    } else {
        control_step(state);
    }
}

static void mixed_pipeline_step(
    int fd,
    int sockets[2],
    uint8_t *memory,
    size_t working_set,
    uint8_t *transfer,
    size_t transfer_size,
    uint64_t *state
) {
    io_step(fd, transfer, transfer_size, state);
    network_step(sockets, transfer, transfer_size, state);
    memory_step(memory, working_set, state);
    cpu_step(state);
}

static void workflow_fanout_step(uint64_t *state, uint64_t deadline_us, int fanout) {
    for (int i = 0; i < fanout; ++i) {
        control_step(state);
        if (i % 2 == 0) {
            sleep_us(50);
        }
    }
    passive_wait_step(deadline_us, state);
}


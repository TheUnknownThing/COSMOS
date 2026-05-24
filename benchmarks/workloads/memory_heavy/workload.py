#!/usr/bin/env python3

import argparse
import time


def main() -> int:
    parser = argparse.ArgumentParser(description="Synthetic memory-heavy benchmark.")
    parser.add_argument("--duration-ms", type=int, default=25)
    parser.add_argument("--buffer-mb", type=int, default=64)
    args = parser.parse_args()

    deadline = time.monotonic_ns() + args.duration_ms * 1_000_000
    size = args.buffer_mb * 1024 * 1024
    buf = bytearray((idx * 7) % 251 for idx in range(size))
    stride = 4096

    while time.monotonic_ns() < deadline:
        for offset in range(0, size, stride):
            buf[offset] = (buf[offset] + offset) & 0xFF
        snapshot = bytes(buf[: stride * 32])
        _ = sum(snapshot)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

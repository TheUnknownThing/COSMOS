#!/usr/bin/env python3

import argparse
import os
import tempfile
import time


def burn_cpu(iterations: int) -> int:
    total = 0
    for idx in range(iterations):
        total += (idx * 13) % 97
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description="Synthetic CPU + disk IO workload.")
    parser.add_argument("--duration-ms", type=int, default=20)
    parser.add_argument("--chunk-bytes", type=int, default=65536)
    args = parser.parse_args()

    deadline = time.monotonic_ns() + args.duration_ms * 1_000_000
    payload = bytes((idx % 251 for idx in range(args.chunk_bytes)))

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "payload.bin")
        while time.monotonic_ns() < deadline:
            burn_cpu(50_000)
            with open(path, "wb") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            with open(path, "rb") as fh:
                fh.read()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

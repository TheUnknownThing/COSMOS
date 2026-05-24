#!/usr/bin/env python3

import argparse
import os
import time
import zlib


def main() -> int:
    parser = argparse.ArgumentParser(description="Synthetic compression-oriented benchmark.")
    parser.add_argument("--duration-ms", type=int, default=20)
    parser.add_argument("--payload-kb", type=int, default=512)
    args = parser.parse_args()

    payload = os.urandom(args.payload_kb * 1024)
    deadline = time.monotonic_ns() + args.duration_ms * 1_000_000

    while time.monotonic_ns() < deadline:
        compressed = zlib.compress(payload, level=6)
        decompressed = zlib.decompress(compressed)
        if decompressed != payload:
            raise RuntimeError("compression roundtrip mismatch")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

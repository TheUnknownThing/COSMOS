#!/usr/bin/env python3

import argparse
import time


def main() -> int:
    parser = argparse.ArgumentParser(description="Short sleep calibration workload.")
    parser.add_argument("--duration-ms", type=int, default=2)
    args = parser.parse_args()
    time.sleep(args.duration_ms / 1000.0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

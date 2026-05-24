#!/usr/bin/env python3

import argparse
import time


def burn(duration_ms: int, matrix_size: int) -> int:
    deadline = time.monotonic_ns() + duration_ms * 1_000_000
    a = [[(row + col) % 7 for col in range(matrix_size)] for row in range(matrix_size)]
    b = [[(row * 3 + col) % 11 for col in range(matrix_size)] for row in range(matrix_size)]
    iterations = 0

    while time.monotonic_ns() < deadline:
        total = 0
        for row in range(matrix_size):
            for col in range(matrix_size):
                acc = 0
                for inner in range(matrix_size):
                    acc += a[row][inner] * b[inner][col]
                total += acc
        iterations += total % 17

    return iterations


def main() -> int:
    parser = argparse.ArgumentParser(description="Synthetic CPU burst benchmark.")
    parser.add_argument("--duration-ms", type=int, default=10)
    parser.add_argument("--matrix-size", type=int, default=24)
    args = parser.parse_args()

    burn(args.duration_ms, args.matrix_size)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

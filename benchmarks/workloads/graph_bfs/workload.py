#!/usr/bin/env python3

import argparse
import collections
import time


def build_graph(nodes: int, degree: int) -> list[list[int]]:
    graph: list[list[int]] = []
    for node in range(nodes):
        graph.append([((node + offset) % nodes) for offset in range(1, degree + 1)])
    return graph


def main() -> int:
    parser = argparse.ArgumentParser(description="Synthetic graph BFS benchmark.")
    parser.add_argument("--duration-ms", type=int, default=20)
    parser.add_argument("--nodes", type=int, default=4000)
    parser.add_argument("--degree", type=int, default=6)
    args = parser.parse_args()

    graph = build_graph(args.nodes, args.degree)
    deadline = time.monotonic_ns() + args.duration_ms * 1_000_000
    start = 0

    while time.monotonic_ns() < deadline:
        visited = {start}
        queue = collections.deque([start])
        while queue:
            node = queue.popleft()
            for neighbor in graph[node]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)
        start = (start + 97) % args.nodes

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

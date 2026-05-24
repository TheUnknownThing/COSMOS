#!/usr/bin/env python3

import argparse
import socket
import threading
import time


def main() -> int:
    parser = argparse.ArgumentParser(description="Synthetic loopback network-heavy benchmark.")
    parser.add_argument("--duration-ms", type=int, default=25)
    parser.add_argument("--chunk-bytes", type=int, default=262144)
    args = parser.parse_args()

    stop_event = threading.Event()
    payload = b"x" * args.chunk_bytes

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def server() -> None:
        conn, _ = listener.accept()
        with conn:
            while not stop_event.is_set():
                try:
                    data = conn.recv(args.chunk_bytes)
                except OSError:
                    break
                if not data:
                    break
                try:
                    conn.sendall(data)
                except OSError:
                    break

    thread = threading.Thread(target=server, daemon=True)
    thread.start()

    deadline = time.monotonic_ns() + args.duration_ms * 1_000_000
    client = socket.create_connection(("127.0.0.1", port))
    with client:
        while time.monotonic_ns() < deadline:
            client.sendall(payload)
            remaining = len(payload)
            while remaining > 0:
                chunk = client.recv(min(remaining, 65536))
                if not chunk:
                    remaining = 0
                    break
                remaining -= len(chunk)

    stop_event.set()
    listener.close()
    thread.join(timeout=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

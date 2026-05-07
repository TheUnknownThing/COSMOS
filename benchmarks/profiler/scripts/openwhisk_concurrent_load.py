#!/usr/bin/env python3
import argparse
import base64
import json
import os
import queue
import ssl
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Send a synchronized concurrent request burst to an OpenWhisk action."
    )
    parser.add_argument("--apihost", required=True)
    parser.add_argument("--auth", required=True)
    parser.add_argument("--action", required=True)
    parser.add_argument("--out-dir", default="benchmarks/runs")
    parser.add_argument("--run-dir")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--timeout-s", type=float, default=90.0)
    parser.add_argument("--warmup-requests", type=int, default=1)
    parser.add_argument("--param", action="append", default=[], help="key=value; value may be JSON")
    parser.add_argument("--param-file", action="append", default=[])
    parser.add_argument("--blocking", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--result", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--insecure", action="store_true")
    return parser.parse_args()


def now_ns():
    return time.time_ns()


def sanitize(value):
    return "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in value)


def load_payload(args):
    payload = {}
    for raw_path in args.param_file:
        with open(raw_path, "r", encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, dict):
            raise SystemExit(f"--param-file must contain a JSON object: {raw_path}")
        payload.update(value)
    for raw in args.param:
        if "=" not in raw:
            raise SystemExit(f"--param must be key=value: {raw}")
        key, value = raw.split("=", 1)
        try:
            payload[key] = json.loads(value)
        except json.JSONDecodeError:
            payload[key] = value
    return payload


def action_url(args):
    action = args.action.strip("/")
    quoted = "/".join(urllib.parse.quote(part, safe="") for part in action.split("/"))
    query = urllib.parse.urlencode(
        {
            "blocking": str(args.blocking).lower(),
            "result": str(args.result).lower(),
        }
    )
    return f"{args.apihost.rstrip('/')}/api/v1/namespaces/_/actions/{quoted}?{query}"


def make_opener(args):
    if not args.insecure:
        return urllib.request.build_opener()
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return urllib.request.build_opener(urllib.request.HTTPSHandler(context=context))


def invoke(opener, url, auth, payload, request_id, timeout_s):
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    token = base64.b64encode(auth.encode("utf-8")).decode("ascii")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
            "X-Cosmos-Request-Id": str(request_id),
        },
    )
    start_ns = now_ns()
    status = 0
    response_body = b""
    error = ""
    try:
        with opener.open(request, timeout=timeout_s) as response:
            status = response.status
            response_body = response.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        response_body = exc.read()
        error = str(exc)
    except Exception as exc:
        error = type(exc).__name__ + ": " + str(exc)
    end_ns = now_ns()

    activation_id = ""
    parsed = None
    if response_body:
        try:
            parsed = json.loads(response_body.decode("utf-8"))
            if isinstance(parsed, dict):
                activation_id = str(parsed.get("activationId") or parsed.get("activation_id") or "")
        except Exception:
            parsed = None

    return {
        "request_id": request_id,
        "start_ns": start_ns,
        "end_ns": end_ns,
        "latency_ms": (end_ns - start_ns) / 1_000_000.0,
        "status": status,
        "ok": 200 <= status < 300,
        "activation_id": activation_id,
        "response_bytes": len(response_body),
        "error": error,
        "response": parsed,
    }


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def write_json(path, value):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")


def run_warmup(args, opener, url, payload):
    for request_id in range(args.warmup_requests):
        result = invoke(opener, url, args.auth, payload, f"warmup-{request_id}", args.timeout_s)
        if not result["ok"]:
            print(f"warmup failed: status={result['status']} error={result['error']}", file=sys.stderr)


def main():
    args = parse_args()
    if args.requests <= 0:
        raise SystemExit("--requests must be positive")
    if args.concurrency <= 0:
        raise SystemExit("--concurrency must be positive")

    out_dir = Path(args.out_dir)
    if args.run_dir:
        run_dir = Path(args.run_dir)
    else:
        run_dir = out_dir / f"openwhisk-concurrent-{sanitize(args.action)}-{now_ns()}-{os.getpid()}"
    run_dir.mkdir(parents=True, exist_ok=False)

    payload = load_payload(args)
    url = action_url(args)
    opener = make_opener(args)
    write_json(
        run_dir / "config.json",
        {
            "apihost": args.apihost,
            "action": args.action,
            "requests": args.requests,
            "concurrency": args.concurrency,
            "timeout_s": args.timeout_s,
            "warmup_requests": args.warmup_requests,
            "blocking": args.blocking,
            "result": args.result,
            "payload": payload,
            "url": url,
        },
    )

    run_warmup(args, opener, url, payload)

    pending = queue.Queue()
    for request_id in range(args.requests):
        pending.put(request_id)
    start = threading.Event()
    write_lock = threading.Lock()
    ready = threading.Barrier(min(args.concurrency, args.requests) + 1)
    results = []

    def worker():
        local_opener = make_opener(args)
        ready.wait()
        start.wait()
        while True:
            try:
                request_id = pending.get_nowait()
            except queue.Empty:
                return
            result = invoke(local_opener, url, args.auth, payload, request_id, args.timeout_s)
            with write_lock:
                results.append(result)
                with open(run_dir / "requests.jsonl", "a", encoding="utf-8") as handle:
                    handle.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
            pending.task_done()

    threads = [
        threading.Thread(target=worker, name=f"openwhisk-load-{idx}", daemon=True)
        for idx in range(min(args.concurrency, args.requests))
    ]
    for thread in threads:
        thread.start()
    ready.wait()
    burst_start_ns = now_ns()
    start.set()
    for thread in threads:
        thread.join()
    burst_end_ns = now_ns()

    latencies = [item["latency_ms"] for item in results]
    successes = sum(1 for item in results if item["ok"])
    status_counts = {}
    for item in results:
        status_counts[str(item["status"])] = status_counts.get(str(item["status"]), 0) + 1
    elapsed_s = max((burst_end_ns - burst_start_ns) / 1_000_000_000.0, 0.000001)
    summary = {
        "run_dir": str(run_dir),
        "requests": args.requests,
        "completed": len(results),
        "successes": successes,
        "errors": len(results) - successes,
        "status_counts": status_counts,
        "concurrency": args.concurrency,
        "burst_start_ns": burst_start_ns,
        "burst_end_ns": burst_end_ns,
        "elapsed_s": elapsed_s,
        "throughput_rps": len(results) / elapsed_s,
        "latency_ms": {
            "min": min(latencies) if latencies else None,
            "mean": statistics.fmean(latencies) if latencies else None,
            "p50": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
            "max": max(latencies) if latencies else None,
        },
    }
    write_json(run_dir / "summary.json", summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    if successes == 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

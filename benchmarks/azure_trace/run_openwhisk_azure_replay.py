#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUT_DIR = SCRIPT_DIR / "results" / "openwhisk-azure-replay"


@dataclass(frozen=True)
class ReplayInvocation:
    event_id: str
    invocation_id: int
    at_ms: float
    function_id: str
    profile_id: str
    workload: str
    action: str
    target_duration_ms: int
    deadline_us: int
    slo_class: int
    profile_hints: dict[str, Any]
    payload: dict[str, Any] | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay Azure-derived invocation schedules against OpenWhisk actions."
    )
    parser.add_argument("--replay", type=Path)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument(
        "--action-map",
        action="append",
        default=[],
        help="Mapping key=value. Key may be profile_id, kernel/workload, or '*'.",
    )
    parser.add_argument("--wsk", default="wsk")
    parser.add_argument("--wsk-arg", action="append", default=[])
    parser.add_argument("--blocking", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--result", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--timeout-s", type=float, default=120.0)
    parser.add_argument("--accept-async-completion", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--poll-accepted", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--activation-poll-timeout-s", type=float, default=180.0)
    parser.add_argument("--activation-poll-interval-s", type=float, default=1.0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--calibrate",
        action="store_true",
        help="Run isolated action calibration instead of trace replay.",
    )
    parser.add_argument(
        "--calibration-target-ms",
        action="append",
        type=int,
        default=[],
        help="Candidate target duration in ms. Defaults cover the profile duration classes.",
    )
    parser.add_argument("--calibration-repetitions", type=int, default=3)
    parser.add_argument("--calibration-output", type=Path)
    parser.add_argument(
        "--event-bridge-port",
        type=int,
        default=0,
        help=(
            "Optional COSMOS event bridge port. Sends local_start/local_end around "
            "the wsk process so scheduler metadata records profile fields."
        ),
    )
    parser.add_argument(
        "--metadata-target",
        choices=("wsk", "openwhisk-container"),
        default="openwhisk-container",
        help=(
            "Target for COSMOS metadata. openwhisk-container tags the action "
            "container process tree; wsk tags only the client process."
        ),
    )
    parser.add_argument(
        "--container-name-map",
        action="append",
        default=[],
        help=(
            "Mapping key=value for OpenWhisk action container names or IDs. "
            "Key may be profile_id, kernel/workload, action, or '*'."
        ),
    )
    return parser.parse_args()


def now_ns() -> int:
    return time.monotonic_ns()


def wall_ns() -> int:
    return time.time_ns()


def parse_action_map(raw_items: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for raw in raw_items:
        if "=" not in raw:
            raise SystemExit(f"--action-map must be key=value: {raw}")
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            raise SystemExit(f"--action-map must be key=value: {raw}")
        mapping[key] = value
    return mapping


def action_container_name(action: str) -> str:
    return "wsk0_*_" + re.sub(r"[^A-Za-z0-9]", "", action).lower()


def load_replay(path: Path, action_map: dict[str, str], limit: int | None) -> list[ReplayInvocation]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    invocations = raw.get("invocations")
    if not isinstance(invocations, list):
        raise ValueError(f"{path} must contain an invocations array")

    result: list[ReplayInvocation] = []
    for idx, item in enumerate(invocations, start=1):
        if limit is not None and len(result) >= limit:
            break
        workload = str(item.get("workload") or item.get("kernel") or "")
        profile_id = str(item.get("profile_id") or workload)
        action = (
            action_map.get(profile_id)
            or action_map.get(workload)
            or action_map.get("*")
            or workload
        )
        if not action:
            raise ValueError(f"no OpenWhisk action mapping for invocation {idx}: {item}")
        target_duration_ms = int(
            item.get("target_duration_ms") or item.get("duration_ms") or 250
        )
        deadline_us = int(item.get("deadline_us") or max(target_duration_ms * 2000, 5000))
        function_id = str(item.get("function_id") or item.get("function_hash") or workload)
        payload = item.get("sebs_payload")
        if payload is None:
            payload = item.get("payload")
        if payload is not None and not isinstance(payload, dict):
            raise ValueError(f"payload must be an object for invocation {idx}: {item}")
        result.append(
            ReplayInvocation(
                event_id=str(item.get("event_id") or f"replay-{idx:08d}"),
                invocation_id=int(item.get("invocation_id") or idx),
                at_ms=float(item.get("at_ms") or 0.0),
                function_id=function_id,
                profile_id=profile_id,
                workload=workload,
                action=action,
                target_duration_ms=target_duration_ms,
                deadline_us=deadline_us,
                slo_class=int(item.get("slo_class", 1)),
                profile_hints=dict(item.get("profile_hints") or {}),
                payload=payload,
            )
        )
    result.sort(key=lambda item: (item.at_ms, item.invocation_id))
    return result


def make_payload(invocation: ReplayInvocation) -> dict[str, Any]:
    if invocation.payload is not None:
        return invocation.payload
    return {
        "event_id": invocation.event_id,
        "invocation_id": invocation.invocation_id,
        "function_id": invocation.function_id,
        "profile_id": invocation.profile_id,
        "kernel": invocation.workload,
        "target_duration_ms": invocation.target_duration_ms,
        "deadline_us": invocation.deadline_us,
        "slo_class": invocation.slo_class,
        "profile_hints": invocation.profile_hints,
    }


def wsk_command(
    args: argparse.Namespace,
    invocation: ReplayInvocation,
    payload_path: Path,
) -> list[str]:
    command = [
        args.wsk,
        *args.wsk_arg,
        "action",
        "invoke",
    ]
    if args.blocking:
        command.append("--blocking")
    if args.result:
        command.append("--result")
    command.extend([invocation.action, "--param-file", str(payload_path)])
    return command


def parse_activation_id(stdout: str) -> str:
    text = stdout.strip()
    if not text:
        return ""
    match = re.search(r"\bwith id ([0-9a-fA-F]{32})\b", text)
    if match:
        return match.group(1)
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        json_start = text.find("{")
        value = None
        if json_start >= 0:
            try:
                value = json.loads(text[json_start:])
            except json.JSONDecodeError:
                value = None
    if isinstance(value, dict):
        for key in ("activationId", "activation_id"):
            if value.get(key):
                return str(value[key])
        response = value.get("response")
        if isinstance(response, dict) and response.get("activationId"):
            return str(response["activationId"])
    for line in text.splitlines():
        parts = line.strip().split()
        if parts and parts[-1].isalnum() and "activation" in line.lower():
            return parts[-1]
    return ""


def parse_prefixed_json(stdout: str) -> dict[str, Any] | None:
    text = stdout.strip()
    start = text.find("{")
    if start < 0:
        return None
    try:
        value = json.loads(text[start:])
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def activation_get_command(args: argparse.Namespace, activation_id: str) -> list[str]:
    return [args.wsk, *args.wsk_arg, "activation", "get", activation_id]


def activation_is_done(stdout: str) -> bool:
    value = parse_prefixed_json(stdout)
    return bool(value and "end" in value and value.get("response") is not None)


def poll_activation_until_done(
    args: argparse.Namespace,
    activation_id: str,
    run_dir: Path,
) -> str:
    deadline = time.monotonic() + max(0.0, args.activation_poll_timeout_s)
    output_path = run_dir / "activations" / f"{activation_id}.json"
    last_status = ""
    while time.monotonic() <= deadline:
        completed = subprocess.run(
            activation_get_command(args, activation_id),
            text=True,
            capture_output=True,
            timeout=min(30.0, max(1.0, args.activation_poll_interval_s + 5.0)),
            check=False,
        )
        last_status = f"exit:{completed.returncode}"
        if completed.stdout:
            output_path.write_text(completed.stdout, encoding="utf-8")
            if completed.returncode == 0 and activation_is_done(completed.stdout):
                return "complete"
        if completed.returncode not in (0, 1):
            last_status = f"poll-error:{completed.returncode}:{completed.stderr.strip()[:200]}"
        time.sleep(max(0.05, args.activation_poll_interval_s))
    return f"poll-error:timeout:{last_status}"


def send_event_bridge_event(port: int, event: dict[str, Any]) -> str:
    payload = json.dumps(event, separators=(",", ":")).encode("utf-8") + b"\n"
    with socket.create_connection(("127.0.0.1", port), timeout=10.0) as conn:
        conn.sendall(payload)
        raw = b""
        while not raw.endswith(b"\n"):
            chunk = conn.recv(1024)
            if not chunk:
                break
            raw += chunk
    return raw.decode("utf-8", errors="replace").strip()


def metadata_fields(invocation: ReplayInvocation, activation_id: str) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "activation_id": activation_id,
        "timeout_ms": max(1, invocation.deadline_us // 1000),
        "estimated_duration_ms": invocation.target_duration_ms,
        "slo_class": invocation.slo_class,
        "action_name": invocation.action,
        "cold_start": False,
        "profile_id": invocation.profile_id,
    }
    if invocation.profile_hints:
        fields["profile_hints"] = invocation.profile_hints
    return fields


def metadata_start_event(invocation: ReplayInvocation, activation_id: str, tgid: int) -> dict[str, Any]:
    return {
        "type": "local_start",
        **metadata_fields(invocation, activation_id),
        "tgid": tgid,
        "kind": "openwhisk-wsk",
    }


def container_metadata_start_event(
    invocation: ReplayInvocation,
    activation_id: str,
    container_id: str,
) -> dict[str, Any]:
    return {
        "type": "start",
        **metadata_fields(invocation, activation_id),
        "container_id": container_id,
        "kind": "openwhisk-container",
    }


def resolve_container_ref(invocation: ReplayInvocation, container_map: dict[str, str]) -> str:
    explicit = (
        container_map.get(invocation.profile_id)
        or container_map.get(invocation.workload)
        or container_map.get(invocation.action)
        or container_map.get("*")
    )
    return explicit or action_container_name(invocation.action)


def run_invocation(
    args: argparse.Namespace,
    invocation: ReplayInvocation,
    run_dir: Path,
    container_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    scheduled_ns = now_ns()
    payload_path = run_dir / "payloads" / f"{invocation.invocation_id}.json"
    payload_path.write_text(
        json.dumps(make_payload(invocation), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    command = wsk_command(args, invocation, payload_path)
    command_display = " ".join(command)
    if args.dry_run:
        return {
            "event_id": invocation.event_id,
            "invocation_id": invocation.invocation_id,
            "function_id": invocation.function_id,
            "profile_id": invocation.profile_id,
            "workload": invocation.workload,
            "action": invocation.action,
            "activation_id": "",
            "scheduled_at_ms": invocation.at_ms,
            "target_duration_ms": invocation.target_duration_ms,
            "deadline_us": invocation.deadline_us,
            "slo_class": invocation.slo_class,
            "scheduled_monotonic_ns": scheduled_ns,
            "submit_monotonic_ns": scheduled_ns,
            "completion_monotonic_ns": scheduled_ns,
            "latency_ms": 0.0,
            "exit_status": 0,
            "ok": True,
            "metadata_status": "dry-run",
            "stdout_path": "",
            "stderr_path": "",
            "command": command_display,
        }

    stdout_path = run_dir / "stdout" / f"{invocation.invocation_id}.json"
    stderr_path = run_dir / "stderr" / f"{invocation.invocation_id}.txt"
    submit_ns = now_ns()
    with stdout_path.open("w", encoding="utf-8") as stdout_fh, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr_fh:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=os.environ.copy(),
        )
        activation_guess = f"{invocation.event_id}-{process.pid}"
        metadata_status = ""
        metadata_end_event: dict[str, Any] | None = None
        if args.event_bridge_port:
            try:
                if args.metadata_target == "openwhisk-container":
                    container_id = resolve_container_ref(invocation, container_map or {})
                    start_event = container_metadata_start_event(
                        invocation, activation_guess, container_id
                    )
                    metadata_end_event = {
                        "type": "end",
                        "activation_id": activation_guess,
                        "container_id": container_id,
                    }
                else:
                    start_event = metadata_start_event(
                        invocation, activation_guess, process.pid
                    )
                    metadata_end_event = {
                        "type": "local_end",
                        "activation_id": activation_guess,
                        "tgid": process.pid,
                    }
                metadata_status = send_event_bridge_event(args.event_bridge_port, start_event)
            except Exception as exc:
                metadata_status = f"start-error:{type(exc).__name__}:{exc}"
        try:
            stdout, stderr = process.communicate(timeout=args.timeout_s)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            completion_ns = now_ns()
            stdout_fh.write(stdout)
            stderr_fh.write(stderr)
            activation_id = parse_activation_id(stdout) or activation_guess
            return {
                "event_id": invocation.event_id,
                "invocation_id": invocation.invocation_id,
                "function_id": invocation.function_id,
                "profile_id": invocation.profile_id,
                "workload": invocation.workload,
                "action": invocation.action,
                "activation_id": activation_id,
                "scheduled_at_ms": invocation.at_ms,
                "target_duration_ms": invocation.target_duration_ms,
                "deadline_us": invocation.deadline_us,
                "slo_class": invocation.slo_class,
                "scheduled_monotonic_ns": scheduled_ns,
                "submit_monotonic_ns": submit_ns,
                "completion_monotonic_ns": completion_ns,
                "latency_ms": (completion_ns - submit_ns) / 1_000_000.0,
                "exit_status": -9,
                "ok": False,
                "metadata_status": metadata_status,
                "activation_status": "",
                "poll_status": "",
                "error": "timeout",
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
                "command": command_display,
            }
        completion_ns = now_ns()
        stdout_fh.write(stdout)
        stderr_fh.write(stderr)

    activation_id = parse_activation_id(stdout) or activation_guess
    parsed_stdout = parse_prefixed_json(stdout)
    accepted_not_finished = (
        process.returncode == 202
        and activation_id != activation_guess
        and args.accept_async_completion
    )
    poll_status = ""
    if accepted_not_finished and args.poll_accepted:
        poll_status = poll_activation_until_done(args, activation_id, run_dir)
        completion_ns = now_ns()
    if args.event_bridge_port:
        try:
            if metadata_end_event is None:
                raise RuntimeError("missing metadata end event")
            end_status = send_event_bridge_event(args.event_bridge_port, metadata_end_event)
            metadata_status = f"{metadata_status};end:{end_status}"
        except Exception as exc:
            metadata_status = f"{metadata_status};end-error:{type(exc).__name__}:{exc}"

    return {
        "event_id": invocation.event_id,
        "invocation_id": invocation.invocation_id,
        "function_id": invocation.function_id,
        "profile_id": invocation.profile_id,
        "workload": invocation.workload,
        "action": invocation.action,
        "activation_id": activation_id,
        "scheduled_at_ms": invocation.at_ms,
        "target_duration_ms": invocation.target_duration_ms,
        "deadline_us": invocation.deadline_us,
        "slo_class": invocation.slo_class,
        "scheduled_monotonic_ns": scheduled_ns,
        "submit_monotonic_ns": submit_ns,
        "completion_monotonic_ns": completion_ns,
        "submit_wall_ns": wall_ns(),
        "latency_ms": (completion_ns - submit_ns) / 1_000_000.0,
        "exit_status": process.returncode,
        "ok": process.returncode == 0
        or (accepted_not_finished and not poll_status.startswith("poll-error")),
        "metadata_status": metadata_status,
        "activation_status": (
            "accepted-polled"
            if accepted_not_finished and poll_status
            else "accepted"
            if accepted_not_finished
            else str(parsed_stdout.get("response", {}).get("status", ""))
            if parsed_stdout
            else ""
        ),
        "poll_status": poll_status,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "command": command_display,
    }


def write_latency_header(path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "event_id",
                "invocation_id",
                "function_id",
                "profile_id",
                "workload",
                "action",
                "activation_id",
                "scheduled_at_ms",
                "target_duration_ms",
                "deadline_us",
                "slo_class",
                "scheduled_monotonic_ns",
                "submit_monotonic_ns",
                "completion_monotonic_ns",
                "latency_ms",
                "exit_status",
                "ok",
                "metadata_status",
                "activation_status",
                "poll_status",
                "stdout_path",
                "stderr_path",
            ],
        )
        writer.writeheader()


def append_latency(path: Path, result: dict[str, Any]) -> None:
    fieldnames = [
        "event_id",
        "invocation_id",
        "function_id",
        "profile_id",
        "workload",
        "action",
        "activation_id",
        "scheduled_at_ms",
        "target_duration_ms",
        "deadline_us",
        "slo_class",
        "scheduled_monotonic_ns",
        "submit_monotonic_ns",
        "completion_monotonic_ns",
        "latency_ms",
        "exit_status",
        "ok",
        "metadata_status",
        "activation_status",
        "poll_status",
        "stdout_path",
        "stderr_path",
    ]
    with path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writerow({key: result.get(key, "") for key in fieldnames})


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def summarize_latencies(results: list[dict[str, Any]]) -> dict[str, Any]:
    latencies = [float(item["latency_ms"]) for item in results if item.get("ok")]
    return {
        "attempts": len(results),
        "successes": len(latencies),
        "p50_ms": percentile(latencies, 0.50),
        "p90_ms": percentile(latencies, 0.90),
        "min_ms": min(latencies) if latencies else None,
        "max_ms": max(latencies) if latencies else None,
    }


def calibration_invocation(
    action: str,
    workload: str,
    target_ms: int,
    repetition: int,
    sequence: int,
) -> ReplayInvocation:
    profile_id = f"calibration_{workload}"
    return ReplayInvocation(
        event_id=f"calibration-{workload}-{target_ms}-{repetition}",
        invocation_id=sequence,
        at_ms=0.0,
        function_id=f"calibration:{workload}",
        profile_id=profile_id,
        workload=workload,
        action=action,
        target_duration_ms=target_ms,
        deadline_us=max(target_ms * 3_000, 10_000),
        slo_class=1,
        profile_hints={},
        payload=None,
    )


def run_calibration(
    args: argparse.Namespace,
    action_map: dict[str, str],
    run_dir: Path,
) -> int:
    targets = args.calibration_target_ms or [25, 125, 300, 1000, 3000]
    if args.calibration_repetitions <= 0:
        raise SystemExit("--calibration-repetitions must be positive")
    if not action_map:
        raise SystemExit("--calibrate requires at least one --action-map entry")

    run_dir.mkdir(parents=True, exist_ok=False)
    (run_dir / "stdout").mkdir()
    (run_dir / "stderr").mkdir()
    (run_dir / "payloads").mkdir()
    (run_dir / "activations").mkdir()
    latency_csv = run_dir / "client_latency.csv"
    write_latency_header(latency_csv)

    action_items = [
        (key, value)
        for key, value in sorted(action_map.items())
        if key != "*"
    ]
    if not action_items and "*" in action_map:
        action_items = [("default", action_map["*"])]

    records: dict[str, Any] = {}
    failures = 0
    sequence = 0
    for workload, action in action_items:
        per_target: dict[str, Any] = {}
        for target_ms in targets:
            target_results: list[dict[str, Any]] = []
            for repetition in range(1, args.calibration_repetitions + 1):
                sequence += 1
                invocation = calibration_invocation(
                    action, workload, target_ms, repetition, sequence
                )
                result = run_invocation(args, invocation, run_dir)
                result["calibration_target_ms"] = target_ms
                target_results.append(result)
                append_latency(latency_csv, result)
                with (run_dir / "requests.jsonl").open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
                if not result.get("ok"):
                    failures += 1
            per_target[str(target_ms)] = {
                "target_ms": target_ms,
                "summary": summarize_latencies(target_results),
                "results": target_results,
            }
        records[workload] = {"action": action, "targets": per_target}

    calibration = {
        "mode": "openwhisk-action-calibration",
        "dry_run": args.dry_run,
        "repetitions": args.calibration_repetitions,
        "targets_ms": targets,
        "actions": records,
    }
    output = args.calibration_output or run_dir / "calibration.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(calibration, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (run_dir / "summary.json").write_text(
        json.dumps(
            {"ok": failures == 0, "failures": failures, "calibration_output": str(output)},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print(run_dir)
    return 1 if failures else 0


def main() -> int:
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        raise SystemExit("--limit must be positive")
    if not args.dry_run and shutil.which(args.wsk) is None:
        raise SystemExit(f"wsk executable not found: {args.wsk}")

    action_map = parse_action_map(args.action_map)
    container_map = parse_action_map(args.container_name_map)
    run_dir = args.run_dir or args.out_dir / f"openwhisk-azure-{wall_ns()}-{os.getpid()}"
    if args.calibrate:
        return run_calibration(args, action_map, run_dir)

    if args.replay is None:
        raise SystemExit("--replay is required unless --calibrate is set")
    invocations = load_replay(args.replay, action_map, args.limit)
    if not invocations:
        raise SystemExit("replay contained no invocations")

    run_dir.mkdir(parents=True, exist_ok=False)
    (run_dir / "stdout").mkdir()
    (run_dir / "stderr").mkdir()
    (run_dir / "payloads").mkdir()
    (run_dir / "activations").mkdir()
    (run_dir / "requests.jsonl").write_text("", encoding="utf-8")
    latency_csv = run_dir / "client_latency.csv"
    write_latency_header(latency_csv)

    manifest = {
        "replay": str(args.replay),
        "count": len(invocations),
        "action_map": action_map,
        "container_name_map": container_map,
        "wsk": args.wsk,
        "wsk_arg": args.wsk_arg,
        "blocking": args.blocking,
        "result": args.result,
        "timeout_s": args.timeout_s,
        "accept_async_completion": args.accept_async_completion,
        "poll_accepted": args.poll_accepted,
        "activation_poll_timeout_s": args.activation_poll_timeout_s,
        "dry_run": args.dry_run,
        "event_bridge_port": args.event_bridge_port,
        "metadata_target": args.metadata_target,
    }
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    write_lock = threading.Lock()
    failures = 0
    base_ns = now_ns()
    threads: list[threading.Thread] = []

    def worker(invocation: ReplayInvocation) -> None:
        nonlocal failures
        target_ns = base_ns + int(max(0.0, invocation.at_ms) * 1_000_000)
        sleep_ns = target_ns - now_ns()
        if sleep_ns > 0:
            time.sleep(sleep_ns / 1_000_000_000)
        result = run_invocation(args, invocation, run_dir, container_map)
        with write_lock:
            if not result.get("ok"):
                failures += 1
            with (run_dir / "requests.jsonl").open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
            append_latency(latency_csv, result)

    for invocation in invocations:
        thread = threading.Thread(
            target=worker,
            args=(invocation,),
            name=f"ow-replay-{invocation.invocation_id}",
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    summary = {
        "count": len(invocations),
        "failures": failures,
        "ok": failures == 0,
        "run_dir": str(run_dir),
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(run_dir)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

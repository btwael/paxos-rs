#!/usr/bin/env python3
import argparse
import fnmatch
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError as exc:
    raise SystemExit("Python 3.11+ is required") from exc

CONFIG = "bench.toml"
DECISION_PREFIX = "TRACEFORGE_PAXOS_EXEC_DECISIONS "
ACTIVE_PROCS = {}
ACTIVE_PROCS_LOCK = threading.Lock()
TERMINATING = False
TERMINATION_SIGNAL = None


@dataclass(frozen=True)
class Job:
    case_id: str
    args: tuple[str, ...]
    nodes: int
    requests: int
    max_slots: int
    max_ballots: int
    workers: int
    print_decisions: bool


def slug(value):
    return re.sub(r"[^A-Za-z0-9._-]+", "-", str(value)).strip("-")


def toml_quote(value):
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def load_config(path):
    with Path(path).open("rb") as f:
        return tomllib.load(f)


def resolve_path(base_dir, value):
    path = Path(value)
    return path if path.is_absolute() else base_dir / path


def settings(config_path, config, args):
    base_dir = Path(config_path).resolve().parent
    raw = config.get("settings", {})
    available_cores = args.available_cores or int(raw.get("available_cores", os.cpu_count() or 1))
    jobs = args.jobs or int(raw.get("jobs", available_cores))
    timeout_hours = args.timeout_hours
    if timeout_hours is None:
        timeout_hours = float(raw.get("timeout_hours", 1))
    snapshot_interval_hours = args.snapshot_interval_hours
    if snapshot_interval_hours is None:
        snapshot_interval_hours = float(raw.get("snapshot_interval_hours", 1))
    bin_dir = resolve_path(base_dir, args.bin_dir or raw.get("bin_dir", "examples/traceforge-paxos/target/release"))
    results_dir = resolve_path(base_dir, args.results_dir or raw.get("results_dir", "results/tf-kvstore"))
    binary = args.binary or raw.get("binary", "traceforge-paxos")
    default_workers = args.default_workers or int(raw.get("default_workers", 4))
    default_max_slots = args.default_max_slots or int(raw.get("default_max_slots", 2))
    default_max_ballots = args.default_max_ballots
    if default_max_ballots is None:
        default_max_ballots = int(raw.get("default_max_ballots", 1))
    default_print_decisions = args.default_print_decisions
    if default_print_decisions is None:
        default_print_decisions = bool_value(raw.get("default_print_decisions", False), "default_print_decisions")
    if available_cores < 1 or jobs < 1 or default_workers < 1 or default_max_slots < 1:
        raise ValueError("available_cores, jobs, default_workers, and default_max_slots must be >= 1")
    if default_max_ballots < 0:
        raise ValueError("default_max_ballots must be >= 0")
    return {
        "available_cores": available_cores,
        "jobs": jobs,
        "timeout_hours": timeout_hours,
        "timeout_seconds": timeout_hours * 3600,
        "snapshot_interval_hours": snapshot_interval_hours,
        "snapshot_interval_seconds": snapshot_interval_hours * 3600,
        "bin_dir": bin_dir,
        "binary": binary,
        "results_dir": results_dir,
        "default_workers": default_workers,
        "default_max_slots": default_max_slots,
        "default_max_ballots": default_max_ballots,
        "default_print_decisions": default_print_decisions,
    }


def positive_int(value, name):
    parsed = int(value)
    if parsed < 1:
        raise ValueError(f"{name} must be >= 1")
    return parsed


def non_negative_int(value, name):
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"{name} must be >= 0")
    return parsed


def bool_value(value, name):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"{name} must be a boolean")


def case_id(run, nodes, requests, max_slots, max_ballots, workers, print_decisions):
    if "name" in run:
        return slug(run["name"])
    return "__".join(
        slug(part)
        for part in [
            "tf-kvstore",
            f"nodes-{nodes}",
            f"requests-{requests}",
            f"slots-{max_slots}",
            f"ballots-{max_ballots}",
            f"workers-{workers}",
            f"decisions-{'on' if print_decisions else 'off'}",
        ]
        if slug(part)
    )


def build_job(cfg, run):
    if "nodes" not in run:
        raise ValueError("each [[run]] must specify nodes")
    if "requests" not in run:
        raise ValueError("each [[run]] must specify requests")
    nodes = positive_int(run["nodes"], "nodes")
    if nodes < 2:
        raise ValueError("nodes must be >= 2 for this Paxos model")
    requests = non_negative_int(run["requests"], "requests")
    max_slots = positive_int(run.get("max_slots", cfg["default_max_slots"]), "max_slots")
    max_ballots = non_negative_int(
        run.get("max_ballots", cfg["default_max_ballots"]), "max_ballots"
    )
    workers = positive_int(run.get("workers", cfg["default_workers"]), "workers")
    print_decisions = bool_value(
        run.get("print_decisions", cfg["default_print_decisions"]), "print_decisions"
    )
    binary = cfg["bin_dir"] / cfg["binary"]
    args = [
        str(binary),
        "--nodes",
        str(nodes),
        "--requests",
        str(requests),
        "--max-slots",
        str(max_slots),
        "--max-ballots",
        str(max_ballots),
        "--workers",
        str(workers),
    ]
    if print_decisions:
        args.append("--print-decisions")
    return Job(
        case_id=case_id(run, nodes, requests, max_slots, max_ballots, workers, print_decisions),
        args=tuple(args),
        nodes=nodes,
        requests=requests,
        max_slots=max_slots,
        max_ballots=max_ballots,
        workers=workers,
        print_decisions=print_decisions,
    )


def jobs_from_config(config, cfg, match_patterns):
    jobs = []
    for run in config.get("run", []):
        job = build_job(cfg, run)
        if match_patterns and not any(fnmatch.fnmatch(job.case_id, pattern) for pattern in match_patterns):
            continue
        jobs.append(job)
    return jobs


def job_from_args(cfg, args):
    run = {
        "nodes": args.nodes,
        "requests": args.requests,
        "max_slots": args.max_slots if args.max_slots is not None else cfg["default_max_slots"],
        "max_ballots": args.max_ballots
        if args.max_ballots is not None
        else cfg["default_max_ballots"],
        "workers": args.workers or cfg["default_workers"],
        "print_decisions": args.print_decisions
        if args.print_decisions is not None
        else cfg["default_print_decisions"],
    }
    if args.name:
        run["name"] = args.name
    return build_job(cfg, run)


def validate_jobs(jobs, available_cores):
    seen = {}
    for job in jobs:
        if job.case_id in seen:
            raise ValueError(f"duplicate case id {job.case_id!r}")
        seen[job.case_id] = job
        if job.workers > available_cores:
            raise ValueError(
                f"{job.case_id} requests {job.workers} workers, but available_cores is {available_cores}"
            )


def wrap_time(args):
    time_bin = shutil.which("time")
    if not time_bin:
        return list(args), None
    if sys.platform == "darwin":
        return [time_bin, "-l", *args], "darwin"
    return [time_bin, "-v", *args], "linux"


def parse_max_rss_kb(mode, stderr):
    if mode == "linux":
        match = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", stderr)
        return int(match.group(1)) if match else None
    if mode == "darwin":
        match = re.search(r"^\s*(\d+)\s+maximum resident set size", stderr, re.MULTILINE)
        if match:
            return (int(match.group(1)) + 1023) // 1024
    return None


def parse_stats(stdout):
    matches = re.findall(r"Stats\s*=\s*(\d+)\s*,\s*(\d+)", stdout)
    if not matches:
        return -1, -1
    execs, blocked = matches[-1]
    return int(execs), int(blocked)


def decision_lines(stdout):
    return [line for line in stdout.splitlines() if line.startswith(DECISION_PREFIX)]


class OutputBuffer:
    def __init__(self):
        self._chunks = []
        self._lock = threading.Lock()

    def append(self, chunk):
        with self._lock:
            self._chunks.append(chunk)

    def text(self):
        with self._lock:
            return b"".join(self._chunks).decode("utf-8", errors="replace")


def read_stream(stream, output):
    try:
        while True:
            chunk = os.read(stream.fileno(), 65536)
            if not chunk:
                break
            output.append(chunk)
    finally:
        stream.close()


def write_stdout_snapshots(case_dir, output, stop_event, interval_seconds):
    if interval_seconds <= 0:
        return
    snapshots_dir = case_dir / "snapshots"
    index = 1
    while not stop_event.wait(interval_seconds):
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        (snapshots_dir / f"stdout-{index}.txt").write_text(
            output.text(),
            encoding="utf-8",
            errors="replace",
        )
        index += 1


def register_proc(job, proc):
    with ACTIVE_PROCS_LOCK:
        ACTIVE_PROCS[proc.pid] = (job, proc)


def unregister_proc(proc):
    with ACTIVE_PROCS_LOCK:
        ACTIVE_PROCS.pop(proc.pid, None)


def kill_process_group(proc, sig):
    if proc.poll() is not None:
        return
    try:
        if hasattr(os, "killpg"):
            os.killpg(proc.pid, sig)
        else:
            proc.send_signal(sig)
    except ProcessLookupError:
        pass


def terminate_active_processes(sig=signal.SIGTERM):
    with ACTIVE_PROCS_LOCK:
        procs = list(ACTIVE_PROCS.values())
    for _, proc in procs:
        kill_process_group(proc, sig)


def handle_termination(signum, _frame):
    global TERMINATING, TERMINATION_SIGNAL
    if TERMINATING:
        terminate_active_processes(signal.SIGKILL)
        raise SystemExit(128 + signum)
    TERMINATING = True
    TERMINATION_SIGNAL = signum
    with ACTIVE_PROCS_LOCK:
        active = list(ACTIVE_PROCS.values())
    print(f"[signal] received {signum}; terminating {len(active)} active job(s)", file=sys.stderr, flush=True)
    for _, proc in active:
        kill_process_group(proc, signal.SIGTERM)


def install_signal_handlers():
    signal.signal(signal.SIGTERM, handle_termination)
    signal.signal(signal.SIGINT, handle_termination)


def shlex_quote(value):
    import shlex

    return shlex.quote(value)


def print_job_output(job, stdout, stderr, label, max_lines=200):
    def tail(text):
        lines = text.splitlines()
        if len(lines) <= max_lines:
            return "\n".join(lines)
        return "\n".join([f"... truncated to last {max_lines} lines ...", *lines[-max_lines:]])

    print(f"\n[{label}] {job.case_id} stdout:", flush=True)
    print(tail(stdout), flush=True)
    if stderr.strip():
        print(f"\n[{label}] {job.case_id} stderr:", flush=True)
        print(tail(stderr), flush=True)


def write_time_file(path, job, status, exit_code, elapsed, timeout_hours, max_rss_kb, execs, blocked):
    lines = [
        f"case = {toml_quote(job.case_id)}",
        'protocol = "tf-kvstore"',
        f"status = {toml_quote(status)}",
        f"exit_code = {exit_code}",
        f"elapsed_seconds = {elapsed:.6f}",
        f"timeout_hours = {timeout_hours}",
        f"nodes = {job.nodes}",
        f"requests = {job.requests}",
        f"max_slots = {job.max_slots}",
        f"max_ballots = {job.max_ballots}",
        f"workers = {job.workers}",
        f"print_decisions = {'true' if job.print_decisions else 'false'}",
        f"cores = {job.workers}",
        f"execs = {execs}",
        f"blocked = {blocked}",
    ]
    if max_rss_kb is not None:
        lines.append(f"max_rss_kb = {max_rss_kb}")
    lines.append("command = [")
    for arg in job.args:
        lines.append(f"  {toml_quote(arg)},")
    lines.append("]")
    path.write_text("\n".join(lines) + "\n")


def run_job(job, results_dir, timeout_seconds, timeout_hours, snapshot_interval_seconds):
    case_dir = results_dir / job.case_id
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True)
    (case_dir / "command.txt").write_text(" ".join(shlex_quote(a) for a in job.args) + "\n")

    cmd, time_mode = wrap_time(job.args)
    start = time.perf_counter()
    timed_out = False
    stdout_buffer = OutputBuffer()
    stderr_buffer = OutputBuffer()
    stop_snapshots = threading.Event()
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid if hasattr(os, "setsid") else None,
    )
    stdout_reader = threading.Thread(target=read_stream, args=(proc.stdout, stdout_buffer), daemon=True)
    stderr_reader = threading.Thread(target=read_stream, args=(proc.stderr, stderr_buffer), daemon=True)
    snapshot_writer = threading.Thread(
        target=write_stdout_snapshots,
        args=(case_dir, stdout_buffer, stop_snapshots, snapshot_interval_seconds),
        daemon=True,
    )
    stdout_reader.start()
    stderr_reader.start()
    snapshot_writer.start()

    register_proc(job, proc)
    try:
        proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        timed_out = True
        kill_process_group(proc, signal.SIGKILL)
        proc.wait()
    finally:
        unregister_proc(proc)
        stop_snapshots.set()
        stdout_reader.join()
        stderr_reader.join()
        snapshot_writer.join()

    elapsed = time.perf_counter() - start
    exit_code = proc.returncode if proc.returncode is not None else -1
    status = "timeout" if timed_out else ("ok" if exit_code == 0 else f"exit_code_{exit_code}")
    stdout = stdout_buffer.text()
    stderr = stderr_buffer.text()
    max_rss_kb = parse_max_rss_kb(time_mode, stderr)
    execs, blocked = parse_stats(stdout)

    (case_dir / "stdout.txt").write_text(stdout, encoding="utf-8", errors="replace")
    (case_dir / "stderr.txt").write_text(stderr, encoding="utf-8", errors="replace")
    decisions = decision_lines(stdout)
    (case_dir / "decisions.txt").write_text(
        ("\n".join(decisions) + "\n") if decisions else "",
        encoding="utf-8",
        errors="replace",
    )
    write_time_file(case_dir / "time.toml", job, status, exit_code, elapsed, timeout_hours, max_rss_kb, execs, blocked)

    if timed_out:
        print_job_output(job, stdout, stderr, "timeout-output")
    elif TERMINATING and exit_code != 0:
        print_job_output(job, stdout, stderr, "interrupted-output")

    return {"job": job, "status": status, "elapsed": elapsed, "execs": execs, "blocked": blocked}


def run_scheduled(jobs, cfg):
    validate_jobs(jobs, cfg["available_cores"])
    jobs = sorted(jobs, key=lambda job: (job.workers, job.case_id))
    pending = list(jobs)
    running = {}
    used_cores = 0
    max_workers = min(cfg["jobs"], len(jobs)) or 1
    cfg["results_dir"].mkdir(parents=True, exist_ok=True)

    print(f"selected_jobs = {len(jobs)}")
    print(f"available_cores = {cfg['available_cores']}")
    print(f"max_processes = {max_workers}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while pending or running:
            if TERMINATING:
                pending.clear()

            launched = False
            i = 0
            while not TERMINATING and i < len(pending) and len(running) < max_workers:
                job = pending[i]
                if used_cores + job.workers <= cfg["available_cores"]:
                    pending.pop(i)
                    used_cores += job.workers
                    print(f"[start] {job.case_id} cores={job.workers}", flush=True)
                    fut = executor.submit(
                        run_job,
                        job,
                        cfg["results_dir"],
                        cfg["timeout_seconds"],
                        cfg["timeout_hours"],
                        cfg["snapshot_interval_seconds"],
                    )
                    running[fut] = job
                    launched = True
                else:
                    i += 1

            if not running:
                if TERMINATING:
                    break
                raise RuntimeError("no runnable jobs despite non-empty pending queue")

            if launched and pending and not TERMINATING:
                continue

            done, _ = wait(running.keys(), return_when=FIRST_COMPLETED)
            for fut in done:
                job = running.pop(fut)
                used_cores -= job.workers
                result = fut.result()
                print(
                    f"[done] {job.case_id} {result['elapsed']:.2f}s "
                    f"{result['status']} Stats={result['execs']},{result['blocked']}",
                    flush=True,
                )

    if TERMINATING:
        raise SystemExit(128 + (TERMINATION_SIGNAL or signal.SIGTERM))


def print_jobs(jobs):
    for job in jobs:
        print(f"{job.case_id} cores={job.workers}")
        print("  " + " ".join(shlex_quote(arg) for arg in job.args))
    print(f"total = {len(jobs)}")


def parse_common(parser):
    parser.add_argument("--config", default=CONFIG)
    parser.add_argument("--bin-dir")
    parser.add_argument("--binary")
    parser.add_argument("--results-dir")
    parser.add_argument("--available-cores", type=int)
    parser.add_argument("--jobs", type=int)
    parser.add_argument("--timeout-hours", type=float)
    parser.add_argument("--snapshot-interval-hours", type=float)
    parser.add_argument("--default-workers", type=int)
    parser.add_argument("--default-max-slots", type=int)
    parser.add_argument("--default-max-ballots", type=int)
    parser.add_argument("--default-print-decisions", dest="default_print_decisions", action="store_true", default=None)
    parser.add_argument("--no-default-print-decisions", dest="default_print_decisions", action="store_false")


def main():
    install_signal_handlers()

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    list_p = sub.add_parser("list")
    parse_common(list_p)
    list_p.add_argument("--match", action="append", default=[])

    run_p = sub.add_parser("run")
    parse_common(run_p)
    run_p.add_argument("--all", action="store_true")
    run_p.add_argument("--match", action="append", default=[])

    run_one_p = sub.add_parser("run-one")
    parse_common(run_one_p)
    run_one_p.add_argument("--name")
    run_one_p.add_argument("--nodes", type=int, required=True)
    run_one_p.add_argument("--requests", type=int, required=True)
    run_one_p.add_argument("--max-slots", type=int)
    run_one_p.add_argument("--max-ballots", type=int)
    run_one_p.add_argument("--workers", type=int)
    run_one_p.add_argument("--print-decisions", action="store_true", default=None)
    run_one_p.add_argument("--no-print-decisions", dest="print_decisions", action="store_false")

    args = parser.parse_args()
    config = load_config(args.config)
    cfg = settings(args.config, config, args)

    if args.cmd == "list":
        jobs = jobs_from_config(config, cfg, args.match)
        validate_jobs(jobs, max(cfg["available_cores"], max((job.workers for job in jobs), default=1)))
        print_jobs(jobs)
        return

    if args.cmd == "run":
        if not args.all and not args.match:
            raise SystemExit("use --all or --match")
        jobs = jobs_from_config(config, cfg, args.match)
        run_scheduled(jobs, cfg)
        return

    if args.cmd == "run-one":
        run_scheduled([job_from_args(cfg, args)], cfg)
        return


if __name__ == "__main__":
    main()

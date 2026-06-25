#!/usr/bin/env python3
"""check_connector_status.py — Kafka Connect / Debezium CDC health checker.

Theme 1 (Pause-Driven Offset Aging) edition.

The original vendor tool derived health from connector/task *state* only, so a
connector that reported RUNNING while making no forward progress showed up as
HEALTHY -- exactly the silent-stall masking behavior (issues #16, #17) that turns
a pause into multi-day data loss. This version moves from state-only to
state + offset-progression and adds pause-age awareness and auto-resume:

  1. Offset-progression detection      -> new STALLED state
  2. Pause-age awareness               -> new AT_RISK state
  3. Auto-resume (prevention lever)    -> --resume-paused (--dry-run by default)
  4. Monitoring-friendly operation     -> --watch/--interval + --exit-code

Requires: Python 3.7+, requests (pip install requests)
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

try:
    import requests
except ImportError:  # pragma: no cover
    sys.stderr.write("This tool requires the 'requests' library: pip install requests\n")
    sys.exit(2)


# --------------------------------------------------------------------------- #
# Health states
# --------------------------------------------------------------------------- #
HEALTHY = "HEALTHY"
FAILED = "FAILED"
TASK_FAILED = "TASK_FAILED"
PAUSED = "PAUSED"
STOPPED = "STOPPED"
UNASSIGNED = "UNASSIGNED"
STALLED = "STALLED"        # NEW: RUNNING but offset not advancing (Theme 1 core)
AT_RISK = "AT_RISK"        # NEW: paused long enough to risk offset aging out

# States that should drive a non-zero exit code under --exit-code.
UNHEALTHY_STATES = {FAILED, TASK_FAILED, STALLED, AT_RISK, UNASSIGNED}

# ANSI colors for table mode.
COLORS = {
    HEALTHY: "\033[32m",       # green
    FAILED: "\033[31m",        # red
    TASK_FAILED: "\033[31m",   # red
    STALLED: "\033[31m",       # red
    AT_RISK: "\033[33m",       # yellow
    PAUSED: "\033[33m",        # yellow
    STOPPED: "\033[33m",       # yellow
    UNASSIGNED: "\033[35m",    # magenta
}
RESET = "\033[0m"

DEFAULT_STATE_FILE = os.path.join(
    os.path.expanduser("~"), ".connector_status_state.json"
)


# --------------------------------------------------------------------------- #
# Time helpers
# --------------------------------------------------------------------------- #
def now_ts():
    """Current time as a UTC epoch float."""
    return datetime.now(timezone.utc).timestamp()


def human_duration(seconds):
    """Render a duration in seconds as a compact human string."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h{minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d{hours:02d}h"


# --------------------------------------------------------------------------- #
# State persistence (offset snapshots + first-paused timestamps)
# --------------------------------------------------------------------------- #
def load_state(path):
    """Load the persisted snapshot. Returns {} on any error so a missing or
    corrupt state file degrades gracefully into a first-run baseline."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def save_state(path, state):
    """Persist the snapshot. Best-effort: warn but don't fail the check."""
    try:
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except OSError as exc:  # pragma: no cover
        sys.stderr.write(f"Warning: could not write state file {path}: {exc}\n")


# --------------------------------------------------------------------------- #
# Kafka Connect REST client
# --------------------------------------------------------------------------- #
class ConnectClient:
    def __init__(self, host, timeout):
        self.base = host.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def _get(self, path):
        resp = self.session.get(f"{self.base}{path}", timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def list_connectors(self):
        return self._get("/connectors")

    def status(self, name):
        return self._get(f"/connectors/{name}/status")

    def offsets(self, name):
        """GET /connectors/{name}/offsets (Kafka Connect 3.6+). Returns the
        parsed body, or None if the endpoint is unavailable (older Connect)."""
        try:
            return self._get(f"/connectors/{name}/offsets")
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (404, 405, 501):
                return None
            raise

    def resume(self, name):
        resp = self.session.put(
            f"{self.base}/connectors/{name}/resume", timeout=self.timeout
        )
        resp.raise_for_status()


# --------------------------------------------------------------------------- #
# Offset fingerprinting
# --------------------------------------------------------------------------- #
def offset_fingerprint(offsets_body):
    """Reduce the /offsets response to a stable, comparable string.

    Debezium source offsets vary by connector type (Oracle SCN, MySQL file+pos,
    SQL Server LSN), so we don't interpret the fields -- we just canonicalize the
    whole offset map. If it changes between runs, the connector advanced."""
    if not offsets_body:
        return None
    offsets = offsets_body.get("offsets")
    if offsets is None:
        return None
    try:
        return json.dumps(offsets, sort_keys=True)
    except (TypeError, ValueError):
        return str(offsets)


# --------------------------------------------------------------------------- #
# Health derivation
# --------------------------------------------------------------------------- #
def base_health(status):
    """Derive the original state-only health, plus task counts and errors."""
    conn_state = status.get("connector", {}).get("state", "UNKNOWN")
    tasks = status.get("tasks", [])
    total = len(tasks)
    running = sum(1 for t in tasks if t.get("state") == "RUNNING")
    failed = sum(1 for t in tasks if t.get("state") == "FAILED")

    errors = []
    conn_trace = status.get("connector", {}).get("trace")
    if conn_trace:
        errors.append((status.get("name", "?"), conn_trace.splitlines()[0]))
    for t in tasks:
        if t.get("trace"):
            label = f"{status.get('name', '?')}#task-{t.get('id')}"
            errors.append((label, t["trace"].splitlines()[0]))

    if conn_state == "FAILED":
        health = FAILED
    elif failed > 0:
        health = TASK_FAILED
    elif conn_state == "PAUSED":
        health = PAUSED
    elif conn_state == "STOPPED":
        health = STOPPED
    elif total == 0 or running < total:
        health = UNASSIGNED
    else:
        health = HEALTHY

    worker = ""
    if tasks:
        worker = tasks[0].get("worker_id", "")

    return {
        "connector_state": conn_state,
        "health": health,
        "running": running,
        "total": total,
        "failed": failed,
        "worker": worker,
        "errors": errors,
    }


def refine_health(name, info, offsets_body, prev, current_ts, args):
    """Layer Theme 1 detection on top of the state-only health.

    Mutates and returns `info`, recording the offset fingerprint and pause
    bookkeeping that must be persisted for the next run."""
    fp = offset_fingerprint(offsets_body)
    info["offset_fingerprint"] = fp
    prev_entry = prev.get(name, {})

    # --- Offset-progression -> STALLED ------------------------------------ #
    # Only meaningful for connectors that claim to be making progress.
    info["stall_seconds"] = 0
    info["offset_available"] = fp is not None
    if info["health"] == HEALTHY and fp is not None:
        prev_fp = prev_entry.get("offset_fingerprint")
        prev_fp_since = prev_entry.get("offset_fp_since")
        if prev_fp == fp and prev_fp_since is not None:
            # Offset unchanged since prev_fp_since.
            stalled_for = current_ts - prev_fp_since
            info["stall_seconds"] = stalled_for
            info["offset_fp_since"] = prev_fp_since
            if stalled_for >= args.stall_threshold:
                info["health"] = STALLED
        else:
            # Offset moved (or first observation): reset the stall clock.
            info["offset_fp_since"] = current_ts
    else:
        info["offset_fp_since"] = current_ts

    # --- Pause-age -> AT_RISK --------------------------------------------- #
    info["paused_seconds"] = 0
    if info["health"] in (PAUSED, AT_RISK):
        paused_since = prev_entry.get("paused_since") or current_ts
        info["paused_since"] = paused_since
        paused_for = current_ts - paused_since
        info["paused_seconds"] = paused_for
        if args.retention_warn is not None and paused_for >= args.retention_warn:
            info["health"] = AT_RISK
    # else: not paused -> drop any stored paused_since by not carrying it.

    return info


def snapshot_for(name, info, current_ts):
    """Extract the fields that must persist to the next run."""
    entry = {
        "offset_fingerprint": info.get("offset_fingerprint"),
        "offset_fp_since": info.get("offset_fp_since", current_ts),
        "last_seen": current_ts,
        "last_health": info["health"],
    }
    if info.get("paused_since") is not None and info["health"] in (PAUSED, AT_RISK):
        entry["paused_since"] = info["paused_since"]
    return entry


# --------------------------------------------------------------------------- #
# Collection
# --------------------------------------------------------------------------- #
def collect(client, prev, args):
    """Return (results dict keyed by connector, new_state dict)."""
    current_ts = now_ts()
    names = sorted(client.list_connectors())
    results = {}
    new_state = {}

    for name in names:
        try:
            status = client.status(name)
        except requests.RequestException as exc:
            results[name] = {
                "health": FAILED,
                "connector_state": "UNREACHABLE",
                "running": 0, "total": 0, "failed": 0, "worker": "",
                "type": "?",
                "errors": [(name, f"status query failed: {exc}")],
                "stall_seconds": 0, "paused_seconds": 0, "offset_available": False,
            }
            new_state[name] = {"last_seen": current_ts, "last_health": FAILED}
            continue

        info = base_health(status)
        info["type"] = status.get("type", "?")

        offsets_body = None
        # Only source connectors carry source coordinates worth tracking.
        if info["type"] != "sink":
            try:
                offsets_body = client.offsets(name)
            except requests.RequestException:
                offsets_body = None

        refine_health(name, info, offsets_body, prev, current_ts, args)
        results[name] = info
        new_state[name] = snapshot_for(name, info, current_ts)

    return results, new_state


# --------------------------------------------------------------------------- #
# Auto-resume (prevention lever)
# --------------------------------------------------------------------------- #
def maybe_resume(client, results, args):
    """Resume PAUSED/AT_RISK connectors. Dry-run by default."""
    targets = [n for n, i in results.items() if i["health"] in (PAUSED, AT_RISK)]
    if not targets:
        return
    for name in targets:
        if args.dry_run:
            print(f"[dry-run] would resume: {name} "
                  f"(paused {human_duration(results[name]['paused_seconds'])})")
            continue
        try:
            client.resume(name)
            print(f"[resume] resumed: {name}")
        except requests.RequestException as exc:
            sys.stderr.write(f"[resume] FAILED to resume {name}: {exc}\n")


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #
def shorten_worker(worker):
    if not worker:
        return "-"
    # Strip host:port down to the pod/host token for readability.
    host = worker.split(":")[0]
    return host[-24:]


def detail_note(info):
    """Extra context shown in the HEALTH column for the new states."""
    if info["health"] == STALLED:
        return f" (no offset {human_duration(info['stall_seconds'])})"
    if info["health"] == AT_RISK:
        return f" (paused {human_duration(info['paused_seconds'])})"
    if info["health"] == PAUSED and info.get("paused_seconds"):
        return f" ({human_duration(info['paused_seconds'])})"
    return ""


def print_table(results, use_color):
    headers = ["CONNECTOR", "TYPE", "HEALTH", "TASKS", "FAILED", "WORKER"]
    rows = []
    for name in sorted(results):
        info = results[name]
        health_txt = info["health"] + detail_note(info)
        rows.append([
            name,
            info.get("type", "?"),
            health_txt,
            f"{info['running']}/{info['total']}",
            str(info["failed"]),
            shorten_worker(info["worker"]),
        ])

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(cells, color=None):
        out = "  ".join(c.ljust(widths[i]) for i, c in enumerate(cells))
        if color and use_color:
            return f"{color}{out}{RESET}"
        return out

    print(fmt_row(headers))
    print("  ".join("-" * w for w in widths))
    for name, row in zip(sorted(results), rows):
        color = COLORS.get(results[name]["health"]) if use_color else None
        print(fmt_row(row, color))

    # Error section.
    all_errors = []
    for name in sorted(results):
        all_errors.extend(results[name]["errors"])
    if all_errors:
        print("\nERRORS:")
        for label, line in all_errors:
            print(f"  {label}: {line}")

    # Summary line.
    counts = {}
    for info in results.values():
        counts[info["health"]] = counts.get(info["health"], 0) + 1
    summary = "  ".join(f"{k}={counts[k]}" for k in sorted(counts))
    print(f"\nSUMMARY: {len(results)} connectors  |  {summary}")


def print_json(results):
    out = {}
    for name, info in results.items():
        out[name] = {
            "type": info.get("type", "?"),
            "health": info["health"],
            "connector_state": info.get("connector_state"),
            "tasks_running": info["running"],
            "tasks_total": info["total"],
            "tasks_failed": info["failed"],
            "worker": info["worker"],
            "stall_seconds": int(info.get("stall_seconds", 0)),
            "paused_seconds": int(info.get("paused_seconds", 0)),
            "offset_available": info.get("offset_available", False),
            "errors": [{"source": s, "message": m} for s, m in info["errors"]],
        }
    print(json.dumps(out, indent=2, sort_keys=True))


# --------------------------------------------------------------------------- #
# Filtering & exit status
# --------------------------------------------------------------------------- #
def apply_filters(results, args):
    if args.unhealthy:
        results = {n: i for n, i in results.items() if i["health"] != HEALTHY}
    if args.filter:
        wanted = args.filter.upper()
        results = {n: i for n, i in results.items() if i["health"] == wanted}
    return results


def worst_exit(results):
    return 1 if any(i["health"] in UNHEALTHY_STATES for i in results.values()) else 0


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def run_once(client, args):
    prev = load_state(args.state_file)
    results, new_state = collect(client, prev, args)
    save_state(args.state_file, new_state)

    if args.resume_paused:
        maybe_resume(client, results, args)

    shown = apply_filters(results, args)
    if args.json:
        print_json(shown)
    else:
        use_color = sys.stdout.isatty() and not args.no_color
        print_table(shown, use_color)

    # Exit status is computed over ALL results, not just the filtered view,
    # so a --filter doesn't hide unhealthy connectors from an alerting cron.
    return worst_exit(results)


def parse_args(argv):
    p = argparse.ArgumentParser(
        description="Kafka Connect / Debezium CDC health checker "
                    "with offset-progression (Theme 1) detection."
    )
    p.add_argument("--host", required=True,
                   help="Kafka Connect REST API URL (e.g. http://localhost:8083)")
    p.add_argument("--json", action="store_true",
                   help="Output as JSON instead of a table")
    p.add_argument("--filter", metavar="STATE",
                   help="Show only connectors in this health state")
    p.add_argument("--unhealthy", action="store_true",
                   help="Show only non-HEALTHY connectors")
    p.add_argument("--timeout", type=float, default=30,
                   help="Per-request timeout in seconds (default: 30)")

    # Theme 1 additions.
    p.add_argument("--state-file", default=DEFAULT_STATE_FILE,
                   help="Where offset snapshots persist between runs "
                        f"(default: {DEFAULT_STATE_FILE})")
    p.add_argument("--stall-threshold", type=float, default=300,
                   help="Seconds of no offset movement before a RUNNING "
                        "connector is marked STALLED (default: 300)")
    p.add_argument("--retention-warn", type=float, default=None, metavar="SECONDS",
                   help="Pause duration (seconds) after which a PAUSED connector "
                        "is escalated to AT_RISK. Set near the source retention "
                        "window. Disabled if omitted.")
    p.add_argument("--watch", action="store_true",
                   help="Poll continuously instead of running once")
    p.add_argument("--interval", type=float, default=60,
                   help="Seconds between polls in --watch mode (default: 60)")
    p.add_argument("--resume-paused", action="store_true",
                   help="Resume PAUSED/AT_RISK connectors (see --dry-run)")
    p.add_argument("--dry-run", dest="dry_run", action="store_true", default=True,
                   help="With --resume-paused, only report intended resumes "
                        "(default)")
    p.add_argument("--no-dry-run", dest="dry_run", action="store_false",
                   help="With --resume-paused, actually issue resume calls")
    p.add_argument("--exit-code", action="store_true",
                   help="Exit non-zero if any connector is FAILED/TASK_FAILED/"
                        "STALLED/AT_RISK/UNASSIGNED (for cron/CronJob alerting)")
    p.add_argument("--no-color", action="store_true",
                   help="Disable ANSI colors in table mode")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    client = ConnectClient(args.host, args.timeout)

    if not args.watch:
        try:
            status = run_once(client, args)
        except requests.RequestException as exc:
            sys.stderr.write(f"Error contacting Kafka Connect at {args.host}: {exc}\n")
            return 2
        return status if args.exit_code else 0

    # Watch mode: loop until interrupted. Each iteration prints a timestamped
    # block so the persisted offset snapshots accumulate across polls.
    try:
        while True:
            stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
            print(f"===== {stamp} =====")
            try:
                run_once(client, args)
            except requests.RequestException as exc:
                sys.stderr.write(f"Error contacting {args.host}: {exc}\n")
            print()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())

"""
bot/core/heartbeat.py
─────────────────────
Liveness that survives the process — a heartbeat file plus the deadman that reads it.

WHY IT IS SHAPED THIS WAY. The death that actually happens in practice is SIGKILL — memory
pressure, not a polite signal — and it is the one death that always strands live orders. SIGKILL
runs no handler, so a dying process CANNOT report its own death. The only construction that works
is the inverse: the living process continuously asserts that it is alive, and a SEPARATE observer
notices when the assertions stop.

That observer needs nothing but the filesystem. Each beat is a small durable JSON file under
`logs/heartbeat/<name>.json` carrying the operator's three questions — how many markets are
quoted, what inventory is on, what the P&L is — plus the fields the deadman needs. Critically the
file carries its OWN staleness budget (`stale_after_s`): a watchdog reading it does not have to be
configured with the process's cadence, so the budget cannot drift out of sync with the writer.
A separate watchdog script is that observer, and it exits non-zero, so a `systemd` timer or cron
entry can be the thing that actually watches.

FIVE STATES, NOT TWO. `ok` / `stale` / `missing` / `corrupt` / `exited`.
  · `missing` and `corrupt` are NOT `ok`: absence of evidence of life is not evidence of health
    (the reconciler's cannot-verify-is-not-flat rule, applied to liveness). A truncated heartbeat
    is precisely the artefact of a SIGKILL mid-write, and reading it as "never started" tells a
    benign story about the worst event.
  · `exited` exists so a PLANNED shutdown does not look identical to a crash. If it did, every
    deploy would page someone and the alarm would be trained into noise within a week. A clean
    exit is `ok`; a halt (`halted:loss_cap`, `halted:kill_switch`, `halted:memory`) is not —
    and neither is `record_open:<status>` (the Poly maker's teardown left its crash record
    OPEN: the sweep could not confirm nothing is resting, so the exit is a must-see even when
    the status it wraps is "clean"). `Deadman.ok` is exact equality on "clean", so any new
    prefix in this family is not-ok by default — the safe direction.

A stale beat additionally reports whether the recorded PID still exists, because "stale + pid
gone" (killed) and "stale + pid alive" (hung — this repo's black-hole-socket freeze) are different
diagnoses with different fixes.

NEVER RAISES. `beat()` is called from the quote loop; instrumentation must not be able to stop
trading.
"""
from __future__ import annotations

import glob
import json
import logging
import math
import os
import time
from dataclasses import dataclass

from bot.core import durable

log = logging.getLogger(__name__)

DEFAULT_DIR = durable.repo_path("logs", "heartbeat")

# How many missed beats before the deadman trips. 3 tolerates an ordinary slow cycle — the maker's
# `_positions` alone retries a 429 at 1.5/3/6s — without tolerating a death. The floor keeps a
# fast-cadence writer (1s) from tripping on a single GC pause or a scheduler hiccup.
_GRACE_MULTIPLIER = 3.0
_GRACE_FLOOR_S = 30.0


def _stale_after(interval_s: float) -> float:
    return max(interval_s * _GRACE_MULTIPLIER, interval_s + _GRACE_FLOOR_S)


def heartbeat_path(name: str, directory: str | None = None) -> str:
    return os.path.join(directory or DEFAULT_DIR, f"{name}.json")


def _pid_running(pid: int | None) -> bool | None:
    """True/False, or None if we cannot tell. `signal 0` checks existence without delivering."""
    if not pid or pid <= 0:
        return None
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True                      # exists, owned by someone else
    except (OSError, ValueError, TypeError):
        return None
    return True


class Heartbeat:
    """The writer. One instance per long-running process; call `beat()` every cycle."""

    def __init__(self, name: str, *, interval_s: float, directory: str | None = None,
                 pid: int | None = None, stale_after_s: float | None = None) -> None:
        self.name = name
        self.interval_s = float(interval_s)
        self.pid = int(pid if pid is not None else os.getpid())
        self.path = heartbeat_path(name, directory)
        # The writer owns its own budget: a teardown that legitimately blocks (the maker's
        # `--flatten-wait-s 60` passive flatten) needs a wider one than its requote interval.
        self.stale_after_s = float(stale_after_s if stale_after_s is not None
                                   else _stale_after(self.interval_s))
        self.started_ts = time.time()
        self._seq = 0
        # The last beat's caller-supplied fields, carried into `mark_exit` — see there.
        self._last_fields: dict[str, object] = {}

    def beat(self, *, now: float | None = None, **fields: object) -> None:
        """Write one beat. Extra kwargs are recorded verbatim — `markets_quoted=`, `inventory=`,
        `pnl=` are the conventional three, but nothing here constrains them."""
        ts = time.time() if now is None else float(now)
        self._seq += 1
        self._last_fields = dict(fields)
        payload = {
            "name": self.name,
            "pid": self.pid,
            "ts": ts,
            "seq": self._seq,
            "started_ts": self.started_ts,
            "uptime_s": max(0.0, ts - self.started_ts),
            "interval_s": self.interval_s,
            "stale_after_s": self.stale_after_s,
            "exit_status": None,
            **fields,
        }
        self._write(payload)

    def mark_exit(self, status: str = "clean", *, now: float | None = None,
                  **fields: object) -> None:
        """Record that this process is finishing on purpose.

        Only reachable when cleanup runs — so its ABSENCE after the beats stop is exactly the
        SIGKILL signature the deadman is looking for. Use `clean` for a normal finish and
        `halted:<reason>` for a guard-triggered stop; the deadman treats the latter as not-ok.

        CARRIES THE LAST BEAT'S FIELDS FORWARD. The state at the moment of the halt — what
        inventory was on, what the P&L was — is the single most useful thing in this file, and a
        bare exit stamp would overwrite it with nothing. Explicit kwargs still win.
        """
        ts = time.time() if now is None else float(now)
        self._seq += 1
        merged = {**self._last_fields, **fields}
        self._write({
            "name": self.name,
            "pid": self.pid,
            "ts": ts,
            "seq": self._seq,
            "started_ts": self.started_ts,
            "uptime_s": max(0.0, ts - self.started_ts),
            "interval_s": self.interval_s,
            "stale_after_s": self.stale_after_s,
            "exit_status": str(status),
            **merged,
        })

    def _write(self, payload: dict) -> None:
        try:
            durable.write_json_durable(self.path, payload)
        except Exception as exc:              # instrumentation must never stop trading
            log.error(f"heartbeat: could not write {self.path} ({exc!r}) — this process is now "
                      f"INVISIBLE to the deadman")


@dataclass(frozen=True)
class Deadman:
    """One process's liveness verdict, as read from outside it."""
    name: str
    path: str
    state: str                     # ok | stale | missing | corrupt | exited
    detail: str
    age_s: float | None = None
    pid: int | None = None
    pid_running: bool | None = None
    exit_status: str | None = None
    fields: dict | None = None

    #: Set when an operator has explicitly acknowledged a HALT — see `acknowledge`.
    acknowledged_ts: float | None = None
    acknowledged_by: str | None = None

    @property
    def ok(self) -> bool:
        """`exited` is ok ONLY for a clean exit — a halt is an exit an operator must SEE.

        ⛔ …and "seen" needs a way to be recorded, or the alarm is permanent. Without an
        acknowledgement path, a halt the operator has already dealt with keeps being reported as
        the process's ONLY problem on every watchdog run, for as long as the record sits there.
        A rail that cannot be answered trains the operator to ignore it, and an ignored rail is
        worse than no rail — it is the only thing standing between a real halt and silence, and
        by the time the next halt lands it has been crying wolf for hours.

        Acknowledgement is deliberately NOT deletion: the record stays on disk with the halt
        reason intact, so the evidence survives and the watchdog still PRINTS the row. It stops
        being a *problem*, not a fact.
        """
        if self.state == "ok":
            return True
        if self.state == "exited":
            return self.exit_status == "clean" or self.acknowledged_ts is not None
        return False


def read_heartbeat(path: str) -> dict | None:
    return durable.read_json_or_none(path)


def deadman_check(path: str, *, now: float | None = None) -> Deadman:
    """Classify one heartbeat file. Pure with respect to `now`, so it is testable without sleeping."""
    ts_now = time.time() if now is None else float(now)
    name = os.path.splitext(os.path.basename(path))[0]
    try:
        raw = durable.read_json_strict(path)
    except durable.StateCorrupt as exc:
        return Deadman(name, path, "corrupt",
                       f"heartbeat file is unreadable ({exc}) — liveness UNKNOWN. A truncated "
                       f"file is what a SIGKILL mid-write leaves behind.")
    if raw is None:
        return Deadman(name, path, "missing",
                       "no heartbeat file — the process never started, never beat, or its file "
                       "was removed. This is NOT evidence of health.")
    if not isinstance(raw, dict):
        return Deadman(name, path, "corrupt", f"heartbeat is {type(raw).__name__}, not an object")

    pid = raw.get("pid")
    try:
        ts = float(raw.get("ts"))
    except (TypeError, ValueError):
        return Deadman(name, path, "corrupt", "heartbeat has no usable `ts`", pid=pid)
    age = ts_now - ts
    running = _pid_running(pid)
    exit_status = raw.get("exit_status")

    if exit_status:
        ack_ts = raw.get("acknowledged_ts")
        try:
            ack_ts = float(ack_ts) if ack_ts is not None else None
        except (TypeError, ValueError):
            ack_ts = None          # a malformed ack is NO ack — never silence on junk
        # ⛔ …and `json` accepts NaN/Infinity by default, so `float("NaN")` does NOT raise and
        # the guard above let a non-finite value silence the alarm. This repo already learned
        # that lesson in the orderbook, where `feed._levels_to_dict` drops non-finite wire
        # values explicitly. A bool also floats cleanly (True → 1.0), hence the type check.
        if ack_ts is not None and (isinstance(raw.get("acknowledged_ts"), bool)
                                   or not math.isfinite(ack_ts)):
            ack_ts = None
        ack_by = raw.get("acknowledged_by")
        detail = f"process exited with status {exit_status!r} {age:.0f}s ago"
        if ack_ts is not None:
            detail += f" — ACKNOWLEDGED by {ack_by or '?'} {(ts_now - ack_ts) / 3600:.1f}h ago"
        return Deadman(name, path, "exited", detail,
                       age_s=age, pid=pid, pid_running=running,
                       exit_status=str(exit_status), fields=raw,
                       acknowledged_ts=ack_ts,
                       acknowledged_by=str(ack_by) if ack_by else None)

    try:
        budget = float(raw.get("stale_after_s") or _stale_after(float(raw.get("interval_s") or 0)))
    except (TypeError, ValueError):
        budget = _GRACE_FLOOR_S
    if age > budget:
        if running is False:
            why = ("the PID is GONE and no exit was recorded — this is an unclean death "
                   "(SIGKILL / OOM-kill / power loss). Live orders may be resting on the venue.")
        elif running is True:
            why = ("the PID is still alive but stopped beating — the process is HUNG, not dead "
                   "(a blocked socket or a wedged loop).")
        else:
            why = "no exit was recorded and the PID cannot be checked."
        return Deadman(name, path, "stale",
                       f"DEADMAN TRIPPED: last beat {age:.0f}s ago (budget {budget:.0f}s); {why}",
                       age_s=age, pid=pid, pid_running=running, fields=raw)

    return Deadman(name, path, "ok", f"beating ({age:.0f}s ago)",
                   age_s=age, pid=pid, pid_running=running, fields=raw)


#: Exit statuses an operator may acknowledge from the CLI. ⛔ ALLOWLIST — anything not named
#: here refuses, so a status family added later inherits the SAFE default rather than the
#: permissive one. `record_open:*` and `halted:prior_run_unresolved` are deliberately absent:
#: both mean the venue's state is UNKNOWN, which is the condition the rail exists to surface.
ACKNOWLEDGEABLE_STATUSES: tuple[str, ...] = ("clean", "halted")

#: …and these substrings veto an otherwise-acknowledgeable status. The family allowlist is not
#: fine enough on its own: `halted:prior_run_unresolved` sits inside the acknowledgeable
#: `halted:` family while meaning an EARLIER crash is still open, which is precisely the state
#: that must not be answered from here. Substring, not prefix, so it holds wherever the marker
#: appears in a composed status.
UNACKNOWLEDGEABLE_MARKERS: tuple[str, ...] = ("prior_run_unresolved", "record_open")


def acknowledge(name: str, *, by: str, directory: str | None = None,
                now: float | None = None) -> str:
    """Record that an operator has SEEN a halted process's heartbeat. Returns the path written.

    ⛔ REFUSES to acknowledge anything that is not a recorded exit. A `stale` heartbeat means the
    PID vanished with no exit recorded — an unclean death, where live orders may still be resting
    on the venue — and a `missing`/`corrupt` one means the rail itself is broken. Those are not
    things an operator can wave through from here; they need the venue asked. Only a process that
    said WHY it stopped can be acknowledged.

    ⛔ Does NOT delete the file, and does not touch `exit_status`. The halt reason stays readable
    forever; only its status as an open PROBLEM changes.
    """
    path = heartbeat_path(name, directory)
    raw = read_heartbeat(path)
    if not isinstance(raw, dict):
        raise SystemExit(f"REFUSING: no readable heartbeat at {path}")
    status = str(raw.get("exit_status") or "")
    if not status:
        raise SystemExit(
            f"REFUSING: {name} recorded no exit_status — this is an unclean death, not a halt. "
            f"Live orders may be resting. Ask the venue (scripts.maker_recover) before "
            f"acknowledging anything.")
    # ⛔ AN ALLOWLIST, NOT A TRUTHINESS CHECK. The first cut
    # guarded on `exit_status` being non-empty, reasoning "no status means the venue was never
    # asked". But `record_open:<status>` IS a written status, and its entire meaning is that the
    # teardown sweep's LISTING FAILED — "the venue could not confirm that nothing is resting"
    # (bot/poly_us/maker.py). So the one status that most needs a human to open the venue was
    # the one this waved through, and an operator trained by this very tool to answer the rail
    # would have silenced it. A truthiness check also gives every FUTURE status family the
    # wrong default; an allowlist gives them the safe one.
    if (any(bad in status for bad in UNACKNOWLEDGEABLE_MARKERS)
            or not any(status == ok or status.startswith(ok + ":")
                       for ok in ACKNOWLEDGEABLE_STATUSES)):
        raise SystemExit(
            f"REFUSING: {name} exited {status!r}, which is not acknowledgeable. "
            f"`record_open:*` means the teardown could not READ the venue, and "
            f"`halted:prior_run_unresolved` means an earlier crash is still open — both need "
            f"the venue opened, not the alarm answered. Check resting orders "
            f"(scripts.maker_recover, or scripts.poly_us_orders for Poly), then acknowledge "
            f"whatever remains.")
    raw["acknowledged_ts"] = time.time() if now is None else float(now)
    raw["acknowledged_by"] = by
    tmp = f"{path}.tmp"
    with open(tmp, "w") as fh:
        json.dump(raw, fh, indent=2)
    os.replace(tmp, path)
    return path


def scan(directory: str | None = None, *, now: float | None = None) -> list[Deadman]:
    """Every heartbeat in `directory`. An empty/absent directory yields [] — which a caller must
    read as "nothing is being watched", not as "everything is fine"."""
    d = directory or DEFAULT_DIR
    return [deadman_check(p, now=now) for p in sorted(glob.glob(os.path.join(d, "*.json")))]


def problems(directory: str | None = None, *, now: float | None = None) -> list[str]:
    """Human-readable reasons a watched process is not healthy. Empty ⇒ all beating (or cleanly
    exited)."""
    return [f"{s.name}: {s.detail}" for s in scan(directory, now=now) if not s.ok]

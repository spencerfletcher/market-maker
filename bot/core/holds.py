"""
bot/core/holds.py
─────────────────
Operator-commandable, SELF-EXPIRING holds for pass-shaped producer units.

WHY THIS EXISTS
───────────────
On 2026-08-27/28 the operator hand-stopped producer units three times to clear a launch window
(`systemctl stop <unit> <unit>`, …). Stop-and-restore is stateful and
error-prone in two directions that both bit:

  · a FORGOTTEN restore leaves a producer off for hours or days while systemd reports nothing
    wrong — verbatim the T0-2 failure mode `<deploy unit>`'s header exists to prevent,
    and for `<unit>` it starves `poly_event_risk` gate 7, which FAILS CLOSED and refuses the
    NEXT launch (measured 2026-08-28: 2.50h realized lookback against a 3.0h floor);
  · `Persistent=true` on the timer makes the restore fire the pass IMMEDIATELY — i.e. straight
    back into the window the operator was trying to keep quiet.

A HOLD is the same intent expressed so that it heals itself: a file with a deadline. The unit's
timer keeps ticking, the pass wakes up, sees the hold, prints one line and exits 0 — and the first
tick after the deadline runs normally with nobody in the loop. Nothing to restore, nothing to
forget. It is the operator-driven twin of `bot/core/venue_backoff.py` (the venue-driven latch) and
shares its shape deliberately.

THE FAIL DIRECTION IS *TOWARD RUNNING*
──────────────────────────────────────
A missing, unreadable, corrupt, unrecognised, NON-FINITE or expired hold means NOT HELD. A hold that cannot be
read is a hold that cannot be cleared, and a producer disabled forever by a broken JSON file is a
far worse outcome than one extra pass — the whole point of this module is that a stuck-off producer
is the thing we are trying to stop happening. Corrupt is therefore treated as absent but LOUDLY
(`log.error`), never silently, per this repo's cannot-verify-is-not-flat rule.

The one place the module is deliberately STRICT is at WRITE time, and it VALIDATES BEFORE IT
WRITES (a refusal raised after `write_json_durable` publishes the file leaves an operator staring
at a traceback while the hold is already on disk):
  · an `until` more than `MAX_HOLD_S` (24h) ahead is REFUSED. A fat-fingered epoch (milliseconds
    pasted into a seconds field) would otherwise hold a producer for forty thousand years, and the
    read side — fail-open on everything else — would honour it perfectly, because such a file is
    not corrupt, just wrong;
  · a NON-FINITE `until` is REFUSED, on both sides. Every NaN comparison is False, so NaN passes
    BOTH range checks above AND never satisfies `until <= now` — a permanent, silent hold, which
    is the precise failure this module exists to prevent. `json.loads` accepts the literal tokens
    `NaN`/`Infinity`, so a hand-written or third-party file can carry one without being corrupt.

THE FILE
────────
`logs/holds/<unit>.json`, one per systemd UNIT (not per script — `scripts/poly_market_screen.py`
serves BOTH `<unit>` and `<unit>`, and holding one must not hold the other):

    {"until": 1756270000.0, "by": "spencer", "why": "launch window", "written_ts": 1756269400.0}

Written through `bot.core.durable` (atomic + fsync'd; SIGKILL is the death this box actually
suffers) at mode 0o644 — the heartbeat precedent, incident 2026-08-28: `tempfile.mkstemp` creates
0600 regardless of umask, and a rail written by one user and read by another under 0600 reports
"broken rail" instead of the state it holds. Producers run as `ubuntu`; a future pressure-shed or
supervisor writing a hold may well run as root.

WHAT V1 DOES NOT DO
───────────────────
The check happens ONCE, at pass entry. A hold written mid-pass does not abort the running pass —
see the private design notes § I-PRODUCER-HOLD for what is queued (mid-pass abort, spread-watch, and
hold-aware opswatch reporting so a held unit reads as WARNING rather than silence).
"""
from __future__ import annotations

import argparse
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from bot.core import durable
from bot.core.logger import get_logger

log = get_logger(__name__)

#: Default hold directory. ABSOLUTE via `repo_path` — producers run from assorted cwds and a
#: relative operational path is how the loss caps went silently inert (durable.py's header).
HOLDS_DIR = durable.repo_path("logs", "holds")

#: ⛔ WORLD-READABLE ON PURPOSE. See the module header — same reasoning, and the same incident,
#: as `bot.core.heartbeat.HEARTBEAT_MODE`.
HOLD_MODE = 0o644

#: The write-time ceiling. Long enough for any real operator intent (a launch window is minutes,
#: an outage a few hours), short enough that the worst fat-finger costs one day of one producer.
MAX_HOLD_S = 24 * 60 * 60.0

#: The environment variable each systemd unit sets to name ITSELF. ⛔ The unit is NOT guessable
#: from argv: `<unit>` and `<unit>` run the SAME script with different flags, so a
#: script-name-derived hold would hold both or neither.
UNIT_ENV = "PMB_UNIT"

#: ⛔ A LOAD-BEARING STRING, NOT A COSMETIC ONE — `<deploy unit>` greps for it.
#: That wrapper writes its `start (avail …)` marker BEFORE launching the pass, and its CF-1015
#: evidence window is bounded (backwards, via `tac`) at the newest such marker. A held pass would
#: therefore drop a marker that HIDES the previous pass's 1015 blob, and the next real pass would
#: walk ~12,000 origin reads into a possibly-live Cloudflare ban beside a maker — every request
#: during a ban extends it. The wrapper's `awk` recognises this token and skips past a held pass's
#: marker instead of terminating the window there. Change this string and you must change
#: `<deploy unit>` in the same commit; `tests/test_holds.py` pins the pair.
SKIPPED_MARKER = "pass skipped, no budget spent"

#: The unit whose hold has a LAUNCH consequence: `<unit>` feeds `poly_event_risk` gate 7,
#: which fails closed on a stale flow map (measured 2026-08-28: a 2.50h realized VOLATILE lookback
#: against a 3.0h floor closed the evening). Holding it longer than this earns a printed warning —
#: the shed-EV amendment in the private design notes in one line.
GATE_FEEDING_UNITS = ("collector-a",)
GATE_WARN_S = 3 * 60 * 60.0

#: A unit name is concatenated into a FILENAME. `../../x` would write the hold outside the watched
#: directory — the heartbeat's `_LANE_RE` reasoning, one layer down.
_UNIT_RE = re.compile(r"^[A-Za-z0-9_.@-]+$")


class HoldRefused(ValueError):
    """A hold that would not be written. Raised at WRITE time only — the read side never raises."""


@dataclass(frozen=True)
class Hold:
    unit: str
    until: float
    by: str
    why: str
    written_ts: float

    def remaining_s(self, now: Optional[float] = None) -> float:
        return self.until - (time.time() if now is None else float(now))

    def until_iso(self) -> str:
        return datetime.fromtimestamp(self.until, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def describe(self, now: Optional[float] = None) -> str:
        return (f"HELD until {self.until_iso()} by {self.by}: {self.why} "
                f"({self.remaining_s(now) / 60:.1f} min remaining)")


def hold_path(unit: str, directory: Optional[str] = None) -> str:
    """The hold file for `unit`. Validates the name — see `_UNIT_RE`."""
    unit = str(unit)
    if not _UNIT_RE.match(unit):
        raise HoldRefused(
            f"unit {unit!r} is not a safe filename component (allowed: {_UNIT_RE.pattern}) — a "
            f"unit name is concatenated into the hold filename, and one containing a path "
            f"separator would write the hold where no producer will look for it")
    return os.path.join(directory or HOLDS_DIR, f"{unit}.json")


# ── reading ──────────────────────────────────────────────────────────────────────────────────

def read_hold(unit: str, *, now: Optional[float] = None,
              directory: Optional[str] = None) -> Optional[Hold]:
    """The ACTIVE hold on `unit`, or None.

    None means every one of: no file, an unreadable file, a corrupt file, a file whose shape we do
    not recognise, and an EXPIRED hold. All five are "not held" — see the module header on why the
    fail direction points at running. Everything except absence and expiry is logged at ERROR.

    Expiry is the whole auto-resume mechanism: nothing clears a hold, it simply stops being true.
    The operator who wrote it may be asleep; a hold needing an explicit release is the stop-and-
    forget failure this module replaces.
    """
    ts = time.time() if now is None else float(now)
    rec = _read_record(unit, directory=directory)
    if rec is None:
        return None
    return None if rec.until <= ts else rec


def is_held(unit: str, now: Optional[float] = None, *, directory: Optional[str] = None) -> bool:
    """True iff an active hold exists. The one-line read-side check."""
    return read_hold(unit, now=now, directory=directory) is not None


def _read_record(unit: str, *, directory: Optional[str] = None) -> Optional[Hold]:
    """The stored record REGARDLESS of expiry, or None when there is nothing usable.

    Private because the expiry rule must not be optional at any call site outside this module —
    `read_hold` is the only supported read.
    """
    path = hold_path(unit, directory)
    try:
        obj = durable.read_json_strict(path)
    except durable.StateCorrupt as exc:
        log.error(f"holds: CORRUPT hold file {path} ({exc}) — treating as NOT HELD, the unit will "
                  f"RUN. Delete or rewrite it if you meant to hold this unit.")
        return None
    if obj is None:
        # A literal `null` is a FILE THAT EXISTS saying nothing — the same class as a list or a
        # string, and it must be as loud. `read_json_strict` returns None for both "absent" and
        # "parsed to null", so the file system is the only thing that can tell them apart.
        if os.path.exists(path):
            log.error(f"holds: hold file {path} parses to `null` — treating as NOT HELD, the "
                      f"unit will RUN.")
        return None
    if not isinstance(obj, dict):
        log.error(f"holds: UNRECOGNISED hold shape in {path} ({str(obj)[:200]}) — "
                  f"treating as NOT HELD, the unit will RUN.")
        return None
    try:
        until = float(obj["until"])
    except (KeyError, TypeError, ValueError):
        log.error(f"holds: hold file {path} has no usable `until` ({obj.get('until')!r}) — "
                  f"treating as NOT HELD, the unit will RUN.")
        return None
    # ⛔ NaN/±Inf IS A PERMANENT HOLD, AND IT IS SILENT [round-2 review, reproduced]. Every NaN
    # comparison is False, so `until <= now` in `read_hold` is False and the hold NEVER expires —
    # the exact stuck-off producer this module exists to prevent, and on `<unit>` it ages
    # the flow map past `poly_event_risk`'s volatile lookback floor until gate 7 refuses the next
    # real-money launch. `json.loads` accepts the literal tokens `NaN` / `Infinity`, so any
    # non-CLI writer (or a hand-edited file) can produce one. It also poisons every REPORT that
    # touches it: `datetime.fromtimestamp(nan)` raises, so one bad record would crash `--list`
    # and hide every other hold. Not held, loudly — the same fail direction as corrupt.
    if not math.isfinite(until):
        log.error(f"holds: hold file {path} has a NON-FINITE `until` ({obj.get('until')!r}) — "
                  f"treating as NOT HELD, the unit will RUN. A non-finite deadline never "
                  f"expires; rewrite the hold with `python -m bot.core.holds`.")
        return None
    try:
        written_ts = float(obj.get("written_ts") or 0.0)
    except (TypeError, ValueError):
        written_ts = 0.0
    return Hold(unit=str(unit), until=until, by=str(obj.get("by") or "unrecorded"),
                why=str(obj.get("why") or "unrecorded"), written_ts=written_ts)


def list_holds(*, now: Optional[float] = None,
               directory: Optional[str] = None) -> list[Hold]:
    """Every ACTIVE hold, soonest deadline first. An expired file is not a hold and is omitted."""
    d = directory or HOLDS_DIR
    try:
        names = sorted(os.listdir(d))
    except FileNotFoundError:
        return []
    except OSError as exc:
        log.error(f"holds: cannot list {d} ({exc}) — reporting NO holds")
        return []
    out = []
    for name in names:
        if not name.endswith(".json"):
            continue
        unit = name[: -len(".json")]
        if not _UNIT_RE.match(unit):
            continue
        h = read_hold(unit, now=now, directory=d)
        if h is not None:
            out.append(h)
    return sorted(out, key=lambda h: h.until)


# ── writing ──────────────────────────────────────────────────────────────────────────────────

def write_hold(unit: str, *, minutes: Optional[float] = None, until: Optional[float] = None,
               by: str, why: str, now: Optional[float] = None,
               directory: Optional[str] = None) -> Hold:
    """Write a hold on `unit`. Exactly one of `minutes` / `until` (an absolute epoch).

    Refuses (`HoldRefused`) rather than writes when:
      · neither or both of minutes/until are given — an ambiguous hold is an operator error;
      · the deadline is not in the future — a hold that is already expired is a no-op that LOOKS
        like a hold, which is worse than an error;
      · the deadline is more than `MAX_HOLD_S` ahead. ⛔ THE FAT-FINGER GUARD, and the reason it
        lives here and not on the read side: a millisecond epoch pasted into a seconds field
        produces a perfectly well-formed file that the fail-open reader will honour forever.
    """
    ts = time.time() if now is None else float(now)
    if (minutes is None) == (until is None):
        raise HoldRefused("give exactly one of minutes= or until=")
    deadline = ts + float(minutes) * 60.0 if minutes is not None else float(until)
    # ⛔ FIRST, AND BEFORE ANY WRITE. Both range checks below are False for NaN (every NaN
    # comparison is), so a `--minutes nan` sailed through them into a file that can never expire —
    # and the failure then surfaced only from `until_iso()` in the log line AFTER
    # `write_json_durable` had already published it, so the operator saw a traceback, believed
    # nothing was written, and left a permanent hold on disk. Validate, THEN write.
    if not math.isfinite(deadline):
        raise HoldRefused(
            f"REFUSING: deadline {deadline!r} is not a finite number. A non-finite deadline "
            f"compares False against every clock, so it would hold this unit FOREVER.")
    if deadline <= ts:
        raise HoldRefused(
            f"REFUSING: deadline {deadline!r} is not in the future (now {ts:.0f}). An "
            f"already-expired hold holds nothing while looking like it does.")
    if deadline > ts + MAX_HOLD_S:
        raise HoldRefused(
            f"REFUSING: deadline {deadline!r} is {(deadline - ts) / 3600:.1f}h ahead, over the "
            f"{MAX_HOLD_S / 3600:.0f}h cap. If this was a pasted epoch, check its units "
            f"(milliseconds are 1000x too far). Write repeated shorter holds instead — the "
            f"expiry IS the safety property.")
    rec = Hold(unit=str(unit), until=deadline, by=str(by), why=str(why), written_ts=ts)
    path = hold_path(unit, directory)
    durable.write_json_durable(
        path,
        {"until": rec.until, "by": rec.by, "why": rec.why, "written_ts": rec.written_ts},
        mode=HOLD_MODE)
    log.info(f"holds: {unit} HELD until {rec.until_iso()} by {rec.by}: {rec.why} → {path}")
    return rec


def clear_hold(unit: str, *, directory: Optional[str] = None) -> bool:
    """Remove the hold file. True if one was there. Clearing an absent hold is not an error —
    the operator's intent ("this unit must not be held") is satisfied either way."""
    path = hold_path(unit, directory)
    try:
        os.unlink(path)
    except FileNotFoundError:
        return False
    log.info(f"holds: cleared {unit} ({path})")
    return True


# ── the producer pass-entry check ────────────────────────────────────────────────────────────

def resolve_unit(flag: Optional[str] = None) -> Optional[str]:
    """The unit this process belongs to: `$PMB_UNIT`, else the `--hold-unit` flag, else None.

    ⛔ NEVER DERIVED FROM ARGV. Two units run `scripts/poly_market_screen.py` with different
    flags; a script-derived name would make `--minutes 10` on one of them silence both.
    """
    env = (os.environ.get(UNIT_ENV) or "").strip()
    if env:
        return env
    flag = (flag or "").strip()
    return flag or None


def skip_if_held(unit_flag: Optional[str] = None, *, now: Optional[float] = None,
                 directory: Optional[str] = None) -> Optional[Hold]:
    """Pass-entry check. Returns the active Hold (caller should `return` immediately, spending no
    budget) or None (run the pass).

    Prints rather than logs the held line, because the consumer is a systemd journal entry an
    operator greps at 04:00 and the message must name what happens next. The caller exits 0 — a
    hold is an instruction that was obeyed, not a failure, and a red timer for a deliberate hold
    trains the operator to ignore a rail that also reports real faults (the same reasoning as
    `SuccessExitStatus=75` in the producer units).
    """
    unit = resolve_unit(unit_flag)
    if unit is None:
        log.debug(f"holds: no {UNIT_ENV} and no --hold-unit — no hold check for this pass")
        return None
    try:
        held = read_hold(unit, now=now, directory=directory)
    except HoldRefused as exc:          # an unsafe unit NAME; fail toward running, loudly
        log.error(f"holds: {exc} — no hold check for this pass")
        return None
    if held is None:
        return None
    print(f"HELD until {held.until_iso()} by {held.by}: {held.why} — {SKIPPED_MARKER}",
          flush=True)
    return held


# ── CLI ──────────────────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="python -m bot.core.holds",
        description="Operator holds for producer units — a hold expires on its own; nothing "
                    "needs restoring.")
    ap.add_argument("unit", nargs="?", help="systemd unit name, e.g. collector-a")
    ap.add_argument("--minutes", type=float, default=None, help="hold for N minutes from now")
    ap.add_argument("--until", type=float, default=None, help="hold until this epoch (seconds)")
    ap.add_argument("--by", default=None, help="who is holding (required to write a hold)")
    ap.add_argument("--why", default=None, help="why (required to write a hold)")
    ap.add_argument("--clear", action="store_true", help="remove the hold on UNIT")
    ap.add_argument("--list", action="store_true", help="show every active hold")
    ap.add_argument("--dir", default=None, help="hold directory (default logs/holds)")
    return ap


def main(argv: Optional[list[str]] = None) -> int:
    """⛔ EVERY refusal leaves this function as exit 2 with a message on stderr — never as a
    traceback. `--clear`/status on an unsafe unit name used to raise `HoldRefused` out of `main`,
    and an operator cannot tell a crashed tool from a tool that did something."""
    args = build_parser().parse_args(argv)
    try:
        return _main(args)
    except HoldRefused as exc:
        print(str(exc), file=sys.stderr)
        return 2


def _main(args: argparse.Namespace) -> int:
    if args.list:
        holds = list_holds(directory=args.dir)
        if not holds:
            print("no active holds")
            return 0
        for h in holds:
            print(f"{h.unit:24s} {h.describe()}")
        return 0
    if not args.unit:
        print("give a UNIT, or --list", file=sys.stderr)
        return 2
    if args.clear:
        print(f"cleared {args.unit}" if clear_hold(args.unit, directory=args.dir)
              else f"no hold on {args.unit}")
        return 0
    if args.minutes is None and args.until is None:
        h = read_hold(args.unit, directory=args.dir)
        if h is not None:
            print(h.describe())
        elif os.path.exists(hold_path(args.unit, args.dir)):
            # EXPIRED is not the same answer as NEVER-WRITTEN: the first says the hold did its
            # job and released, the second says the write never landed (wrong unit name, wrong
            # --dir). Collapsing them sends an operator hunting the wrong half.
            print(f"{args.unit}: not held (a hold file is present but EXPIRED — auto-resumed)")
        else:
            print(f"{args.unit}: not held (no hold has been written)")
        return 0
    if not args.by or not args.why:
        print("--by and --why are required to write a hold (a hold with no author and no "
              "reason is unauditable)", file=sys.stderr)
        return 2
    h = write_hold(args.unit, minutes=args.minutes, until=args.until, by=args.by,
                   why=args.why, directory=args.dir)
    print(h.describe())
    if args.unit in GATE_FEEDING_UNITS and h.remaining_s() > GATE_WARN_S:
        print(f"⚠ {args.unit} feeds poly_event_risk gate 7, which FAILS CLOSED on a stale flow "
              f"map (~3h volatile lookback floor). A hold this long can make the NEXT real-money "
              f"launch refuse — hold it in shorter windows unless no launch is planned.",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

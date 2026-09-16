"""
bot/core/venue_budget.py
────────────────────────
ONE request budget for `api.polymarket.us`, shared by every process on the box IP.

WHY A SHARED BUCKET AND NOT A PER-PROCESS PACE
──────────────────────────────────────────────
Cloudflare rate-limits the *IP*, not the process: the recorded bans were the SUM of independently
paced producers, some with no maker running at all. Per-process
pacing constants cannot see that sum, so the budget has to live outside the processes: one file,
one token bucket, `fcntl.flock` for the read-modify-write.

Every Poly US REST call in the tree funnels through `bot/poly_us/client.py` (the SDK's `_request`,
wrapped by `install()` in that client's constructor), so this module has exactly one insertion
point. The WebSocket feed (`bot/poly_us/feed.py`) is NOT metered: a subscription is one connection,
not a request stream.

THREE PRIORITY CLASSES
──────────────────────
`PRIO_MAKER` (a quoting maker, and the launch probe that arms it) > `PRIO_OPERATOR` (hand tools,
teardown, reconcile) > `PRIO_COLLECTOR_HIGH` (the seat-bearing collectors) >
`PRIO_COLLECTOR` (every other timer producer). A waiter never takes a token while
a HIGHER-priority process is waiting for one, so a burst of collector reads cannot put a live
maker's cancel behind twelve book pages. Collectors additionally refuse to draw at all while a
real maker heartbeat is fresh — the `--beside-maker` rule the venue-reading scripts already
apply, moved to where every collector passes — unless the caller says `beside_maker=True`.

THE BAN FLAG
────────────
A 429 / CF-1015 sets `banned_until` in the same file (the header's stated block × the multiple,
floored; a re-trip adds `VENUE_RETRIP_STEP_S` up to `VENUE_RETRIP_CAP_S`, never doubling; a
`VENUE_BREAKER_TRIPS`-trip window holds `VENUE_BREAKER_S` once),
and EVERY acquire refuses while it stands — maker included. The maker sees `VenueBanned` on its
next call and its own 429 handling decides what to do; this module never decides for it. Every
request issued during a Cloudflare ban extends the ban, which is why the refusal is universal.

RELATIONSHIP TO `bot/core/venue_backoff.py`
───────────────────────────────────────────
That module is the venue-HEALTH latch (a producer's opt-in observation, 5xx accounting, a timed
hold). This one is the REQUEST BUDGET, and it is not opt-in. They classify SEPARATELY and on
purpose: the latch arms on any 429/5xx-shaped venue sickness (a producer holding itself back costs
a page of tape), while this file only arms on a **limiter** signal — `is_cf_ban` below — because a
false ban here stops the maker too. `classify` in turn exempts this module's exceptions, so our own
refusal can never be read back as venue evidence.

THE FILE
────────
`logs/venue_budget_<source>.json` — ONE PER SOURCE ADDRESS (`default` for the primary route, the
IP literal for a bound one), beside `logs/venue_backoff.json`, mode 0644 because several unix users
write it (the heartbeat precedent). ⛔ NOT under `logs/heartbeat/`: `heartbeat.scan()` reads every
`*.json` there and would report this file as a corrupt beat.

    {"tokens": 12.5, "ts": 1757500000.0,
     "waiting": {"41231": [0, 1757500000.0]},          # pid -> [prio, last_seen]
     "ban": {"until": 1757500300.0, "duration_s": 3600.0, "n": 1, "last": 1757500000.0,
             "canary_ts": 1757500301.0},   # canary_ts absent = the post-ban canary is unspent
     "ramp_lease": {"pid": 41231, "label": "collector-a",      # the one collector after a lift
                    "ts": 1757500302.0, "last_grant_ts": 1757500303.0},
     # caller -> epoch minute -> endpoint class -> GRANTS (a pre-2026-09-14 file holds a bare int)
     "hour": {"<unit>": {"29291666": {"list": 400, "book_cdn": 12}}}}

A missing file initializes a full bucket. An existing corrupt or unreadable file refuses requests
until repaired; a recovery call cannot spend an unknown budget or bypass an unknown ban.
"""
from __future__ import annotations

import asyncio
import fcntl
import json
import math
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, Optional

from bot.core import durable
from bot.core.logger import get_logger

log = get_logger(__name__)

#: Sustained box-wide budget. The wall we
#: hit is Cloudflare's, not the venue's [operator]. ⛔ THE SAME CLASS OF NUMBER AS THE MAKER'S OWN
#: PACER (`bot/poly_us/maker.py:DEFAULT_MAX_REQ_PER_S`) — a box budget below the operator's chosen
#: per-process ceiling would silently re-pace the maker. Kept equal to it by hand; NOT imported,
#: because `maker` imports the client which imports this module.
VENUE_BUDGET_REQ_PER_S = 0.5   # the maker's own pacer class; the venue also caps a key per minute  # placeholder — production value withheld

#: Bucket depth. A bare 429 does not establish a per-key or public quota; the burst covers part of a
#: maker startup (slate read + open-orders + positions) and the remainder paces in at
#: `VENUE_BUDGET_REQ_PER_S`.
VENUE_BUDGET_BURST = 2  # placeholder — production value withheld

#: The venue's APP limit (a bare 429: "stop, wait ≥1 s, exponential backoff") is NOT a Cloudflare
#: ban and must not arm the ban ladder — it is a one-second pause for the whole box. Applied by
#: draining the bucket and dating it forward, so every class waits it out through the ordinary
#: `acquire` path and nothing raises.
VENUE_APP_LIMIT_PAUSE_S = 1.0

#: Wait granularity. A sleep, never a spin: `acquire` is awaited from the maker's event loop.
VENUE_BUDGET_POLL_S = 0.05

#: Priority classes. LOWER IS HIGHER PRIORITY (`min` is the winner), so a new class can be added
#: at either end without renumbering the two that gate money.
PRIO_MAKER = 0
PRIO_OPERATOR = 1
#: The seat-bearing collectors (`PMB_VENUE_PRIO=2` on <unit> and <unit>): they
#: PREEMPT the collector lease off a bulk pass rather than queue behind a 70-minute census.
PRIO_COLLECTOR_HIGH = 2
PRIO_COLLECTOR = 3

#: THE HOLD COMES FROM THE EDGE'S OWN HEADER: the edge blocks for seconds, and the long
#: "silence per key" once measured was OUR OWN flat hold plus the herd that re-tripped a
#: short-window rule at every lift, not the edge's block. So: `retry_after × VENUE_RETRY_AFTER_MULT`,
#: floored at `VENUE_BAN_FLOOR_S`; `VENUE_BAN_BASE_S` only when the header is absent. What stops the
#: herd is the canary and the paced ramp/lease below, which follow every lift — the hold does not have to.
#:
#: ⛔ A RE-TRIP DOES NOT DOUBLE. Doubling turned short edge blocks into hour-scale maker outages:
#: the edge's block is short, and the ladder
#: was the whole outage. A re-trip inside `VENUE_BAN_RESET_S` adds `VENUE_RETRIP_STEP_S` per
#: previous trip, capped at `VENUE_RETRIP_CAP_S` — 30, 45, 60, 75, 90 s … — and the only long hold
#: is the circuit breaker: `VENUE_BREAKER_TRIPS` trips inside the reset window hold
#: `VENUE_BREAKER_S` ONCE, then the trip counter starts again. `VENUE_BAN_MAX_S` now caps only a
#: header-STATED block and the no-header base.
VENUE_BAN_FLOOR_S = 60.0  # placeholder — production value withheld
VENUE_RETRY_AFTER_MULT = 3.0
VENUE_BAN_BASE_S = 600.0  # placeholder — production value withheld
VENUE_BAN_MAX_S = 7200.0  # placeholder — production value withheld
VENUE_BAN_RESET_S = 7200.0  # placeholder — production value withheld
VENUE_RETRIP_STEP_S = 15.0
VENUE_RETRIP_CAP_S = 120.0
VENUE_BREAKER_TRIPS = 3  # placeholder — production value withheld
VENUE_BREAKER_S = 600.0   # the edge block is short, and long collector holds each cost a whole reseat epoch of pick verifies  # placeholder — production value withheld

#: The lift is a CANARY, not a start gun. For this long after a ban's `until` the bucket grants
#: exactly ONE request in total (priority order still applies among live waiters) and
#: does not refill: every lift in the incident was a herd (the maker's re-verify burst, a
#: collector's long read pass), so the first 429 landed with a dozen requests already
#: in flight. One request means a re-ban costs one request.
VENUE_CANARY_S = 15.0

#: …and the canary is followed by a RAMP, not by the full bucket. Measured: after a long quiet
#: spell a single authed probe returned 200, and the lift burst that followed took a 429 within
#: seconds. The herd trips the edge on its own, so for
#: this long after the canary window the bucket refills at the ramp rate with the ramp depth, then
#: the ordinary `VENUE_BUDGET_REQ_PER_S`/`VENUE_BUDGET_BURST` resume. Priority is unchanged: the
#: maker's waiters drain the ramp first.
VENUE_RAMP_REQ_PER_S = 0.5   # post-lift ramp; the maker binds itself lower via its own pacer  # placeholder — production value withheld
VENUE_RAMP_BURST = 1  # placeholder — production value withheld
VENUE_RAMP_S = 600.0  # placeholder — production value withheld

#: THE COLLECTOR LEASE, PERMANENT and binding the COLLECTOR CLASSES ONLY. Producers must never
#: overlap on a key [operator]: after a post-ban window closed, several collectors resumed together
#: on the normal bucket and re-tripped the key within minutes — as at an earlier lift, where three
#: of them drew hundreds of requests in a few minutes. The bucket paces the AGGREGATE and never serializes processes, so exactly
#: ONE collector process may draw on a bucket AT ANY TIME (`ramp_lease` in the state file).
#:
#: Its pace has two values: `VENUE_COLLECTOR_REQ_PER_S` normally — one process at that rate is the
#: pre-incident single-collector shape; the HERD was the
#: trigger and a mistake now costs 30 s — and the slower `VENUE_COLLECTOR_LEASE_REQ_PER_S` inside
#: the post-ban window `until + VENUE_CANARY_S .. + VENUE_COLLECTOR_LEASE_S`, which now selects
#: only the pace. ⛔ NEITHER is the bucket's rate: throttling the bucket itself would throttle the
#: MAKER — a full requote cycle at that rate blows the heartbeat budget and a `pause.json` flatten
#: would take minutes.
VENUE_COLLECTOR_LEASE_S = 3600.0  # placeholder — production value withheld
VENUE_COLLECTOR_REQ_PER_S = 1.0  # placeholder — production value withheld

#: …but the collector KEY's BUCKET refills far below that: the venue caps a key per minute, and the
#: collector key tripped hourly under the lease pace until its panel was paced down. The
#: lease pace above still governs the ONE holder inside the bucket; the maker's public reads share
#: this bucket as `PRIO_OPERATOR`, which is why it cannot be the lease's number.
VENUE_COLLECTOR_BUCKET_REQ_PER_S = 0.5   # after repeated collector-key trips  # placeholder — production value withheld
VENUE_COLLECTOR_LEASE_REQ_PER_S = 0.5  # placeholder — production value withheld

#: WHEN THE LEASE IS FREE AGAIN. A holder that has not drawn for this long has finished its pass
#: or is between sweeps, so a 24 h daemon that sweeps periodically releases the key
#: between sweeps and re-takes it when free instead of holding it all day
#:.
VENUE_COLLECTOR_IDLE_S = 60.0  # placeholder — production value withheld

#: …and the only way the lease ever REFUSES: a collector that has waited continuously this long
#: gives up the pass and comes back on its next tick. It has to be a wall that long, because a
#: producer's refusal is a failed unit and a short ceiling would fail one every tick.
VENUE_COLLECTOR_WAIT_MAX_S = 3600.0  # placeholder — production value withheld

#: How long a registered waiter stays believed. A waiter re-stamps itself every
#: `VENUE_BUDGET_POLL_S`, so this is 20 polls of slack — and a SIGKILLed waiter cannot starve the
#: classes below it for longer than this. (`flock` liveness would be exact but needs one lock file
#: per waiter; this is the lazy bound and its error direction is "one extra second of yielding".)
VENUE_WAITER_TTL_S = 2.5   # 2026-09-14: > one token interval at the 1/s base bucket, so a waiting maker outranks a collector across a token

#: The maker-liveness read is a directory scan; memoise it for a cadence, not for a poll.
MAKER_LIVE_CACHE_S = 5.0

#: ⛔ ONE BUCKET PER SOURCE ADDRESS. The venue's public limit is per IP and Cloudflare's edge is
#: per IP, so collectors bound to a secondary IP (`config.POLY_SOURCE_IP`, the private design notes § Second
#: IP for collectors) must not share the maker's tokens OR its ban flag — one 429 earned by a
#: census on the second IP would otherwise stop a quoting maker on the first.
STATE_NAME = "venue_budget_{label}.json"
DEFAULT_SOURCE_LABEL = "default"

#: A source label is concatenated into a FILENAME and comes from the environment. An IPv4/IPv6
#: literal is the only shape accepted; anything else is REFUSED rather than sanitised (a silently
#: mangled label is a silently separate bucket, which is the failure this file prevents).
_SOURCE_RE = re.compile(r"^[0-9A-Fa-f.:]+$")


def source_label(source_ip: str = "") -> str:
    """The filename-safe label for a source address. Blank → `default` (the primary route)."""
    ip = (source_ip or "").strip()
    if not ip:
        return DEFAULT_SOURCE_LABEL
    if not _SOURCE_RE.match(ip):
        raise ValueError(f"venue_budget: POLY_SOURCE_IP={source_ip!r} is not an IP literal — "
                         f"the budget's state filename is derived from it")
    return ip


#: The directory the buckets live in. ⛔ Resolved at CALL time by `state_path`, never bound into a
#: default argument: the test sandbox redirects THIS constant, and a bound default escapes it.
#: ⛔ NOT `logs/heartbeat/`: `heartbeat.scan()` reads every `*.json` there and would report this
#: file as a corrupt beat.
STATE_DIR = durable.repo_path("logs")


def state_path(source_ip: str = "") -> str:
    """The bucket file for one source address."""
    return os.path.join(STATE_DIR, STATE_NAME.format(label=source_label(source_ip)))


#: Bounded wait for the state lock. Hold time is one parse + one atomic write (sub-ms), so 0.25 s
#: is ~250× that; beyond it token acquisition REFUSES rather than sending unmetered. ⛔ THE BOUND IS SMALL
#: BECAUSE THE MAKER PAYS IT: `acquire` runs the locked section in a worker thread
#: (`asyncio.to_thread`), but a `flock` wait still delays that request, and the maker's loop has a
#: kill-switch check and a quote cadence to keep.
_LOCK_TIMEOUT_S = 0.25
_STATE_MODE = 0o644

_maker_live_memo: tuple[float, Optional[str]] = (0.0, None)
#: Paths whose corrupt/unreadable state has already been logged — the refusal line is a
#: once-per-path fact, not a per-request one (a shed collector would otherwise flood the journal).
_corrupt_logged: set[str] = set()
#: (path, ban n) already announced as "operator call during a ban", so the operator override logs
#: once per ban per process rather than once per request.
_operator_ban_logged: set[tuple[str, int]] = set()


class VenueRefused(Exception):
    """Base: this process may not issue a Poly US request right now."""


class VenueBanned(VenueRefused):
    """The box IP is rate-limited (429/CF-1015) until `until` (unix seconds)."""

    def __init__(self, until: float, reason: str = "rate-limited") -> None:
        self.until = float(until)
        self.reason = reason
        remaining = max(0.0, self.until - time.time())
        super().__init__(f"venue budget: BANNED for another {remaining / 60:.1f} min "
                         f"(until {_iso(self.until)}) — {reason}")


class LeaseYield(VenueRefused):
    """The one-collector lease's wait ceiling: this pass waited `VENUE_COLLECTOR_WAIT_MAX_S`
    continuously behind a longer one and gives up the TICK, not the unit.

    ⛔ THE ONLY `VenueRefused` A PRODUCER SKIPS ON. `scripts/poly_market_screen.py:producer_cli`
    catches this (and `MakerLive`) as exit 0 + one skip receipt; corrupt state, an unavailable
    lock and `VenueBanned` stay uncaught and still fail the unit [I-LEASE-REFUSAL-SKIP].
    """


class MakerLive(VenueRefused):
    """A collector may not add requests on the same IP as a live real-money maker."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"venue budget: collector refused — {reason}")


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── state file ───────────────────────────────────────────────────────────────────────────────

def _fresh(now: float) -> dict:
    return {"tokens": float(VENUE_BUDGET_BURST), "ts": now, "waiting": {}, "ban": {}, "hour": {}}


def _corrupt(path: str, detail: str) -> None:
    """Refuse an existing invalid state; log once per path per process."""
    if path not in _corrupt_logged:
        _corrupt_logged.add(path)
        log.error(f"venue_budget: corrupt state in {path} ({detail}) — requests refused; "
                  f"repair the state file before retrying")
    raise VenueRefused(f"venue budget: corrupt state — request refused ({path})")


def _read(path: str, now: float) -> dict:
    """The parsed state; only an absent file initializes a full bucket."""
    try:
        with open(path, "rb") as fh:
            obj = json.load(fh)
    except FileNotFoundError:
        if os.path.lexists(path):
            return _corrupt(path, "existing state target is missing")
        return _fresh(now)
    except (OSError, ValueError) as exc:
        return _corrupt(path, f"unreadable/truncated state ({exc})")
    if not isinstance(obj, dict):
        return _corrupt(path, f"state is {type(obj).__name__}, not an object")
    state = _fresh(now)
    try:
        tokens, stamp = float(obj["tokens"]), float(obj["ts"])
        if not math.isfinite(tokens) or tokens < 0 or not math.isfinite(stamp):
            raise ValueError("nonfinite or negative bucket")
        state["tokens"] = min(tokens, float(VENUE_BUDGET_BURST))
        state["ts"] = stamp
    except (KeyError, TypeError, ValueError):
        return _corrupt(path, f"unusable bucket ({obj.get('tokens')!r} @ {obj.get('ts')!r})")
    for key in ("waiting", "ban", "hour", "ramp_lease"):
        if key in obj and not isinstance(obj[key], dict):
            return _corrupt(path, f"unusable {key}")
    ban = obj.get("ban") or {}
    if ban:
        try:
            until = float(ban["until"])
            if not math.isfinite(until):
                raise ValueError("nonfinite ban expiry")
        except (KeyError, TypeError, ValueError):
            return _corrupt(path, "unusable ban expiry")
    waiting = obj.get("waiting")
    if isinstance(waiting, dict):
        try:
            for entry in waiting.values():
                if type(entry[0]) is not int or entry[0] not in (
                        PRIO_MAKER, PRIO_OPERATOR, PRIO_COLLECTOR_HIGH, PRIO_COLLECTOR):
                    raise ValueError("unsupported waiter priority")
                if not math.isfinite(float(entry[1])) or (
                        len(entry) > 2 and not math.isfinite(float(entry[2]))):
                    raise ValueError("nonfinite waiter")
        except (IndexError, KeyError, TypeError, ValueError):
            return _corrupt(path, "unusable waiter")
        state["waiting"] = waiting
    state["ban"] = ban
    hour = obj.get("hour")
    if isinstance(hour, dict):            # absent in a file written before the meter existed
        state["hour"] = hour
    lease = obj.get("ramp_lease")
    if isinstance(lease, dict):           # absent = nobody holds the post-lift lease
        try:
            prio = lease.get("prio", PRIO_COLLECTOR)
            if type(prio) is not int or prio not in (PRIO_COLLECTOR_HIGH, PRIO_COLLECTOR):
                raise ValueError("unsupported collector priority")
            int(lease["pid"])
            if not math.isfinite(float(lease["last_grant_ts"])):
                raise ValueError("nonfinite lease")
        except (KeyError, TypeError, ValueError):
            return _corrupt(path, "unusable collector lease")
        state["ramp_lease"] = lease
    return state


def _write(path: str, state: dict) -> None:
    """Atomic replace, NO fsync. This file is a rate-limiter, not a ledger: the worst a lost write
    can cost is one refilled bucket, and the write happens up to 20×/s per waiter.
    # ponytail: os.replace without fsync; use durable.write_json_durable if it ever holds money.
    """
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".venue_budget.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(state, fh)
        os.chmod(tmp, _STATE_MODE)      # mkstemp is 0600; several unix users read this file
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ── the 60-minute meter ──────────────────────────────────────────────────────────────────────
#
# WHO SPENT THE HOUR'S BUDGET ON THIS KEY, not who received the 429. One incident was
# attributed to the maker because the maker got the ban line; a crawler had spent the
# reaches the venue, so it cannot have cost us anything.

_HOUR_MINUTES = 60

_caller_label: Optional[str] = None


def caller_label() -> str:
    """This process's short meter key. Computed ONCE: it cannot change within a process."""
    global _caller_label
    if _caller_label is None:
        unit = os.environ.get("PMB_UNIT", "").strip()
        arg0 = os.path.basename(sys.argv[0] if sys.argv else "").strip()
        if arg0.endswith(".py"):
            arg0 = arg0[:-3]
        _caller_label = (unit or arg0 or f"pid:{os.getpid()}")[:64]
    return _caller_label


#: The endpoint classes the meter counts in. WHICH READS COST THE KEY, not just how many: one
#: process tripped on nonced book reads while a listing-page collector at the same rate
#: never did, so a per-minute total alone cannot name the rule.
#: `book_origin` is the expensive one — a `_=` nonce is a deliberate Cloudflare cache MISS.
ENDPOINT_CLASSES = ("book_origin", "book_cdn", "list", "orders", "account", "other")


def endpoint_class(method_path: str, nonce: bool = False) -> str:
    """One of `ENDPOINT_CLASSES` for a `"METHOD /path"` (or bare path) string.

    `nonce` is the caller's own fact about the request it is about to make (the `_=` cache-bust
    param), not a guess from the path: the query string never reaches this meter.
    """
    path = str(method_path or "").split(" ")[-1].lower()
    if "/book" in path:
        return "book_origin" if nonce else "book_cdn"
    if "/orders" in path:
        return "orders"
    if "/markets" in path or "/events" in path:
        return "list"
    if "/portfolio" in path or "/positions" in path or "/activities" in path:
        return "account"
    return "other"


def _minute_classes(count: Any) -> dict[str, int]:
    """One minute's per-class counts. ⛔ A state file written before the classes existed holds a
    BARE INT for the minute; it reads as `other`, never as a dropped hour."""
    if isinstance(count, dict):
        out = {}
        for cls, n in count.items():
            try:
                out[str(cls)] = int(n)
            except (TypeError, ValueError):
                continue
        return out
    return {"other": int(count)}


def _prune_hour(hour: Any, now: float) -> dict[str, dict[str, dict[str, int]]]:
    """`hour` with every minute older than `_HOUR_MINUTES` dropped, so the dict stays bounded."""
    cutoff = int(now // 60) - _HOUR_MINUTES
    out: dict[str, dict[str, dict[str, int]]] = {}
    if not isinstance(hour, dict):
        return out
    for caller, minutes in hour.items():
        if not isinstance(minutes, dict):
            continue
        kept = {}
        for minute, count in minutes.items():
            try:
                if int(minute) > cutoff:
                    kept[str(int(minute))] = _minute_classes(count)
            except (TypeError, ValueError):
                continue
        if kept:
            out[str(caller)] = kept
    return out


def _count_grant(state: dict, now: float, cls: str = "other") -> None:
    hour = _prune_hour(state.get("hour"), now)
    minute = str(int(now // 60))
    classes = hour.setdefault(caller_label(), {}).setdefault(minute, {})
    classes[cls] = classes.get(cls, 0) + 1
    state["hour"] = hour


def _hour_counts(state: dict, now: float) -> dict[str, int]:
    return {caller: sum(sum(classes.values()) for classes in minutes.values())
            for caller, minutes in _prune_hour(state.get("hour"), now).items()}


def _hour_classes(state: dict, now: float, since_minute: Optional[int] = None) -> dict[str, int]:
    """endpoint class -> grants, across every caller. `since_minute` limits it to the minute
    buckets at or after that epoch minute."""
    out: dict[str, int] = {}
    for minutes in _prune_hour(state.get("hour"), now).values():
        for minute, classes in minutes.items():
            if since_minute is not None and int(minute) < since_minute:
                continue
            for cls, n in classes.items():
                out[cls] = out.get(cls, 0) + n
    return out


def hour_counts(*, path: Optional[str] = None,
                now: Optional[float] = None) -> dict[str, int]:
    """caller -> granted requests on this key in the last `_HOUR_MINUTES` minutes."""
    ts = time.time() if now is None else float(now)
    return _hour_counts(_read(path or state_path(), ts), ts)


def hour_counts_by_class(*, path: Optional[str] = None,
                         now: Optional[float] = None) -> dict[str, int]:
    """endpoint class -> granted requests on this key in the last `_HOUR_MINUTES` minutes."""
    ts = time.time() if now is None else float(now)
    return _hour_classes(_read(path or state_path(), ts), ts)


def hour_total(*, path: Optional[str] = None, now: Optional[float] = None) -> int:
    """Every caller's granted requests on this key in the last hour."""
    return sum(hour_counts(path=path, now=now).values())


def _meter_line(state: dict, now: float) -> str:
    counts = _hour_counts(state, now)
    top = " · ".join(f"{c} {n:,}"
                     for c, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:3])
    classes = " · ".join(f"{c} {n:,}"
                         for c, n in sorted(_hour_classes(state, now).items(),
                                            key=lambda kv: (-kv[1], kv[0])))
    return (f" — 60-min count on this key: {sum(counts.values()):,}"
            + (f" (top: {top})" if top else "")
            + (f" classes: {classes}" if classes else ""))


class _StateLock:
    """Exclusive advisory lock on `<path>.lock`, held across the read-modify-write. Bounded, and a
    timeout returns False; token acquisition refuses without reading or spending shared state."""

    def __init__(self, path: str) -> None:
        self._lock_path = f"{path}.lock"
        self._fd: int | None = None

    def __enter__(self) -> bool:
        os.makedirs(os.path.dirname(self._lock_path) or ".", exist_ok=True)
        try:
            self._fd = os.open(self._lock_path, os.O_CREAT | os.O_RDWR, _STATE_MODE)
        except OSError as exc:
            log.error(f"venue_budget: cannot open {self._lock_path} ({exc}) — proceeding "
                      f"without shared-state access")
            return False
        deadline = time.monotonic() + _LOCK_TIMEOUT_S
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except (BlockingIOError, InterruptedError):
                if time.monotonic() >= deadline:
                    log.error(f"venue_budget: {self._lock_path} held >{_LOCK_TIMEOUT_S}s — a "
                              f"shared-state access refused")
                    os.close(self._fd)
                    self._fd = None
                    return False
                time.sleep(VENUE_BUDGET_POLL_S)

    def __exit__(self, *exc: Any) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None


# ── the bucket ───────────────────────────────────────────────────────────────────────────────

def _in_canary(state: dict, now: float) -> bool:
    """True inside the `VENUE_CANARY_S` window that follows a lapsed ban's `until`."""
    try:
        until = float((state.get("ban") or {}).get("until") or 0.0)
    except (TypeError, ValueError):
        return False
    return bool(until) and until <= now < until + VENUE_CANARY_S


def _canary_spent(state: dict) -> bool:
    return (state.get("ban") or {}).get("canary_ts") is not None


def _in_post_ban_window(state: dict, now: float) -> bool:
    """True inside the slow-pace window that follows the post-lift canary. The LEASE is permanent;
    this window only selects `VENUE_COLLECTOR_LEASE_REQ_PER_S` over the ordinary pace."""
    try:
        until = float((state.get("ban") or {}).get("until") or 0.0)
    except (TypeError, ValueError):
        return False
    open_s = until + VENUE_CANARY_S
    return bool(until) and open_s <= now < open_s + VENUE_COLLECTOR_LEASE_S


def _collector_pace(state: dict, now: float) -> float:
    """Requests per second for the one collector holding the lease on this bucket."""
    return (VENUE_COLLECTOR_LEASE_REQ_PER_S if _in_post_ban_window(state, now)
            else VENUE_COLLECTOR_REQ_PER_S)


def _pid_alive(pid: int) -> bool:
    """`PermissionError` counts as alive: another unix user's process is still a process."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return True
    return True


def _claim_collector_lease(state: dict, now: float, me: int, prio: int,
                           waiting: dict[str, list]) -> tuple[bool, bool]:
    """`(may_draw, lease_changed)` for a collector asking to draw on this bucket.

    ONE collector process at a time, always. The rule, in order:

    * the lease is FREE (no holder, a dead holder, or one that has not drawn for
      `VENUE_COLLECTOR_IDLE_S`) and no LIVE waiter of a more important class is registered → take it;
    * it is HELD by an active holder of a LESS important class (numerically higher `prio`) → take it,
      so a seat-bearing read never queues behind a 70-minute bulk pass. The displaced holder simply
      finds the lease held on its next poll and waits like anyone else;
    * otherwise → WAIT in place, most-important class first.

    ⛔ NOBODY IS REFUSED BY THE LEASE except at the wait ceiling. A `VenueRefused` no producer
    catches is a failed unit every tick, so a collector
    waits — and only after `VENUE_COLLECTOR_WAIT_MAX_S` of CONTINUOUS waiting does it give the pass
    up for this tick.

    `may_draw=False` means "wait, try again later" and the caller writes nothing but its own waiter
    re-stamp (at most once per half `VENUE_WAITER_TTL_S`).
    """
    lease = state.get("ramp_lease") if isinstance(state.get("ramp_lease"), dict) else None
    pid, label, last, held_prio = me, "?", 0.0, prio
    if lease is not None:
        try:
            pid, label = int(lease["pid"]), str(lease.get("label", "?"))
            last = float(lease.get("last_grant_ts") or 0.0)
            held_prio = int(lease.get("prio", PRIO_COLLECTOR))
        except (TypeError, ValueError, KeyError):
            lease, pid = None, me         # unusable lease = nobody holds it
    if lease is not None and pid != me:
        free = not _pid_alive(pid) or (now - last) >= VENUE_COLLECTOR_IDLE_S
        higher_waiter = any(int(w[0]) < prio for w_pid, w in waiting.items() if w_pid != str(me))
        # ⛔ ONE CEILING ABOVE BOTH WAITING RETURNS [I-LEASE-REFUSAL-SKIP]. Waiting behind a more
        # important WAITER (`free and higher_waiter`) is the same wait as waiting behind the
        # HOLDER, and a pass that alternated between the two never reached the ceiling at all.
        if (free and higher_waiter) or (not free and held_prio <= prio):
            waited = now - _waiting_since(waiting, str(me), now)
            if waited >= VENUE_COLLECTOR_WAIT_MAX_S:
                raise LeaseYield(f"collector lease: waited {waited:.0f} s behind {label} "
                                 f"— giving up this pass")
            return False, False          # wait in place: most-important class first
    if lease is not None and now - last < 1.0 / _collector_pace(state, now):
        return False, False              # paced: no token, no write
    state["ramp_lease"] = {"pid": me, "label": caller_label(), "prio": int(prio), "ts": now,
                           "last_grant_ts": last}
    return True, lease is None or pid != me


def _waiting_since(waiting: dict[str, list], me: str, now: float) -> float:
    """When this process STARTED waiting, per the registry (an entry without it starts now)."""
    entry = waiting.get(me)
    try:
        return float(entry[2]) if entry is not None and len(entry) > 2 else now
    except (TypeError, ValueError):
        return now


def _bucket_rate(path: str) -> tuple[float, float]:
    """`(refill rate, depth)` for one bucket. ⛔ THE RATE IS PER KEY. Only the `default` label is
    the MAKER's key and holds the maker's own pacer (`VENUE_BUDGET_REQ_PER_S`); a bound-source
    bucket is a collector key, which the venue limits separately, and
    refills at `VENUE_COLLECTOR_BUCKET_REQ_PER_S`.
    The post-lift ramp and the collector lease pace on top of this, unchanged.
    """
    if os.path.basename(path) == STATE_NAME.format(label=DEFAULT_SOURCE_LABEL):
        return VENUE_BUDGET_REQ_PER_S, float(VENUE_BUDGET_BURST)
    return VENUE_COLLECTOR_BUCKET_REQ_PER_S, float(VENUE_BUDGET_BURST)


def _refill(state: dict, now: float, path: str) -> None:
    if _in_canary(state, now):
        # No refill in the window: one token if the canary is unspent, none once it is. A state
        # file written before the canary existed has no `canary_ts` and so reads as available.
        state["tokens"] = 0.0 if _canary_spent(state) else 1.0
        state["ts"] = now
        return
    try:
        until = float((state.get("ban") or {}).get("until") or 0.0)
    except (TypeError, ValueError):
        until = 0.0
    rate, cap = _bucket_rate(path)
    if until and now >= until + VENUE_CANARY_S:
        if float(state["ts"]) < until + VENUE_CANARY_S:
            # Refill resumes AT the end of the canary window, never from the pre-ban stamp (which
            # would hand out a full bucket the moment the window closes).
            state["ts"] = until + VENUE_CANARY_S
        if now < until + VENUE_CANARY_S + VENUE_RAMP_S:
            # ⛔ THE RAMP IS A CEILING, NEVER A LIFT. A just-tripped key must not refill faster
            # than its own steady state, so the ramp takes the LOWER of the two rates (the bucket
            # rates are now below the ramp's original 2/s on both keys); the depth still drops.
            rate, cap = min(VENUE_RAMP_REQ_PER_S, rate), float(VENUE_RAMP_BURST)
    elapsed = max(0.0, now - float(state["ts"]))
    state["tokens"] = min(cap, float(state["tokens"]) + elapsed * rate)
    state["ts"] = now


def _live_waiters(state: dict, now: float) -> dict[str, list]:
    """Registered waiters that have re-stamped themselves within `VENUE_WAITER_TTL_S`.

    `[prio, last_seen, first_seen]`. A file written before the wait ceiling existed carries
    `[prio, last_seen]` and reads with `first_seen = last_seen` — never an error.
    """
    out: dict[str, list] = {}
    for pid, entry in (state.get("waiting") or {}).items():
        try:
            prio, seen = int(entry[0]), float(entry[1])
            first = float(entry[2]) if len(entry) > 2 else seen
        except (TypeError, ValueError, IndexError, KeyError):
            continue
        if now - seen <= VENUE_WAITER_TTL_S:
            out[str(pid)] = [prio, seen, first]
    return out


def ban_active(*, path: Optional[str] = None, now: Optional[float] = None) -> Optional[float]:
    """`banned_until` while a ban stands, else None. The read side, for status lines."""
    ts = time.time() if now is None else float(now)
    state = _read(path or state_path(), ts)
    return _ban_until(state, ts)


def _ban_until(state: dict, now: float) -> Optional[float]:
    try:
        until = float((state.get("ban") or {}).get("until", 0.0))
    except (TypeError, ValueError):
        return None
    return until if until > now else None


def record_ban(reason: str, *, path: Optional[str] = None, now: Optional[float] = None,
               retry_after: Optional[float] = None) -> dict:
    """Set (or EXTEND) the ban after a 429/CF-1015. Returns the ban record now in force.

    `retry_after` is the edge's OWN stated block (see `retry_after_s`); we hold it `×
    VENUE_RETRY_AFTER_MULT`, floored, because the lift itself has to be quiet. Without the header
    we fall back to `VENUE_BAN_BASE_S`.

    A re-trip inside `VENUE_BAN_RESET_S` ADDS `VENUE_RETRIP_STEP_S` per previous trip, capped at
    `VENUE_RETRIP_CAP_S` — it does not double (2026-09-14: doubling turned six ten-second edge
    blocks into 8/16/32/60-minute outages). `VENUE_BREAKER_TRIPS` trips inside the window hold
    `VENUE_BREAKER_S` once and then start the counter again; that is the only long hold. A ban more
    than `VENUE_BAN_RESET_S` after the previous one starts again from the header (or the base).
    """
    p = path or state_path()
    ts = time.time() if now is None else float(now)
    with _StateLock(p) as locked:
        if not locked:
            raise VenueRefused(f"venue budget: lock unavailable — ban not recorded ({p})")
        state = _read(p, ts)
        prev = state.get("ban") or {}
        try:
            last = float(prev.get("last", 0.0))
            n = int(prev.get("n", 0))
        except (TypeError, ValueError):
            last, n = 0.0, 0
        # The edge's own stated block, floored and CAPPED (a Retry-After of a day must not write a
        # three-day box ban the maker cannot cancel out of; inf/nan read as the floor).
        stated = None
        if retry_after is not None and retry_after == retry_after and retry_after < float("inf"):
            stated = min(max(VENUE_BAN_FLOOR_S, retry_after * VENUE_RETRY_AFTER_MULT),
                         VENUE_BAN_MAX_S)
        elif retry_after is not None:
            stated = VENUE_BAN_FLOOR_S
        if last and (ts - last) <= VENUE_BAN_RESET_S:
            n += 1
            if n >= VENUE_BREAKER_TRIPS:
                # THE ONE LONG HOLD. Trips this dense mean the pace itself is wrong, not that a
                # single block needs waiting out; `n` starts again so the next trip is a #1.
                duration, trip, n = max(VENUE_BREAKER_S, stated or 0.0), n, 0
                source = (f"breaker: {VENUE_BREAKER_TRIPS} trips "
                          f"in {VENUE_BAN_RESET_S / 60:.0f} min")
            else:
                # A step per previous trip, capped — and never resuming before the edge's OWN
                # stated block on THIS 429 (a header longer than the cap still governs).
                duration = min(max(VENUE_BAN_FLOOR_S, stated or 0.0)
                               + VENUE_RETRIP_STEP_S * (n - 1), VENUE_RETRIP_CAP_S)
                duration = max(duration, stated or 0.0)
                trip = n
                source = f"re-trip #{n} +{VENUE_RETRIP_STEP_S * (n - 1):.0f}s"
        elif stated is not None:
            duration, n, trip = stated, 1, 1
            source = f"retry-after {retry_after:g} x{VENUE_RETRY_AFTER_MULT:g}"
        else:
            duration, n, trip = VENUE_BAN_BASE_S, 1, 1
            source = "no retry-after"
        ban = {
            "until": max(ts + duration, float(prev.get("until", 0.0) or 0.0)),
            "duration_s": duration,
            "n": n,
            "last": ts,
            "reason": " ".join(str(reason).split())[:300],
            # The class mix in the minute the ban landed in, kept in the STATE so the rule can be
            # read back from the bucket files alone tomorrow.
            # The ban's own minute bucket AND the one before it — the grants of the last ~60 s,
            # whole-minute buckets being all the meter keeps.
            "last_minute_classes": _hour_classes(state, ts, since_minute=int(ts // 60) - 1),
        }
        state["ban"] = ban
        meter = _meter_line(state, ts)
        _write(p, state)
    log.warning(f"venue_budget: ban#{trip} until {_iso(ban['until'])} "
                f"({duration:.0f} s: {source}) — {ban['reason']}{meter}")
    return ban


def maker_live_reason(*, now: Optional[float] = None) -> Optional[str]:
    """Why a collector must not draw, or None. A scan failure reads as LIVE — when we cannot tell,
    we do not burst. Memoised for `MAKER_LIVE_CACHE_S` so a poll loop is not a directory scan loop.

    ⛔ Scans `config.HEARTBEAT_DIR`, the directory the makers WRITE (audit F2/C7): an empty scan
    reads as NOT live, so a default-directory scan would burst beside a real maker believing it
    had deferred.
    """
    global _maker_live_memo
    ts = time.time() if now is None else float(now)
    seen, cached = _maker_live_memo
    if seen and (ts - seen) < MAKER_LIVE_CACHE_S:
        return cached
    try:
        from bot.core import config
        from bot.core import heartbeat
        reason = None
        for d in heartbeat.scan(config.HEARTBEAT_DIR, now=ts):
            if (d.fields or {}).get("mode") != "real":
                continue
            if d.state == "ok" or (d.state == "stale"
                                   and getattr(d, "pid_running", None) is not False):
                reason = f"a real maker heartbeat is {d.state} ({d.name})"
                break
    except Exception as exc:                # cannot verify = refuse
        reason = (f"heartbeat scan failed ({type(exc).__name__}) — cannot verify that no real "
                  f"maker is live")
    _maker_live_memo = (ts, reason)
    return reason


def try_take(prio: int, *, path: Optional[str] = None, now: Optional[float] = None,
             pid: Optional[int] = None, register: bool = True,
             allow_during_ban: bool = False, endpoint: str = "", nonce: bool = False) -> bool:
    """One locked attempt at a token. True = taken and this process deregistered as a waiter.

    Refuses — and, with `register`, records itself as a waiter of class `prio` so the classes below
    it yield — when the bucket is empty OR a higher-priority process is already waiting.
    Raises `VenueBanned` while the ban stands — for EVERY class, whatever its priority.

    ⛔ `allow_during_ban` IS THE ONLY EXEMPTION, AND IT IS AN EXPLICIT OPT-IN. A hand close, cancel, pos-guard or reconcile is the operator's own decision
    about their own position, and a rail that refuses it turns a rate limit into an unmanageable
    position — but that argument belongs to the SIX named tools, never to the default class, which
    is also what an unattended producer gets when nobody passes a priority. An exempt call is still
    METERED (it must not be the burst that extends the ban) and says so once per ban.
    """
    p = path or state_path()
    ts = time.time() if now is None else float(now)
    me = str(os.getpid() if pid is None else pid)
    with _StateLock(p) as locked:
        if not locked:
            raise VenueRefused(f"venue budget: lock unavailable — request refused ({p})")
        state = _read(p, ts)
        until = _ban_until(state, ts)
        if until is not None:
            ban = state.get("ban") or {}
            if not allow_during_ban:
                raise VenueBanned(until, str(ban.get("reason", "rate-limited")))
            key = (p, int(ban.get("n", 0) or 0))
            if key not in _operator_ban_logged:
                _operator_ban_logged.add(key)
                log.warning(f"venue_budget: operator call during box ban#{key[1]} "
                            f"(until {_iso(until)}) — metered, not refused")
        _refill(state, ts, p)
        waiting = _live_waiters(state, ts)
        lease_dirty, lease_held_elsewhere = False, False
        # Every grant requires the lock, including recovery calls that may bypass a ban.
        if locked and prio >= PRIO_COLLECTOR_HIGH:
            may_draw, lease_dirty = _claim_collector_lease(state, ts, int(me), prio, waiting)
            lease_held_elsewhere = not may_draw
        blocked_by = min((w[0] for w_pid, w in waiting.items() if w_pid != me), default=None)
        higher_waiting = blocked_by is not None and blocked_by < prio
        got = (float(state["tokens"]) >= 1.0 and not higher_waiting
               and not lease_held_elsewhere)
        # A waiter re-stamps at most once per half TTL: the registration only has to stay FRESH,
        # and a 20 Hz poll loop rewriting the file every tick is 20 writes/s per waiter for one
        # unchanged fact.
        stale_stamp = ts - float((waiting.get(me) or [0, 0.0])[1]) >= VENUE_WAITER_TTL_S / 2
        if got:
            lease = state.get("ramp_lease")
            if (prio >= PRIO_COLLECTOR_HIGH and isinstance(lease, dict)
                    and lease.get("pid") == int(me)):
                lease["last_grant_ts"] = ts     # the lease's pace stamp, on the token's own write
                lease_dirty = True
            state["tokens"] = float(state["tokens"]) - 1.0
            waiting.pop(me, None)
            _count_grant(state, ts, endpoint_class(endpoint, nonce))
            if _in_canary(state, ts):
                ban = state.get("ban") or {}
                ban["canary_ts"] = ts
                state["ban"] = ban
                log.warning(f"venue_budget: canary after ban#{int(ban.get('n', 0) or 0)} "
                            f"granted to prio {prio}")
        elif register and (me not in waiting or stale_stamp):
            waiting[me] = [int(prio), ts, _waiting_since(waiting, me, ts)]
        elif not got and not lease_dirty:
            state["waiting"] = waiting
            return False                    # nothing changed; skip the write entirely
        state["waiting"] = waiting
        if locked:
            _write(p, state)
        return got


async def acquire(prio: int, *, beside_maker: bool = False, source_ip: str = "",
                  allow_during_ban: bool = False, path: Optional[str] = None,
                  endpoint: str = "", nonce: bool = False) -> None:
    """Block until this process may issue ONE Poly US request. The whole gate, in call order:
    collector-beside-a-maker → ban → priority-respecting token (the last two are `try_take`'s,
    under one lock).

    Raises `MakerLive` (a collector with `beside_maker=False` on the shared route) or `VenueBanned`
    (every class, unless `allow_during_ban` — see `try_take`).

    ⛔ AN ABANDONED WAIT IS NOT DEREGISTERED. A raise (a ban arming mid-wait, a cancelled task)
    leaves this process's waiter entry behind to expire on its own after `VENUE_WAITER_TTL_S`. The
    locked drop it replaces cost a lock acquisition on the exception path — inside the maker's
    teardown, where the loop is already unwinding — to save at most one second of the classes below
    us yielding. Error direction: a collector waits up to one extra second.

    ⛔ THE LOCKED SECTION RUNS OFF THE EVENT LOOP (`asyncio.to_thread`). `flock` + read + atomic
    write is blocking work, and the maker awaits this before every request: on the loop thread a
    contended lock would stall the quote cadence and the kill-switch check together
   .

    ⛔ THE MAKER GATE IS PER SOURCE ADDRESS. A collector that BOUND a source address is not on the
    maker's IP: the venue's public limit and Cloudflare's edge are both per IP, and the maker's API
    key never leaves the maker's IP, so that collector's reads cannot cost the maker a token or a
    ban. Only a collector on the DEFAULT route (blank `source_ip`, the shared primary IP) has to
    stand down while a real maker is beating.
    """
    p = path or state_path(source_ip)
    if prio >= PRIO_COLLECTOR_HIGH and not beside_maker and not source_ip:
        reason = maker_live_reason()
        if reason:
            raise MakerLive(reason)
    while not await asyncio.to_thread(try_take, prio, path=p,
                                      allow_during_ban=allow_during_ban,
                                      endpoint=endpoint, nonce=nonce):
        await asyncio.sleep(VENUE_BUDGET_POLL_S)


# ── what counts as "we were rate-limited" ────────────────────────────────────────────────────

#: Cloudflare's own rate-limit page: `error code: 1015`, or its "Access denied" block naming
#: Cloudflare. Served in FRONT of the origin, so the STATUS can be 403/429/503 — the marker is the
#: signal, not the code [venue-reference § Poly US rate limits].
_CF_MARKER_RE = re.compile(r"error\s*(?:code:?\s*)?1015"
                           r"|access denied.{0,400}cloudflare"
                           r"|cloudflare.{0,400}access denied", re.IGNORECASE | re.DOTALL)

#: ⛔ NOT A RATE LIMIT, WHATEVER IT SAYS. "Global Rate Limit Exceeded" on the ORDER endpoints is a
#: DOCUMENTED 5-second order-processing stopgap during high-latency periods (new orders and
#: cancel/replace, not standalone cancels) — the venue's own docs say do not throttle down in
#: response to it [venue-reference § Poly US rate limits]. Banning the whole box for 4 minutes
#: because one create met a 5-second stopgap would cost a quoting evening for a non-event.
_ORDER_STOPGAP_RE = re.compile(r"global rate limit exceeded", re.IGNORECASE)


def _exc_text(exc: BaseException) -> str:
    """Class name + message + the response BODY, which is where Cloudflare's marker lives."""
    parts = [type(exc).__name__, str(exc)]
    body = getattr(exc, "body", None)
    if body is not None:
        parts.append(str(body))
    resp = getattr(exc, "response", None)
    text = getattr(resp, "text", None)
    if isinstance(text, str):
        parts.append(text)
    return " ".join(parts)[:8000]


def _status_of(exc: BaseException) -> Optional[int]:
    for candidate in (getattr(exc, "status_code", None),
                      getattr(getattr(exc, "response", None), "status_code", None)):
        if isinstance(candidate, int):
            return candidate
    return None


#: Response headers worth keeping when the venue limits us, in print order. ⛔ RESPONSE ONLY, and
#: never `set-cookie`: a request header carries the API key.
_LIMIT_HEADERS = ("retry-after", "cf-ray", "cf-mitigated", "cf-cache-status", "date")


def _response_headers(exc: BaseException) -> dict[str, str]:
    """`exc`'s RESPONSE headers, lower-cased, or `{}`. ⛔ Never the request's: it carries the key."""
    headers = getattr(getattr(exc, "response", None), "headers", None)
    try:
        return {str(k).lower(): str(v) for k, v in dict(headers).items()}
    except (TypeError, ValueError, AttributeError):
        return {}


def limit_headers(exc: BaseException) -> str:
    """`k=v k=v` for the limit-bearing RESPONSE headers on `exc`, or `""` when it carries none.

    We have been guessing the block duration from re-ban timings; the edge usually states it
    (`retry-after`) and always identifies the decision (`cf-ray`, `cf-mitigated`). Pure: it reads
    the `httpx.Response` the SDK's `APIStatusError` already holds and decides nothing.
    """
    seen = _response_headers(exc)
    out = [f"{k}={seen[k]}" for k in _LIMIT_HEADERS if k in seen]
    out += [f"{k}={v}" for k, v in sorted(seen.items())
            if k not in _LIMIT_HEADERS and ("ratelimit" in k or "rate-limit" in k)]
    return " ".join(out)


def failing_request(exc: BaseException) -> str:
    """`METHOD /path` for the request that earned `exc`, `nonce` appended when it carried our
    `_=` cache-bust param, or `""`.

    WHICH READ EARNED IT, not just when. Measured: ONE process tripped on nonced book reads while
    a listing-page collector at the same rate never did — the
    other nonces every book read into a Cloudflare cache MISS. Never the query string itself: it
    can carry ids we do not want in a log line.
    """
    request = getattr(getattr(exc, "response", None), "request", None)
    url = getattr(request, "url", None)
    if url is None:
        return ""
    nonce = " nonce" if "_=" in str(getattr(url, "query", b"") or b"") else ""
    return f"{getattr(request, 'method', '?')} {getattr(url, 'path', '?')}{nonce}"


def retry_after_s(exc: BaseException) -> Optional[float]:
    """The edge's own `Retry-After`, in seconds, or None (absent, an HTTP-date form, unparseable).

    RFC 7231 allows a date form; we do not translate it — a clock-skewed guess at a block length is
    worse than the no-header fallback.
    """
    raw = _response_headers(exc).get("retry-after")
    try:
        return float(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def is_cf_ban(exc: BaseException) -> bool:
    """Is this exception evidence of a **Cloudflare EDGE ban** — the only thing that arms the
    ladder? ONE signal: the 1015 / access-denied marker anywhere in the message or body, at any
    status (the edge serves its page in front of the origin, so the exception class is not
    reliable).

    ⛔ A BARE 429 IS NOT THIS. The venue's own guidance for its
    app limit is "stop, wait ≥1 s, exponential backoff" — a one-second box pause, not a
    four-minute stop that also takes the maker down. See `is_app_limit`.

    ⛔ AND NEVER OUR OWN REFUSAL. `VenueBanned`/`MakerLive` carry the ban text by construction;
    reading one back as venue evidence is a ban that re-arms itself off traffic that never left
    the box — `venue_backoff.arm`'s own stuck-latch trap, one layer down.
    """
    if isinstance(exc, VenueRefused):
        return False
    return bool(_CF_MARKER_RE.search(_exc_text(exc)))


def is_app_limit(exc: BaseException) -> bool:
    """Is this a bare HTTP 429 that is neither a Cloudflare page nor the documented
    order-endpoint stopgap? It signals a pause, not a verified quota. Ban nothing.

    A 400 or a 503 that merely MENTIONS a rate limit is the origin talking about an order or an
    outage and is neither.
    """
    if isinstance(exc, VenueRefused) or _status_of(exc) != 429:
        return False
    text = _exc_text(exc)
    return not _ORDER_STOPGAP_RE.search(text) and not _CF_MARKER_RE.search(text)


def app_limit_pause(*, path: Optional[str] = None, now: Optional[float] = None,
                    detail: str = "") -> float:
    """Drain the bucket and date it `VENUE_APP_LIMIT_PAUSE_S` forward. Returns the resume time.

    No new state and no new refusal: `_refill` gives 0 tokens for a bucket stamped in the future,
    so every class — maker included — waits the pause out inside its ordinary `acquire` loop.
    """
    p = path or state_path()
    ts = time.time() if now is None else float(now)
    resume = ts + VENUE_APP_LIMIT_PAUSE_S
    with _StateLock(p) as locked:
        if not locked:
            raise VenueRefused(f"venue budget: lock unavailable — pause not recorded ({p})")
        state = _read(p, ts)
        state["tokens"], state["ts"] = 0.0, resume
        _write(p, state)
    log.warning(f"venue_budget: venue app limit (429) — box paused "
                f"{VENUE_APP_LIMIT_PAUSE_S:.0f}s, bucket drained; NO ban armed"
                + (f" [{detail}]" if detail else ""))
    return resume


# ── the insertion point ──────────────────────────────────────────────────────────────────────

def install(sdk: Any, *, prio: int, beside_maker: bool = False, source_ip: str = "",
            allow_during_ban: bool = False) -> Any:
    """Meter every REST request this SDK instance makes. Idempotent per instance.

    `AsyncPolymarketUS._request` is the ONE funnel: `get`/`post`/`delete` and every resource
    (`markets`, `orders`, `portfolio`, …) route through it, so producers that page
    `client._sdk.markets.list` directly are metered too. Wrapping the INSTANCE, not the class,
    keeps each client's priority its own.

    `source_ip` selects the bucket (and the ban flag) — the address this SDK's transport is bound
    to, never a second guess at it.
    """
    original = getattr(sdk, "_request", None)
    if original is None or getattr(original, "_venue_budget", False):
        return sdk
    path = state_path(source_ip)

    async def _budgeted(*args: Any, **kwargs: Any) -> Any:
        # WHICH read this token is for. The SDK funnel is `_request(method, path, *, query=…)` on
        # a BOUND instance, so `args` is `(method, path)`; `_` in the query is our cache-bust
        # nonce (a deliberate CDN miss), which is the class that trips the key.
        endpoint = " ".join(str(a) for a in args[:2])
        query = kwargs.get("query") or {}
        nonce = "_" in query if isinstance(query, dict) else False
        await acquire(prio, beside_maker=beside_maker, source_ip=source_ip,
                      allow_during_ban=allow_during_ban, path=path,
                      endpoint=endpoint, nonce=nonce)
        try:
            return await original(*args, **kwargs)
        except BaseException as exc:
            # ⛔ TWO DIFFERENT ANSWERS TO TWO DIFFERENT SIGNALS. A Cloudflare page means the EDGE
            # has us and every further request extends it (minutes). A bare 429 signals a pause,
            # without establishing the quota; the documented remedy starts with a second (venue-reference
            # § Poly US rate limits) — arming the ladder on it would stop the maker for four
            # minutes over one over-eager second.
            # The venue's own headers and the failing request, kept ahead of the CF HTML so they
            # survive the reason cap.
            detail = " ".join(x for x in (limit_headers(exc), failing_request(exc)) if x)
            if is_cf_ban(exc):
                text = f"{type(exc).__name__}: {exc}"
                record_ban(f"{detail} | {text}" if detail else text, path=path,
                           retry_after=retry_after_s(exc))
            elif is_app_limit(exc):
                app_limit_pause(path=path, detail=detail)
            raise

    _budgeted._venue_budget = True          # type: ignore[attr-defined]
    sdk._request = _budgeted
    return sdk

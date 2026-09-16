"""
bot/core/venue_backoff.py
─────────────────────────
The shared VENUE-HEALTH LATCH — one small file on disk that lets every producer on this box
hold itself back while the venue (or Cloudflare in front of it) is refusing us.

WHY THIS EXISTS
───────────────
On 2026-08-26 a sick venue route started returning 500s. Nothing in the fleet knew that, so:

  · `<unit>` woke on its timer and walked the events crawl, taking `_RETRY_BACKOFF_S`
    (2s, 5s) retries on EVERY page before giving up;
  · `<unit>` and `<unit>` did the same on their own cadences;
  · each unit then came back on the NEXT tick and did it again.

Retrying an erroring venue on schedule, from three processes, is exactly the burst shape
Cloudflare bans the box IP for — and it did (**1015**). The launch skill §1 records what that
costs: CF bans near ~10 req/s sustained, the ban **outlives its cause by 15–25 minutes**, and
**every request issued during the ban extends it**. So the fleet's blind retries were not merely
useless, they were actively prolonging the outage they were reacting to.

The operator's directive is the shape of this module: producers **hold themselves with backoff**
when the venue is erroring. Not stop (a stopped producer is the T0-2 failure mode — it silently
stays stopped), and not retry blindly (that is the ban). Hold, then resume on the next timer tick.

WHAT THIS IS AND IS NOT
───────────────────────
- It is **advisory and opt-in on the READ side.** Writing the latch is safe for anyone; only a
  client constructed with `obey_venue_backoff=True` ever *obeys* it. The live maker constructs
  its client with the default (False) and is therefore never starved by a producer's latch — its
  budget discipline is its own, and a producer's observation must not stop a real-money run from
  reading its own book.
- It is **not** a rate limiter, a circuit breaker with half-open probing, or a retry policy. It is
  one boolean-with-a-deadline that several unrelated processes can agree on.
- It is **fail-OPEN by construction**: a missing, unreadable, corrupt or expired latch means "no
  backoff", i.e. business as usual. That is the right direction here — the cost of a false latch
  is a lost producer pass (self-healing next tick), but the cost of a latch that cannot be cleared
  is a permanently blind flow map, and `poly_event_risk` gate 7 FAILS CLOSED on a stale map, so a
  stuck latch would block real-money launches. A corrupt file is therefore treated as ABSENT — but
  LOUDLY (`log.error`), never silently, per this repo's cannot-verify-is-not-flat rule.

THE FILE
────────
`logs/venue_backoff.json`, written atomically+durably through `bot.core.durable` (the same rail
the loss cap and heartbeat use — SIGKILL is the death this box actually suffers):

    {"kind": "ban", "until": 1756270000.0, "since": 1756268500.0,
     "duration_s": 1500.0, "extends": 0, "reason": "RateLimitError: error code: 1015",
     "written_by": "poly_market_screen", "written_at": 1756268500.0}

`kind` is ordered: `degraded` < `ban`. A degraded observation may never shorten or downgrade an
active ban, and `until` never moves BACKWARDS — two processes racing can only ever agree on a
longer hold, which is the safe direction against a limiter that punishes requests-during-ban.
"""
from __future__ import annotations

import os
import re
import sys
import time
from typing import Any, Optional

from bot.core import durable
from bot.core.logger import get_logger

log = get_logger(__name__)

try:  # exercised implicitly; the fallback is for a checkout without the SDK
    from polymarket_us.errors import (
        APIStatusError,
        InternalServerError,
        RateLimitError,
    )
except ImportError:
    APIStatusError = InternalServerError = RateLimitError = ()  # type: ignore[assignment,misc]

#: Default latch location. ABSOLUTE via `repo_path` — a relative operational path is how the loss
#: caps went silently inert (durable.py's own header), and producers run from assorted cwds.
LATCH_PATH = durable.repo_path("logs", "venue_backoff.json")

#: A CF-1015 ban outlives its cause by 15–25 min (launch skill §1). 25 = the top of that range:
#: undershooting means resuming INTO the ban, and every request during a ban extends it, so the
#: asymmetry says round up.
BAN_S = 25 * 60.0
#: A degraded (5xx) venue is not punishing us for asking — it is just broken. Shorter hold.
DEGRADED_S = 10 * 60.0
#: Exponential extension ceiling. Beyond an hour the timers have long since provided the retry
#: cadence this module is trying to imitate, and a longer hold only risks a stuck latch.
MAX_S = 60 * 60.0

#: N 5xx within M seconds, across ONE process's own reads, arms `degraded`.
DEGRADED_THRESHOLD = 5
DEGRADED_WINDOW_S = 5 * 60.0

BAN = "ban"
DEGRADED = "degraded"
_KIND_RANK = {DEGRADED: 1, BAN: 2}

#: Cloudflare's rate-limit page says `error code: 1015` in the BODY of an otherwise ordinary
#: 4xx/403/429 — the status alone does not identify it, so the text is scanned too. Word-bounded
#: so a market id or a quantity that happens to contain 1015 cannot arm a 25-minute hold.
_CF_1015_RE = re.compile(r"\b1015\b")
_RATE_TEXT_RE = re.compile(r"rate.?limit|too many requests", re.IGNORECASE)


class VenueBackoff(Exception):
    """Raised INSTEAD of issuing a venue request, while the latch is active.

    A distinct type on purpose. Producers must be able to tell "we deliberately did not ask" from
    "we asked and it failed" — the first is a clean abandon that the systemd timer will retry, the
    second is a real error that deserves its retry budget. Collapsing them is how a hold becomes a
    hammer: a `VenueBackoff` caught by a generic `except Exception` retry loop would sleep and try
    again, which is the exact behaviour this module exists to remove.
    """

    def __init__(self, kind: str, until: float, remaining_s: float, reason: str) -> None:
        super().__init__(
            f"venue backoff active ({kind}): {remaining_s:.0f}s remaining — not spending a "
            f"request. Latched because: {reason}")
        self.kind = kind
        self.until = until
        self.remaining_s = remaining_s
        self.reason = reason


# ── reading ──────────────────────────────────────────────────────────────────────────────────

#: (stat key) -> parsed dict|None. Producers call `read_latch` once per venue read, so the parse
#: is memoised on the file's identity; the EXPIRY is still evaluated against `now` every call, so
#: a cached entry can never keep a lapsed latch alive.
_memo: tuple[Any, Any] = (None, None)


def _read_raw(path: str) -> Optional[dict]:
    """The parsed file, or None when absent/corrupt/unrecognised. Corrupt is LOUD."""
    global _memo
    try:
        st = os.stat(path)
        key = (path, st.st_mtime_ns, st.st_size)
    except FileNotFoundError:
        return None
    except OSError as exc:
        log.error(f"venue_backoff: cannot stat {path} ({exc}) — treating as NO LATCH")
        return None
    if _memo[0] == key:
        return _memo[1]
    try:
        obj = durable.read_json_strict(path)
    except durable.StateCorrupt as exc:
        # Fail OPEN, loudly. See the module header: a latch that cannot be cleared blocks
        # real-money launches through gate 7, which is worse than a lost hold.
        log.error(f"venue_backoff: CORRUPT latch {path} ({exc}) — treating as NO LATCH. "
                  f"Delete it if this persists; producers are running unheld.")
        _memo = (key, None)
        return None
    if not isinstance(obj, dict) or obj.get("kind") not in _KIND_RANK:
        if obj is not None:
            log.error(f"venue_backoff: UNRECOGNISED latch shape in {path} "
                      f"({str(obj)[:200]}) — treating as NO LATCH")
        _memo = (key, None)
        return None
    try:
        obj["until"] = float(obj["until"])
    except (KeyError, TypeError, ValueError):
        log.error(f"venue_backoff: latch in {path} has no usable `until` "
                  f"({obj.get('until')!r}) — treating as NO LATCH")
        _memo = (key, None)
        return None
    _memo = (key, obj)
    return obj


def read_latch(path: Optional[str] = None, *, now: Optional[float] = None) -> Optional[dict]:
    """The ACTIVE latch, or None when there is none (absent, corrupt, or expired).

    Expiry is the whole retry mechanism: nothing clears the latch, it simply stops being true.
    That matters because the process that armed it may be long dead — a hold that needed an
    explicit release would outlive its writer and become the stuck latch described above.
    """
    ts = time.time() if now is None else float(now)
    obj = _read_raw(path or LATCH_PATH)
    if obj is None:
        return None
    return None if obj["until"] <= ts else obj


def raise_if_active(path: Optional[str] = None, *, now: Optional[float] = None) -> None:
    """Raise `VenueBackoff` when the latch is active. The read-side check, in one line."""
    ts = time.time() if now is None else float(now)
    latch = read_latch(path, now=ts)
    if latch is not None:
        raise VenueBackoff(latch["kind"], latch["until"], latch["until"] - ts,
                           str(latch.get("reason", "unrecorded")))


def describe(latch: Optional[dict], *, now: Optional[float] = None) -> str:
    """One human line for a log/report. `None` renders as the clear state, not as an empty string —
    a status rail that prints nothing when healthy is indistinguishable from one that is broken."""
    if latch is None:
        return "venue backoff: CLEAR"
    ts = time.time() if now is None else float(now)
    return (f"venue backoff: {latch['kind'].upper()} for another "
            f"{max(0.0, latch['until'] - ts) / 60:.1f} min "
            f"(armed by {latch.get('written_by', '?')}: {str(latch.get('reason', '?'))[:120]})")


# ── writing ──────────────────────────────────────────────────────────────────────────────────

def _writer_name() -> str:
    """Best-effort identity of the arming process, for the report line. Never raises."""
    try:
        return os.path.basename(sys.argv[0]) or "unknown"
    except Exception:  # a label is never worth an exception
        return "unknown"


def arm(kind: str, reason: str, *, path: Optional[str] = None,
        now: Optional[float] = None) -> dict:
    """Arm (or EXTEND) the latch. Returns the latch now in force.

    Three rules, all of which exist because several unrelated processes write this file:

    1. **A lower-severity observation never downgrades a higher-severity latch.** A stray 5xx seen
       during a CF ban must not replace a 25-minute ban with a 10-minute degraded hold — that
       would resume into the ban and extend it.
    2. **`until` never moves backwards.** Racing writers can only agree on a longer hold.
    3. **Same-severity re-arming EXTENDS exponentially**, capped at `MAX_S`. A venue still erroring
       when the hold lapses is a venue that needed a longer hold.
    """
    if kind not in _KIND_RANK:
        raise ValueError(f"venue_backoff.arm: unknown kind {kind!r}")
    p = path or LATCH_PATH
    ts = time.time() if now is None else float(now)
    cur = read_latch(p, now=ts)

    if cur is not None and _KIND_RANK[cur["kind"]] > _KIND_RANK[kind]:
        return cur                                          # rule 1

    base = BAN_S if kind == BAN else DEGRADED_S
    since, extends, duration = ts, 0, base
    if cur is not None and cur["kind"] == kind:              # rule 3
        try:
            prev = float(cur.get("duration_s", base))
        except (TypeError, ValueError):
            prev = base
        duration = min(max(prev, base) * 2.0, MAX_S)
        extends = int(cur.get("extends", 0) or 0) + 1
        try:
            since = float(cur.get("since", ts))
        except (TypeError, ValueError):
            since = ts

    until = ts + duration
    if cur is not None:
        until = max(until, cur["until"])                     # rule 2
    latch = {
        "kind": kind,
        "until": until,
        "since": since,
        "duration_s": duration,
        "extends": extends,
        "reason": " ".join(str(reason).split())[:300],
        "written_by": _writer_name(),
        "written_at": ts,
    }
    durable.write_json_durable(p, latch)
    global _memo
    _memo = (None, None)            # our own write invalidates the parse memo
    log.warning(f"venue_backoff: ARMED {kind} until +{ (until - ts) / 60:.1f} min "
                f"(extends={extends}) — {latch['reason']}")
    return latch


def clear(path: Optional[str] = None) -> None:
    """Remove the latch. For the operator and for tests — the normal release is EXPIRY."""
    global _memo
    _memo = (None, None)
    try:
        os.unlink(path or LATCH_PATH)
    except FileNotFoundError:
        pass


# ── classification ───────────────────────────────────────────────────────────────────────────

def _error_text(exc: BaseException) -> str:
    parts = [type(exc).__name__, str(exc)]
    body = getattr(exc, "body", None)
    if body is not None:
        parts.append(str(body))
    return " ".join(parts)[:4000]


def classify(exc: BaseException) -> Optional[str]:
    """`BAN`, `DEGRADED`, or None (not a venue-health signal).

    ⚠️ A CF-1015 does NOT reliably arrive as `RateLimitError`. Cloudflare serves its own page in
    front of the origin, and the recorded 1015s on this box came back with the marker in the BODY
    of a 403/429/503 — so the TEXT is authoritative here and the exception class is only a
    secondary signal. Missing a 1015 is the expensive direction: every request we then issue
    extends the ban.
    """
    # ⛔ OUR OWN REQUEST BUDGET'S REFUSAL, same class of trap as the `VenueBackoff` case below and
    # found by the same reasoning. `VenueBanned` carries
    # the venue's original 429/1015 text in its message BY CONSTRUCTION, so a producer that catches
    # one broadly and hands it here would arm this latch off a request that never left the box —
    # and the budget's own ban would then be extended by the latch's holds. Imported locally so
    # that this module — which every producer imports — stays free of a load-order dependency on
    # the budget.
    from bot.core.venue_budget import VenueRefused
    if isinstance(exc, VenueRefused):
        return None
    if isinstance(exc, VenueBackoff):
        # ⛔ OUR OWN NON-REQUEST, not the venue's answer. Found by mutation testing the producer's
        # re-raise: a `VenueBackoff` swallowed by a broad `except Exception` and handed back here
        # carries the original 1015 text inside its own message, so it would re-arm the latch off
        # traffic that never left the box — a hold that extends itself forever, which is exactly
        # the stuck-latch state `venue_backoff_status` has to page for.
        return None
    text = _error_text(exc)
    status = getattr(exc, "status_code", None)
    if _CF_1015_RE.search(text) or _RATE_TEXT_RE.search(text):
        return BAN
    if RateLimitError and isinstance(exc, RateLimitError):
        return BAN
    if isinstance(status, int) and status == 429:
        return BAN
    if InternalServerError and isinstance(exc, InternalServerError):
        return DEGRADED
    if isinstance(status, int) and 500 <= status < 600:
        return DEGRADED
    if APIStatusError and isinstance(exc, APIStatusError):
        return None
    return None


class ErrorWatcher:
    """Per-process 5xx accounting → the shared latch.

    The 5xx WINDOW is deliberately in-process, not in the file. A cross-process counter would need
    a read-modify-write on every single error (contended, and each write is an fsync pair), and the
    threshold is about "am *I* seeing a broken route repeatedly", which one process can answer on
    its own. The RESULT — the latch — is shared; the evidence is not.

    A 1015/429 arms immediately with no threshold: one is already proof, and waiting for five more
    means five more requests into a limiter that extends its ban for each of them.
    """

    def __init__(self, *, path: Optional[str] = None,
                 threshold: int = DEGRADED_THRESHOLD,
                 window_s: float = DEGRADED_WINDOW_S) -> None:
        # Kept as None, NOT resolved to LATCH_PATH here: a module-level watcher is constructed at
        # IMPORT time, and binding the path then would freeze it before a test (or an operator
        # override) could redirect the module default. Resolution happens at use.
        self.path = path
        self.threshold = int(threshold)
        self.window_s = float(window_s)
        self._hits: list[float] = []

    def record(self, exc: BaseException, *, now: Optional[float] = None) -> Optional[dict]:
        """Note one venue error. Returns the latch it armed, or None if it armed nothing.

        Returning the latch (rather than raising) is what lets a caller decide: a producer mid-pass
        wants to abandon, while a one-shot operator tool may only want to record the observation.
        """
        ts = time.time() if now is None else float(now)
        kind = classify(exc)
        if kind is None:
            return None
        if kind == BAN:
            return arm(BAN, _error_text(exc), path=self.path, now=ts)

        # DEGRADED. If a degraded hold is ALREADY in force, every further 5xx extends it — the
        # venue is telling us the hold was too short. Otherwise count within the window.
        cur = read_latch(self.path, now=ts)
        if cur is not None and cur["kind"] == DEGRADED:
            self._hits = [ts]
            return arm(DEGRADED, _error_text(exc), path=self.path, now=ts)
        if cur is not None:                      # a BAN outranks; do not touch it
            return None
        self._hits = [h for h in self._hits if ts - h < self.window_s]
        self._hits.append(ts)
        if len(self._hits) < self.threshold:
            return None
        self._hits = []
        return arm(DEGRADED, _error_text(exc), path=self.path, now=ts)


# ── status (opswatch) ────────────────────────────────────────────────────────────────────────

def venue_backoff_status(*, path: Optional[str] = None, now: Optional[float] = None
                         ) -> tuple[dict, list[str], list[str]]:
    """`(row, problems, notes)` — the `opswatch` rail contract (cf. `screen_pass_status`).

    An ACTIVE latch is a NOTE, not a problem: it is the fleet working as designed, and paging on
    it would train the operator to ignore the rail. What IS a problem is a latch that has been in
    force longer than `MAX_S` past its own arming — that means something is re-arming it forever,
    i.e. either a genuinely dead venue or a stuck writer, and the flow map is going stale behind it.
    """
    ts = time.time() if now is None else float(now)
    p = path or LATCH_PATH
    latch = read_latch(p, now=ts)
    problems: list[str] = []
    notes: list[str] = []
    row = {
        "path": p,
        "kind": None if latch is None else latch["kind"],
        "remaining_s": None if latch is None else max(0.0, latch["until"] - ts),
        "extends": None if latch is None else latch.get("extends"),
        "reason": None if latch is None else str(latch.get("reason", ""))[:120],
    }
    if latch is None:
        return row, problems, notes
    notes.append(describe(latch, now=ts))
    try:
        held_for = ts - float(latch.get("since", ts))
    except (TypeError, ValueError):
        held_for = 0.0
    if held_for > MAX_S:
        problems.append(
            f"venue backoff: the {latch['kind']} latch has been continuously in force for "
            f"{held_for / 3600:.1f}h — producers have been holding that whole time, so the flow "
            f"map is going stale and poly_event_risk gate 7 will start refusing launches. Check "
            f"the venue, then `rm logs/venue_backoff.json` if it is a stuck writer rather than a "
            f"real outage.")
    return row, problems, notes

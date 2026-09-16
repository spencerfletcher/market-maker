"""
bot/safety.py
─────────────
Stateless safety checks for the trading loop:

  is_paused(lane=None)       — kill-switch file present AND covering this lane? The check is
                               CONTENT-AWARE since 2026-08-31: `pause.json` may carry
                               `{"lanes": ["probe"]}` to halt only the named Poly maker lanes.
                               Every other content — empty (the documented `touch`), malformed,
                               or an empty list — is a GLOBAL pause, and a caller that passes no
                               `lane` (arb bot, Kalshi maker) always halts on any file.
  is_daily_loss_cap_hit()    — has today's net P&L breached DAILY_LOSS_LIMIT?
  daily_realized_loss()      — today's REALIZED losses from execution_pnl.csv

Pure functions — no shared state with BotRunner. The cross-arb loop calls
both checks every cycle; the same-platform loop relies on PositionTracker's
own stranded-position pause.
"""
from __future__ import annotations

import csv
import json
import logging
import os
import time
from datetime import datetime, timezone

from decimal import InvalidOperation

from bot.core import config
from bot.core.money import ZERO as _ZERO, parse_wire
from bot.core.logger import get_logger

log = get_logger(__name__)


def _nonfinite_loss(where: str) -> float:
    """A non-finite `amount` in execution_pnl.csv → report an INFINITE loss, so the cap FIRES.

    Fail-closed, deliberately, and it fixes a defect in BOTH directions:
      • Infinity (old float path): `inf > budget` already halted trading. Skipping the row — as an
        earlier version of this guard did — would have let a real, unrecorded loss through silently.
        That was a direction regression; this preserves the halt.
      • NaN (old float path): `nan > budget` is False, so a single NaN row SILENTLY DISABLED the cap
        entirely. Under Decimal it is worse still — `max(_ZERO, Decimal("NaN"))` RAISES
        InvalidOperation, which would escape through runner._trading_halted's 5s check.
    Either way the value is unusable, and the only safe reading of an unusable loss figure is
    "assume the worst". Logged loudly: this is the sole input to both caps, so a corrupted row must
    never pass unnoticed."""
    log.warning(f"🛑 Non-finite amount in {_EXEC_PNL_PATH} ({where}) — treating as an INFINITE loss "
                f"so the cap fires. The loss file is corrupt; inspect it before resuming.")
    return float("inf")


_EXEC_PNL_PATH = "logs/execution_pnl.csv"


def daily_realized_loss(day: str | None = None) -> float:
    """REALIZED losses for ONE UTC day from logs/execution_pnl.csv (positive = loss).

    Same buckets and same loss-only rule as cumulative_realized_loss — that function is the
    reference; this one just windows it to a day. Counts `execution_cost` (loss-positive: realized
    flatten/strand) and the LOSS side of `realized_settled` (settled void/divergence). NEVER
    `marked_unsettled` (booked, optimistic, always positive) and never a settled PROFIT — a good
    settlement must not FUND the day's loss budget. Missing/unreadable file → 0.0: a cap is a floor,
    a transient read error must not wedge it.

    ⚠️ This REPLACES compute_daily_pnl, which could not fire. It summed `guaranteed_profit` from
    trades.log — `shares × edge`, > 0 BY CONSTRUCTION for anything that fires (edge >= the 2% floor)
    — while flatten/strand costs go to execution_pnl.csv and never reach trades.log. So the sum was
    always >= 0 and `pnl < -limit` was unreachable for ANY limit > 0, in any market condition. Not a
    mis-measured cap: a cap with no trigger.

    NOT redundant with cumulative_realized_loss: that is a LIFETIME ratchet ("stop after $X of
    losses ever"); this is a rate limiter ("stop after $X today"). Only this catches a day of many
    unwinds early — the Σ flatten 70.60 vs Σ won 4.50 shape. Under a <n> lifetime budget, eight <n>
    loss-days bleed without tripping the ratchet.
    """
    target = day or datetime.now(timezone.utc).date().isoformat()
    # Decimal accumulation (migration Phase 3b-lite): amounts are decimal STRINGS in the CSV, so
    # parse_wire is exact and the running total carries no drift. This total is compared against a
    # money CAP below — the one place accumulated error could flip a safety decision at the boundary.
    total = _ZERO
    try:
        with open(_EXEC_PNL_PATH, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if not (row.get("timestamp") or "").startswith(target):
                    continue
                bucket = row.get("bucket", "")
                if bucket not in ("execution_cost", "realized_settled"):
                    continue
                try:
                    amount = parse_wire(row.get("amount") or 0)
                except (TypeError, ValueError, InvalidOperation):
                    continue
                if not amount.is_finite():
                    return _nonfinite_loss("daily")          # fail-CLOSED
                total += max(_ZERO, amount) if bucket == "execution_cost" else max(_ZERO, -amount)
    except FileNotFoundError:
        return 0.0
    except OSError as exc:
        log.warning(f"Daily realized-loss read error: {exc}")
        return 0.0
    return float(total)


def is_daily_loss_cap_hit() -> bool:
    """True (and log a warning) if today's REALIZED losses exceed DAILY_LOSS_LIMIT. 0 = off."""
    limit = config.DAILY_LOSS_LIMIT
    if limit <= 0:
        return False
    loss = daily_realized_loss()
    if loss > limit:
        log.warning(
            f"🛑 Daily loss cap hit: realized losses today = ${loss:.2f} "
            f"(limit=${limit:.2f}). No new trades until tomorrow UTC."
        )
        return True
    return False



def cumulative_realized_loss() -> float:
    """Sum of REALIZED losses across the whole run (CUMULATIVE, not windowed) from
    logs/execution_pnl.csv:
      • every `execution_cost` row — realized flatten/strand cost (`amount` is loss-positive)
      • the loss side of any `realized_settled` row — settled void/divergence (negative P&L)
    NEVER counts `marked_unsettled`: a run cannot fund a loss cap on booked, unsettled edge
    (which is always positive). Reads line-by-line each ~5s halt check. Returns 0.0 if the
    file is missing/unreadable — the cap is a floor, a transient read error must not wedge it.
    """
    # Decimal accumulation (Phase 3b-lite) — see daily_realized_loss. This is the LIFETIME total,
    # so it is the accumulator most exposed to drift, and it gates the cumulative budget.
    total = _ZERO
    try:
        with open(_EXEC_PNL_PATH, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                bucket = row.get("bucket", "")
                if bucket not in ("execution_cost", "realized_settled"):
                    continue
                try:
                    amount = parse_wire(row.get("amount") or 0)
                except (TypeError, ValueError, InvalidOperation):
                    continue
                if not amount.is_finite():
                    return _nonfinite_loss("cumulative")     # fail-CLOSED
                # execution_cost amounts are loss-positive; realized_settled is signed P&L
                # (negative = loss). Count only the loss side of each.
                total += max(_ZERO, amount) if bucket == "execution_cost" else max(_ZERO, -amount)
    except FileNotFoundError:
        return 0.0
    except OSError as exc:
        log.warning(f"Realized-loss read error: {exc}")
        return 0.0
    return float(total)


def is_exec_cost_cap_hit() -> bool:
    """True (and log) if cumulative realized loss has breached KALSHI_EXEC_COST_BUDGET.

    CUMULATIVE + monotonic → once breached it stays breached: NO auto-resume (unlike a
    rolling window or the UTC-resetting daily cap). Independent of booked/marked edge, so a
    losing run can't keep funding itself. The void/divergence tail is NOT bounded by the
    per-trade 25% flatten cap — a void can lose most of a leg (LFMP) — so this campaign-total
    cap is the real floor. 0 budget = disabled.
    """
    budget = config.KALSHI_EXEC_COST_BUDGET
    if budget <= 0:
        return False
    loss = cumulative_realized_loss()
    if loss > budget:
        log.critical(
            f"🛑 Realized-loss cap breached: ${loss:.2f} cumulative realized loss "
            f"(flatten/strand + settled void/divergence) > ${budget:.2f} budget. "
            f"Halting new trades — no auto-resume."
        )
        return True
    return False


#: Cap on the kill-switch file's read. It is a hand-written control file of a few dozen bytes;
#: anything larger is already ambiguous and gets the global pause. Unbounded, this read runs once
#: per quote cycle against an operator-writable path on a 1.9 GB box — a stray multi-GB file there
#: would be an OOM path into the live maker.
_PAUSE_READ_MAX_BYTES = 64 * 1024


def _pause_scope(path: str) -> list[str] | None:
    """The lanes an (already-existing) kill-switch file narrows to, or `None` for a GLOBAL pause.

    ⛔ FAIL-CLOSED IS THE WHOLE POINT: **ambiguity must always halt MORE, never less.**
    The file's ONLY narrowing form is a well-formed `{"lanes": ["<name>", ...]}` with a
    NON-EMPTY list of strings; that, and only that, returns a scope. Every other reading is
    `None` = GLOBAL — an empty file (the operator's documented `touch pause.json`), an
    unreadable or non-UTF-8 file, an oversized file, unparseable JSON, a non-object document,
    a missing `lanes` key, a `lanes` that is not a list, an EMPTY list, or a list holding a
    non-string. An operator who reaches for the kill switch and gets a partial halt because
    their JSON had a typo is the failure this ordering refuses to allow.

    ⚠️ The read's `except` covers `ValueError` as well as `OSError`: `f.read()` on non-UTF-8
    bytes raises `UnicodeDecodeError`, a ValueError subclass, and letting it escape would turn
    the kill-switch check — the first statement of the quote cycle — into an anonymous crash
    into teardown with no `halt_reason`. A corrupt pause file must pause, not explode.

    Read fresh on every call — this is polled once per quote cycle (seconds), and a stat/size
    cache would trade correctness for an unmeasurable saving on a file that is normally absent.
    """
    try:
        with open(path, encoding="utf-8") as f:
            raw = f.read(_PAUSE_READ_MAX_BYTES + 1).strip()
    except (OSError, ValueError):
        return None
    if not raw or len(raw) > _PAUSE_READ_MAX_BYTES:
        return None
    try:
        doc = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(doc, dict):
        return None
    lanes = doc.get("lanes")
    if not isinstance(lanes, list) or not lanes:
        return None
    if not all(isinstance(name, str) for name in lanes):
        return None
    return list(lanes)


def pause_file_scope(path: str) -> list[str] | None:
    """Public alias of `_pause_scope` for callers that gate on the file WITHOUT being the loop
    that halts on it (`scripts/poly_prelaunch.py`'s kill-switch gate). Same fail-closed rules;
    `None` means "global / ambiguous — treat as covering everyone"."""
    return _pause_scope(path)


#: How often the scoped-MISS line is allowed to be a WARNING for one unchanged (file, scope, lane).
_SCOPED_MISS_WARN_EVERY_S = 600.0
#: (path, mtime, scope, lane) → epoch of the last WARNING-level emission.
_scoped_miss_last_warn: dict[tuple, float] = {}


def _scoped_miss_level(path: str, scope: list[str], lane: str) -> int:
    """WARNING on the first sighting of a (file, scope, lane) and every 10 min after; INFO between.

    ⛔ NOT COSMETIC — this is a Discord rate-limit guard [review r3, CONCERN A].
    `_DiscordWebhookHandler` posts every WARNING-and-above with only a 30 s dedup window
    [bot/core/logger.py:_DiscordWebhookHandler._DEDUP_WINDOW], and the scoped-miss line is emitted
    once per quote cycle in this feature's INTENDED STEADY STATE (a scoped pause up while the other
    lane runs all evening). At `--requote-s 10` that is ~960 posts an evening onto the same webhook
    that carries strand alerts: alert fatigue plus a real 429 risk on the channel we need working
    during an incident. The log TAIL stays loud — every check still emits a line — but only the
    throttled subset reaches WARNING and therefore Discord.

    The key includes the file's mtime AND the parsed scope, so an operator EDITING the pause file
    (fixing the lane name, adding a lane) re-fires immediately rather than waiting out the window —
    the edit is exactly the moment they are watching for a response.
    """
    try:
        mtime = os.stat(path).st_mtime
    except OSError:
        mtime = -1.0
    key = (path, mtime, tuple(scope), lane)
    now = time.time()
    last = _scoped_miss_last_warn.get(key)
    if last is not None and now - last < _SCOPED_MISS_WARN_EVERY_S:
        return logging.INFO
    if len(_scoped_miss_last_warn) > 100:
        # Unbounded growth is impossible in practice (one key per edit), but this dict outlives
        # every run in a long-lived process; same clamp shape as the Discord handler's own.
        _scoped_miss_last_warn.clear()
    _scoped_miss_last_warn[key] = now
    return logging.WARNING


def is_paused(lane: str | None = None) -> bool:
    """True (and log a warning) if the kill-switch file exists and covers `lane`.

    Operator creates `pause.json` (or whatever KILL_SWITCH_FILE points to) to
    halt new trade execution without restarting. Remove the file to resume.

    `lane` is the Poly maker's `--lane` (main / probe / wsprobe). Passing it opts into the
    lane-scoped form `{"lanes": ["probe"]}`, which halts ONLY the listed lanes so one lane's
    teardown cannot tear down a concurrent run in another lane. See `_pause_scope` for the
    fail-closed rules.

    `lane=None` — the DEFAULT, and what every non-Poly-maker caller passes (arb bot, Kalshi
    maker, probe scripts) — means "I am not in the lane namespace": ANY pause file, scoped or
    not, halts me. Those callers have no lane to match against, so the only fail-closed reading
    of a scoped file is that it applies to them too.
    """
    path = config.KILL_SWITCH_FILE
    if not path or not os.path.exists(path):
        return False
    if lane is not None:
        scope = _pause_scope(path)
        if scope is not None and lane not in scope:
            # ⛔ LOUD ON THE NOT-PAUSED BRANCH TOO. Returning
            # False silently here is the fail-open an operator cannot see: they wrote
            # `{"lanes": ["Probe"]}`, the case does not match, and a live maker keeps quoting
            # through what they believe is a halt — with no line in the tail to say so.
            # EVERY CHECK gets a line (the operator is watching a scrolling tail, and one line at
            # the top of a spell that lasts hours is a line they will not be looking at) — but only
            # a THROTTLED subset is WARNING. See `_scoped_miss_level`. [review r3, CONCERN A]
            log.log(
                _scoped_miss_level(path, scope, lane),
                f"⏸️  Pause file present but scoped to {scope!r}; lane {lane!r} CONTINUES "
                f"quoting ('{path}'). If you meant to halt this lane, add it to \"lanes\" or "
                f"empty the file (an empty file halts EVERY lane)."
            )
            return False
    scope_note = "" if lane is None else f" (lane={lane})"
    log.warning(
        f"⏸️  Kill switch active — '{path}' exists{scope_note}. Remove it to resume trading."
    )
    return True

"""
bot/core/safety.py
──────────────────
Stateless safety checks for the trading loop:

  is_paused()                — kill-switch file present?
  is_daily_loss_cap_hit()    — has today's net P&L breached DAILY_LOSS_LIMIT?
  daily_realized_loss()      — today's REALIZED losses from execution_pnl.csv

Pure functions — no shared state with BotRunner. The cross-arb loop calls
both checks every cycle; the same-platform loop relies on PositionTracker's
own stranded-position pause.
"""
from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone

from decimal import Decimal, InvalidOperation

from bot.core import config
from bot.core.money import parse_wire
from bot.core.logger import get_logger

log = get_logger(__name__)

_ZERO = Decimal(0)


def _nonfinite_loss(where: str) -> float:
    """A non-finite `amount` in execution_pnl.csv → report an INFINITE loss, so the cap FIRES.

    Fail-closed, deliberately, and it fixes a defect in BOTH directions:
      • Infinity (old float path): `inf > budget` already halted trading. Skipping the row — as an
        earlier version of this guard did — would have let a real, unrecorded loss through silently.
        That was a direction regression; this preserves the halt.
      • NaN (old float path): `nan > budget` is False, so a single NaN row SILENTLY DISABLED the cap
        entirely. Under Decimal it is worse still — `max(_ZERO, Decimal("NaN"))` RAISES
        InvalidOperation, which would escape out of the caller's periodic halt check.
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

    NOT redundant with cumulative_realized_loss: that is a LIFETIME ratchet ("stop after X of losses
    ever"); this is a rate limiter ("stop after X today"). Only this catches a day of many unwinds
    early — the shape where the flatten costs dwarf what the winners brought in. Without the daily
    window, a run can bleed a modest amount every day for weeks and never trip the lifetime ratchet.
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


def is_paused() -> bool:
    """True (and log a warning) if the kill-switch file exists.

    Operator creates `pause.json` (or whatever KILL_SWITCH_FILE points to) to
    halt new trade execution without restarting. Remove the file to resume.
    """
    path = config.KILL_SWITCH_FILE
    if path and os.path.exists(path):
        log.warning(
            f"⏸️  Kill switch active — '{path}' exists. Remove it to resume trading."
        )
        return True
    return False

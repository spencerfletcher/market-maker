"""The Kalshi taker-fee formula — one pure function, deliberately standing alone.

Fee math is imported by the client, the maker and anything that prices an edge, so it must not
drag a trading engine in behind it. Value pins live in `tests/test_kalshi_fees.py`.
"""
from __future__ import annotations

from bot.core.money import CENTICENT, D, ceil_to, from_float


def _kalshi_taker_fee(price: float, count: int = 1) -> float:
    """Per-contract Kalshi taker fee, ceiled to the CENTICENT ($0.0001) on the ORDER TOTAL.

    Kalshi's published fee schedule gives the formula verbatim:

        fees = round up(M x 0.07 x C x P x (1-P))
        "round up = rounds up such that the fee + positionCost is rounded to a centicent"

    M is the per-series taker multiplier and defaults to 1; it is 1 for every series this code
    quotes, so it is omitted here. Two details in that one line do all the damage if you miss
    them, and both are pinned by tests:

    ⚠️ THE CEILING IS ON THE ORDER TOTAL, NOT PER CONTRACT. Three contracts at 0.45 cost
    `ceil(0.07·3·0.45·0.55)` = $0.0520 for the order. Ceiling each contract first and multiplying
    gives $0.0522 — small, wrong, and wrong in the direction that quietly overstates cost on
    every multi-contract order.

    ⚠️ THE GRID IS THE CENTICENT (1e-4), **NOT** the cent. The schedule prints a convenience
    table alongside the formula that shows a single contract at $0.30 costing $0.02. That table
    is DISPLAY-rounded up to the cent and is not the formula — the same table's 100-contract
    column gives the truth, $1.47 = 0.07·100·0.3·0.7 exactly, i.e. $0.0147 per contract. Reading
    the table instead of the formula overstates the fee by up to ~0.9c/contract, which
    UNDERSTATES every edge computed from it; a meaningful share of trades rejected as
    below-the-minimum-edge in fact cleared the floor. **Read the formula, never the table.**

    This ceil is the single operation the money core exists for. In float,
    `0.07*0.5*0.5*10000 == 175.00000000000003`, so a bare `ceil()` yields 176 → $0.0176 instead
    of the true $0.0175 (the schedule's own 100-contract column confirms $1.75 for 100 at 0.50).
    Exact `Decimal` plus `ceil_to` makes the guard STRUCTURAL rather than a hand-placed `round()`
    a future edit can drop.

    Direction is pinned deliberately: relative to the raw formula this may only ever OVER-charge,
    never under — so an effective cost built on it is never understated and an edge built on it
    is never overstated. Float in and float out is a concession to callers that still write
    `ask + _kalshi_taker_fee(...)`; everything in between is exact.
    """
    p = from_float(price)
    c = D(int(count))
    total = ceil_to(D("0.07") * c * p * (D(1) - p), CENTICENT)   # ceil on the ORDER TOTAL
    return float(total / c)                                       # ...then per contract

"""Fail-closed venue-quantity parsing, shared by every process that reads a venue quantity.

It lives on its own because a sixteen-line pure parser should never drag a trading engine in
behind it — every consumer of this function is somewhere you do NOT want an import cycle.
"""
from __future__ import annotations


def _first_qty(item: dict, *keys: str) -> float | None:
    """First parseable quantity among `keys`, or None if NONE parse.

    None means CANNOT VERIFY — never 0.0. Kalshi's field is `position_fp` (fixed-point STRING),
    with `position` as the older int; reading the wrong one silently reports "flat" on a real
    position, which is the reconciler's whole failure mode. This repo already hit that exact trap
    once (`position` vs `position_fp`). Try both; if neither parses, say so loudly rather than
    inventing a zero."""
    for k in keys:
        v = item.get(k)
        if v is None or v == "":
            continue
        try:
            return abs(float(v))
        except (TypeError, ValueError):
            continue
    return None

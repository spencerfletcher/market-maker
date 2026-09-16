"""Fail-closed venue-quantity parsing, shared by the reconciler and the maker supervisor.

Moved here from `bot.runner.reconcile` 2026-08-13 (public-repo decoupling: the supervisor must
not import the arb runner for a 16-line pure parser) — CODE byte-identical; the docstring's two
self-references were re-homed. `reconcile` re-exports it, so its callers and tests are unchanged.
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

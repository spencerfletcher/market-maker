"""Pins the CONTRACT of Decimal Phase 1 (the private design notes) — the guarantees the exact
rewrite of `kalshi_tick_floor` / `_kalshi_taker_fee` must keep as later phases move the float boundary.

Three things are asserted that nothing else covered:
  1. EQUIVALENCE to the old float implementation over the reachable price grid — the migration's own
     central claim, previously unpinned.
  2. DIRECTION — floors may only reduce a price (never push a limit up past breakeven); the fee may
     only be over-charged (never understating `kalshi_effective`, never overstating `edge`).
  3. TICK 0.005 — roughly half the live slate, and untested before this file (test_kalshi_arb.py
     covers 0.01 and 0.05 only).
"""
import math
from decimal import Decimal

import pytest

from bot.core.money import CENTICENT, D
from bot.kalshi.fees import _kalshi_taker_fee, kalshi_tick_floor

TICKS = (0.01, 0.005)
# every price the venues can quote, sampled coarsely enough to keep the suite ~3s
GRID = [round(k * 1e-4, 4) for k in range(100, 9901, 7)]


# ── the OLD implementations, reproduced verbatim, as the equivalence oracle ──────────────
def _old_tick_floor(price: float, tick: float = 0.01) -> float:
    return math.floor(round(price / tick, 6)) * tick


def _old_taker_fee(price: float, count: int = 1) -> float:
    total = math.ceil(round(0.07 * count * price * (1 - price) * 10000, 6)) / 10000
    return total / count


# ── 1. equivalence over the reachable domain ─────────────────────────────────────────────
@pytest.mark.parametrize("count", [1, 2, 3, 10, 100])
def test_fee_is_bit_identical_to_the_old_float_path(count):
    """The migration must not move a single fee on any wire-quotable price."""
    for p in GRID:
        assert _kalshi_taker_fee(p, count) == pytest.approx(_old_taker_fee(p, count), abs=1e-12), p


@pytest.mark.parametrize("tick", TICKS)
def test_tick_floor_agrees_with_the_old_path_on_wire_prices(tick):
    for p in GRID:
        assert kalshi_tick_floor(p, tick) == pytest.approx(_old_tick_floor(p, tick), abs=1e-12), p


# ── 2. direction safety (the money-relevant invariants) ──────────────────────────────────
@pytest.mark.parametrize("tick", TICKS)
def test_tick_floor_never_rounds_a_price_UP(tick):
    """A tick adjustment may only make us pay less. Rounding up could push a limit past breakeven."""
    for p in GRID:
        assert kalshi_tick_floor(p, tick) <= p + 1e-12, p


def test_fee_is_never_UNDER_the_raw_formula():
    """Under-charging would understate kalshi_effective and OVERSTATE edge — it would fire trades that
    are not actually above the floor. Over-charging is merely conservative."""
    for p in GRID:
        raw = 0.07 * p * (1 - p)
        assert _kalshi_taker_fee(p) >= raw - 1e-12, p
        assert _kalshi_taker_fee(p) <= raw + float(CENTICENT) + 1e-12, p   # and never wildly over


# ── 3. tick 0.005 — the untested half of the live slate ──────────────────────────────────
def test_half_cent_tick_lands_exactly_on_its_grid():
    for p in GRID:
        got = D(f"{kalshi_tick_floor(p, 0.005):.6f}")
        assert got % D("0.005") == 0, f"{p} -> {got} is off the 0.005 grid"


def test_half_cent_tick_is_finer_than_the_cent_tick():
    # 0.4370 floors to 0.43 on a 1c grid but stays 0.435 on a half-cent grid
    assert kalshi_tick_floor(0.4370, 0.01) == pytest.approx(0.43)
    assert kalshi_tick_floor(0.4370, 0.005) == pytest.approx(0.435)


# ── regression anchors: the exact values from the two production incidents ───────────────
def test_the_tick_incident_value():
    assert kalshi_tick_floor(0.29, 0.01) == pytest.approx(0.29)   # bare floor() gave 0.28


def test_an_on_tick_price_is_unchanged_by_flooring():
    """Idempotence — flooring a price already on its tick must be a no-op, at both ticks."""
    for tick in TICKS:
        for p in (0.01, 0.25, 0.50, 0.75, 0.99):
            assert kalshi_tick_floor(p, tick) == pytest.approx(p), (p, tick)


# ── the ORDER-TOTAL ceil (not per-contract) — the property worth ~0.9c/contract ──────────
def test_ceil_applies_to_the_order_total_not_per_contract():
    """Measured on a real demo fill: 3 @ 0.45 cost <n> total. Ceiling per-contract would give
    3 × 0.0174 = 0.0522 — the error class that made the original fee bug worth ~10% of our edges."""
    assert _kalshi_taker_fee(0.45, 3) * 3 == pytest.approx(0.0520, abs=1e-9)
    per_contract_ceil = 3 * (math.ceil(0.07 * 0.45 * 0.55 * 10000) / 10000)
    assert per_contract_ceil == pytest.approx(0.0522, abs=1e-9)   # what we must NOT produce


def test_fee_total_lands_on_the_centicent_grid():
    for p in GRID[::13]:
        total = D(f"{_kalshi_taker_fee(p) :.8f}")
        assert total % CENTICENT == 0, f"{p} -> {total} off the centicent grid"

"""Value pins for the Kalshi taker-fee formula.

Every expected number below is derived from the published fee schedule and nothing else:

    fee(order) = ceil_to_centicent(M x 0.07 x C x P x (1 - P)),  M = 1

Two properties of that one line are what actually bite, and each gets its own pin:

  1. the ceiling applies to the ORDER TOTAL, not to each contract; and
  2. the grid is the CENTICENT ($0.0001), not the cent — the schedule's convenience table is
     display-rounded to the cent and is NOT the formula.

The schedule's own 100-contract column is the useful cross-check, because at 100 contracts the
product lands exactly on the grid and the ceiling does nothing: 100 @ $0.30 -> $1.47 and
100 @ $0.50 -> $1.75. Those two are arithmetic, not folklore, and they are what pins the
per-contract answers at 0.0147 and 0.0175.
"""
from __future__ import annotations

import math

import pytest

from bot.kalshi.fees import _kalshi_taker_fee


def _order_total(price: float, count: int) -> float:
    return _kalshi_taker_fee(price, count) * count


# ── The schedule's own worked points ────────────────────────────────────────────────────────

def test_one_contract_at_30c_is_a_centicent_not_a_cent():
    """0.07 x 1 x 0.30 x 0.70 = 0.0147 exactly — already on the centicent grid.

    The schedule's convenience table shows $0.02 here. That is the cent-rounded DISPLAY value;
    implementing it overcharges by 0.53c on this single contract.
    """
    assert _kalshi_taker_fee(0.30, 1) == pytest.approx(0.0147, abs=1e-12)


def test_hundred_contracts_at_30c_matches_the_schedules_own_column():
    """0.07 x 100 x 0.30 x 0.70 = 1.47 exactly — the schedule prints this figure itself."""
    assert _order_total(0.30, 100) == pytest.approx(1.47, abs=1e-9)


def test_hundred_contracts_at_50c_is_the_float_dust_case():
    """0.07 x 100 x 0.50 x 0.50 = 1.75 exactly, and it is the case a float ceil gets WRONG.

    In binary float `0.07*0.5*0.5*10000` evaluates to 175.00000000000003, so a bare ceil()
    rounds up a phantom ulp and yields $0.0176/contract. Exact Decimal arithmetic is the only
    reason this pin passes.
    """
    assert _kalshi_taker_fee(0.50, 100) == pytest.approx(0.0175, abs=1e-12)
    assert _order_total(0.50, 100) == pytest.approx(1.75, abs=1e-9)


# ── The ceiling is on the order total ───────────────────────────────────────────────────────

def test_three_at_45c_ceils_the_order_not_each_contract():
    """0.07 x 3 x 0.45 x 0.55 = 0.051975 -> ceil to the centicent -> $0.0520 for the ORDER.

    Ceiling per contract first would give ceil(0.0173250) = 0.0174 each = $0.0522, which is the
    wrong answer by 0.02c. This is the pin that stops a refactor from moving the ceiling inside
    the multiply.
    """
    assert _order_total(0.45, 3) == pytest.approx(0.0520, abs=1e-9)
    assert _kalshi_taker_fee(0.45, 3) == pytest.approx(0.0520 / 3, abs=1e-12)

    per_contract_ceiling = math.ceil(0.07 * 0.45 * 0.55 * 10_000) / 10_000 * 3
    assert per_contract_ceiling == pytest.approx(0.0522, abs=1e-9)
    assert _order_total(0.45, 3) < per_contract_ceiling


# ── Shape of the formula ────────────────────────────────────────────────────────────────────

def test_fee_is_symmetric_about_the_midpoint():
    """P x (1 - P) is symmetric, so buying at P and at 1 - P cost the same."""
    for price in (0.10, 0.25, 0.42, 0.49):
        assert _kalshi_taker_fee(price, 100) == pytest.approx(
            _kalshi_taker_fee(1 - price, 100), abs=1e-12
        )


def test_fee_peaks_at_the_midpoint():
    """P x (1 - P) is maximised at 0.50 — the most expensive place on the book to cross."""
    mid = _kalshi_taker_fee(0.50, 100)
    for price in (0.05, 0.20, 0.35, 0.65, 0.80, 0.95):
        assert _kalshi_taker_fee(price, 100) < mid


def test_default_count_is_one_contract():
    assert _kalshi_taker_fee(0.30) == _kalshi_taker_fee(0.30, 1)


# ── Direction of the rounding error ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("price", [0.0173, 0.1234, 0.3007, 0.4270, 0.6666, 0.8891])
@pytest.mark.parametrize("count", [1, 2, 3, 7, 50, 833])
def test_rounding_may_only_overcharge_never_undercharge(price: float, count: int):
    """The ceiling must round the ORDER TOTAL up to the grid, never down.

    The invariant that matters downstream: a fee built this way is never understated, so an
    effective cost is never understated and an edge computed from it is never overstated. The
    excess is bounded by one centicent on the whole order, regardless of size.
    """
    raw = 0.07 * count * price * (1 - price)
    total = _order_total(price, count)

    assert total >= raw - 1e-12
    assert total - raw < 1e-4 + 1e-12

    on_grid = round(total * 10_000)
    assert abs(total * 10_000 - on_grid) < 1e-6, "order total must land on the centicent grid"


def test_fee_is_zero_at_the_boundaries():
    """P x (1 - P) is zero at 0 and 1, and a ceiling of zero is still zero."""
    assert _kalshi_taker_fee(0.0, 100) == 0.0
    assert _kalshi_taker_fee(1.0, 100) == 0.0

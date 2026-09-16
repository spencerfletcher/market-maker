"""Kalshi taker-fee model — pinned against REAL MEASURED FILLS, not the schedule's display table.

Why this file exists: the fee is the core of the edge (`edge = 1 - poly_effective - kalshi_effective`,
CLAUDE.md: "Edges are ALWAYS net of taker fees"), and it was WRONG from the start — it ceiled to the
CENT because someone implemented the fee schedule's human-readable convenience table ("<n> → <n>
for 1 contract", pp.4-5) instead of the formula printed above it:

    fees = round up(M x 0.07 x C x P x (1-P))
    "round up = rounds up such that the fee + positionCost is rounded to a centicent"
                                                                     ^^^^^^^^^ 1e-4, not 1e-2

That 100x precision error overstated the fee by up to ~0.9c/contract, understating EVERY edge and
rejecting ~10% of `below_min_edge` opportunities (327/3234) that actually cleared the 2% net floor.

The pins below are ACTUAL CHARGES from real fills on 2026-07-14 — ground truth, not doc-reading.
If someone "simplifies" this back to the table, these fail.
"""
import math

import pytest

from bot.kalshi.fees import _kalshi_taker_fee, kalshi_tick_floor


# ── Ground truth: what Kalshi ACTUALLY charged (fee_cost from /portfolio/fills) ──────────
# (price, count, actual_total_fee_dollars, venue, note)
REAL_FILLS = [
    (0.4270, 1, 0.017200, "prod", "sub-cent fill price; centicent ceil of 0.017127"),
    (0.45,   3, 0.052000, "demo", "excludes PER-CONTRACT ceiling (that would be 0.0522)"),
    (0.30,   1, 0.014700, "demo", "exact centicent; excludes our old cent-ceil 0.02"),
]


@pytest.mark.parametrize("price,count,actual,venue,note", REAL_FILLS)
def test_matches_real_measured_fills(price, count, actual, venue, note):
    """THE pin: reproduce what the exchange actually charged, to the centicent."""
    assert _kalshi_taker_fee(price, count) * count == pytest.approx(actual, abs=5e-7), note


@pytest.mark.parametrize("price,count,actual,venue,note", REAL_FILLS)
def test_the_old_cent_ceiling_would_have_been_wrong(price, count, actual, venue, note):
    """Regression guard: the OLD model (ceil to cent) must NOT reproduce reality.
    If this ever passes, someone reverted the precision to the display table."""
    old = math.ceil(0.07 * count * price * (1 - price) * 100) / 100
    assert old != pytest.approx(actual, abs=5e-7), "old cent-ceil model must not match reality"


def test_ceiling_is_on_the_total_not_per_contract():
    """3 @ 0.45 discriminates: per-contract ceiling gives 0.0522, total ceiling gives 0.0520.
    Kalshi charged 0.0520."""
    per_contract_ceil = math.ceil(0.07 * 0.45 * 0.55 * 10000) / 10000 * 3
    total_ceil = math.ceil(0.07 * 3 * 0.45 * 0.55 * 10000) / 10000
    assert per_contract_ceil == pytest.approx(0.0522)
    assert total_ceil == pytest.approx(0.0520)
    assert _kalshi_taker_fee(0.45, 3) * 3 == pytest.approx(0.0520, abs=5e-7)


def test_ceils_to_centicent_never_below_the_raw_formula():
    """Fee must never be UNDER the raw formula (Kalshi rounds UP), and never more than
    one centicent over it."""
    for p in [i / 100 for i in range(1, 100)]:
        for n in (1, 3, 10, 137):
            raw = 0.07 * n * p * (1 - p)
            got = _kalshi_taker_fee(p, n) * n
            assert got >= raw - 1e-12, f"fee under-charges at p={p} n={n}"
            assert got - raw <= 1e-4 + 1e-12, f"fee more than a centicent over at p={p} n={n}"


def test_rate_is_007_recovered_from_a_real_charge():
    """The schedule withholds the rate in the API docs; recover it from the actual charge.
    0.0147 / (0.30 * 0.70) == 0.07 exactly."""
    assert 0.014700 / (0.30 * 0.70) == pytest.approx(0.07, abs=1e-9)


def test_fee_is_symmetric_around_half_and_peaks_there():
    """p(1-p) is symmetric and maximal at 0.5 — a sanity property of the schedule's formula."""
    assert _kalshi_taker_fee(0.30) == pytest.approx(_kalshi_taker_fee(0.70))
    assert _kalshi_taker_fee(0.50) >= _kalshi_taker_fee(0.30)


def test_zero_and_extreme_prices_do_not_explode():
    for p in (0.0001, 0.01, 0.99, 0.9999):
        f = _kalshi_taker_fee(p)
        assert 0.0 <= f < 0.02


# ── formula value pins (moved 2026-09-05 from tests/test_kalshi_arb.py with cross_arb's removal;
# the symbol lives in bot/kalshi/fees.py and outlives the arb engine) ─────────────────────────

# NOTE: these previously pinned a CENT ceiling (0.02 / 0.01) — that was the bug, copied from the
# fee schedule's DISPLAY table rather than its formula. Kalshi ceils to the CENTICENT. Corrected
# 2026-07-14 against real measured fills; the ground truth is REAL_FILLS above.

def test_kalshi_fee_at_50_cents():
    """At P=0.50: ceil(0.07 * 0.5 * 0.5 * 10000) / 10000 = 175/10000 = 0.0175.
    (The schedule's own 100-contract column confirms: <n> -> <n> = 0.0175/contract.)"""
    assert _kalshi_taker_fee(0.50) == pytest.approx(0.0175)


def test_kalshi_fee_at_30_cents():
    """At P=0.30: 0.07 * 0.3 * 0.7 = 0.0147 exactly (already a centicent).
    MEASURED on a real fill 2026-07-14: Kalshi charged <n> — NOT the <n> we assumed."""
    assert _kalshi_taker_fee(0.30) == pytest.approx(0.0147)


def test_kalshi_fee_at_90_cents():
    """At P=0.90: 0.07 * 0.9 * 0.1 = 0.0063 exactly. (Old model said 0.01 — 59% too high.)"""
    assert _kalshi_taker_fee(0.90) == pytest.approx(0.0063)


def test_kalshi_fee_per_contract_is_now_size_independent():
    """10 @ 0.50: total = ceil(0.07*10*0.5*0.5*10000)/10000 = 0.175 -> 0.0175/contract.

    Property change worth pinning: with the CENT ceiling the per-contract fee varied with count
    (ceil waste amortised over the lot, 0.02 at n=1 vs 0.018 at n=10). At centicent precision the
    ceiling is negligible, so the per-contract fee is effectively size-independent."""
    assert _kalshi_taker_fee(0.50, count=10) == pytest.approx(0.0175)
    assert _kalshi_taker_fee(0.50, count=1) == pytest.approx(_kalshi_taker_fee(0.50, count=10))


# ── kalshi_tick_floor (same move; Decimal-boundary pins live in test_decimal_phase1.py) ───────

def test_kalshi_tick_floor_rounds_sub_cent_down():
    """Sub-cent prices (the bug that 400-rejected the Kalshi leg) floor to <n>."""
    assert kalshi_tick_floor(0.2850) == pytest.approx(0.28)
    assert kalshi_tick_floor(0.3047) == pytest.approx(0.30)
    assert kalshi_tick_floor(0.7150) == pytest.approx(0.71)


def test_kalshi_tick_floor_leaves_whole_cents_unchanged():
    """Float noise must not knock a whole cent down a tick (0.29 stays 0.29)."""
    assert kalshi_tick_floor(0.29) == pytest.approx(0.29)
    assert kalshi_tick_floor(0.01) == pytest.approx(0.01)
    assert kalshi_tick_floor(0.70) == pytest.approx(0.70)


def test_kalshi_tick_floor_honors_a_non_default_tick():
    """A live non-<n> tick floors to that grid (defensive; all markets are <n> today)."""
    assert kalshi_tick_floor(0.2850, 0.05) == pytest.approx(0.25)   # nickel floor
    assert kalshi_tick_floor(0.27, 0.05) == pytest.approx(0.25)
    assert kalshi_tick_floor(0.30, 0.05) == pytest.approx(0.30)


def test_kalshi_tick_floor_default_tick_is_behavior_preserving():
    """Passing the 0.01 default is identical to the no-arg call at today's <n> tick — the swap
    changes nothing now. Compare call-to-call (exact), plus the literal cent value."""
    for p in (0.2850, 0.3047, 0.7150, 0.29, 0.01):
        assert kalshi_tick_floor(p, 0.01) == kalshi_tick_floor(p)
    assert kalshi_tick_floor(0.2850, 0.01) == pytest.approx(0.28)


def test_kalshi_tick_floor_keeps_no_side_price_on_tick():
    """For a NO buy we send yes_price = 1 - floor(limit); both stay whole cents."""
    limit = kalshi_tick_floor(0.2850)        # 0.28
    yes_price = round(1.0 - limit, 4)         # 0.72
    assert (yes_price * 100) % 1 == pytest.approx(0.0)

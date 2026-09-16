"""Pins the correctness remainder after Phase 3b-lite: the surviving complements in the CLIENTS
(one of which prices a real order) and reconcile's compare-to-zero on a venue position.

Neither is plain type purity:
  • `_v2_order_params` complements a NO order's price — that number is SENT to Kalshi.
  • `reconcile` escalates a truthy position through `add_stranded`, a GLOBAL PAUSE requiring a
    manual resume. Dust there halts the bot; it is the same class as the crossed-book bug, and the
    2026-07-19 live run returned fractional position_fp values, so the input is demonstrably dirty.
"""
import pytest

from bot.core.money import complement
from bot.kalshi.client import _v2_order_params
from bot.core.reconcile import _is_real_position


# ── the NO-order wire price ──────────────────────────────────────────────────────────────
@pytest.mark.parametrize("no_price,expected_yes", [
    (0.55, 0.45), (0.07, 0.93), (0.99, 0.01), (0.4270, 0.5730), (0.33, 0.67),
])
def test_no_order_wire_price_is_the_exact_complement(no_price, expected_yes):
    """V2 is YES-only, so a NO buy is sent at 1-n. In float 1.0-0.55 is 0.44999999999999996;
    the price on the wire must be exactly 0.45."""
    book_side, yes_price = _v2_order_params("no", "buy", no_price)
    assert book_side == "ask"
    assert yes_price == expected_yes


def test_yes_order_price_is_passed_through_untouched():
    assert _v2_order_params("yes", "buy", 0.55) == ("bid", 0.55)


def test_complementing_twice_returns_the_original_price():
    """The two legs of one trade are complements of each other; the round trip must be exact or the
    two sides disagree about the same price."""
    for p in (0.01, 0.07, 0.29, 0.4270, 0.55, 0.99):
        assert complement(complement(p)) == p


# ── reconcile: dust must not trigger a global pause ──────────────────────────────────────
@pytest.mark.parametrize("dust", [1e-13, 5.68e-14, 1e-5, 0.0])
def test_sub_resolution_residue_is_not_a_position(dust):
    """A truthy qty here becomes a VenuePosition -> add_stranded -> global pause. Venues report
    holdings to 2 dp, so anything under wire resolution is residue, not size."""
    assert _is_real_position(dust) is False


@pytest.mark.parametrize("real", [0.0001, 0.01, 1.0, 4.65, 8.0, 333.0])
def test_a_real_holding_is_still_reported(real):
    """The far more dangerous direction: missing a real position is exactly what this module exists
    to prevent, so the floor must never swallow one."""
    assert _is_real_position(real) is True


# ⛔ REMOVED 2026-09-04 — the three `_kalshi_ask_levels` sign/finiteness pins went with
# `bot.runner.kalshi_arb` when the cross-arb bot was removed
# (archive branch archive/arb-bot-2026-09-04).


def test_non_finite_position_fails_toward_alerting():
    """reconcile's principle is 'fail toward alerting, never silently drop a position' — an
    uninterpretable holding must surface as a divergence, not vanish (and must not raise)."""
    assert _is_real_position(float("nan")) is True
    assert _is_real_position(float("inf")) is True

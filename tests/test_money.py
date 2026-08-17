"""Pins bot/core/money.py against the REAL production precision bugs it exists to make impossible.

Every adversarial value below is taken from an incident, not invented:
  • 0.07*0.5*0.5*1e4 == 175.00000000000003  → the cent-vs-centicent fee overcharge
  • 0.29/0.01        == 28.999999999999996  → the tick-floor off-by-one
  • ~1e-13 residual qty                     → the crossed-book dust / phantom-edge bug
"""
import math
from decimal import Decimal

import pytest

from bot.core.money import CENT, CENTICENT, D, ceil_to, floor_to, from_float, is_zero


# ── the constructor refuses the silent-error path ─────────────────────────────
def test_D_refuses_float_because_it_imports_binary_error():
    with pytest.raises(TypeError):
        D(0.1)                      # Decimal(0.1) == 0.1000000000000000055511151231257827…


def test_D_is_exact_from_the_wire_string():
    assert D("0.1") + D("0.2") == D("0.3")          # the canonical float failure, exact here
    assert float(D("0.1") + D("0.2")) != 0.1 + 0.2  # ...and float still gets it wrong


def test_from_float_is_the_explicit_named_escape_hatch():
    assert from_float(0.1) == D("0.1")   # repr-based → shortest round-trip, not the binary expansion


# ── the fee bug: ceil at centicent precision ──────────────────────────────────
def test_ceil_to_fixes_the_fee_overcharge():
    # float: 0.07*0.5*0.5*10000 == 175.00000000000003 → bare ceil() → 176 → $0.0176 (overcharge)
    assert math.ceil(0.07 * 0.5 * 0.5 * 10000) == 176, "the float bug still reproduces"
    # exact: the fee is 0.0175 on the nose, no pre-round guard required
    assert ceil_to(D("0.07") * D("0.5") * D("0.5"), CENTICENT) == D("0.0175")


def test_ceil_to_still_rounds_a_genuine_fraction_up():
    assert ceil_to(D("0.01234"), CENTICENT) == D("0.0124")   # real excess → rounds up
    assert ceil_to(D("0.0175"), CENTICENT) == D("0.0175")    # exact multiple → unchanged


# ── the tick bug: floor of a price/tick ratio ─────────────────────────────────
def test_floor_to_fixes_the_tick_off_by_one():
    assert math.floor(0.29 / 0.01) == 28, "the float bug still reproduces"
    assert floor_to(D("0.29"), CENT) == D("0.29")            # exact — stays on its own tick
    assert floor_to(D("0.2949"), CENT) == D("0.29")          # genuine fraction floors down


def test_floor_to_never_rounds_a_price_up():
    # load-bearing: flooring a limit must only ever make us pay LESS, never push past breakeven
    for raw in ("0.4999", "0.4301", "0.0101"):
        assert floor_to(D(raw), CENT) <= D(raw)


# ── the dust bug: compare-to-zero on a removed book level ─────────────────────
def test_is_zero_treats_book_dust_as_gone():
    dust = D("1E-13")                     # what orderbook_delta arithmetic left on a removed level
    assert dust > 0                        # a naive `qty > 0` keeps the ghost level...
    assert is_zero(dust)                   # ...but at venue resolution it is simply gone


def test_is_zero_keeps_real_depth():
    assert not is_zero(D("0.01"))
    assert not is_zero(D("174.69"))        # the real level the ghost was masking


# ── quantized results land exactly on the venue grid ─────────────────────────
@pytest.mark.parametrize("step", [CENT, CENTICENT])
def test_results_sit_exactly_on_the_grid(step):
    for raw in ("0.4270", "0.0175", "0.9900", "0.3333"):
        assert floor_to(D(raw), step) % step == 0
        assert ceil_to(D(raw), step) % step == 0


def test_grid_constants_match_the_venues():
    assert CENTICENT == Decimal("0.0001")   # Kalshi fee grid (schedule: "rounded to a centicent")
    assert CENT == Decimal("0.01")          # Poly fee grid (cent, on the ORDER TOTAL)

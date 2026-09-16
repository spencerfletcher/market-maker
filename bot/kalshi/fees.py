"""The venue taker-fee formulas — pure functions, extracted from `cross_arb` (Kalshi 2026-08-13,
Poly 2026-09-05; public-repo decoupling: the Kalshi maker client and scripts/settlement_capture.py
must not import the arb engine for fee math). `cross_arb` re-exports both; the value pins live in
tests/test_kalshi_fee_model.py and tests/test_poly_fee_model.py.

`kalshi_tick_floor` moved here verbatim from `cross_arb` 2026-09-05 (price-grid arithmetic, no prod
caller left once the arb engine goes); its pins live in tests/test_kalshi_arb.py and
tests/test_decimal_phase1.py.
"""
from __future__ import annotations

from decimal import Decimal

from bot.core.money import CENTICENT, D, ceil_to, floor_to, from_float


def _effective_share_cost(p_poly: float, fee_rate: float = 0.0) -> float:
    """
    Effective cost per share purchased on Polymarket (including taker fee).

    Payout on resolution is always <n> per share; the taker fee raises the entry cost:

        fee (USDC, per ORDER) = round(Θ · C · p · (1-p), 2)      ← rounded to the CENT on the TOTAL
        fee_per_share         = Θ · p · (1-p)
        effective_cost        = p + Θ · p · (1-p)

    [VERIFIED 2026-07-15 against 9 REAL exchange commissions (commissionNotionalTotalCollected,
    read back from /v1/order/{id} + portfolio/activities across p=0.008…0.67). All 9 match the
    formula EXACTLY at the Θ in force when they traded. Pinned: tests/test_poly_fee_model.py.
    Previously this docstring cited "Polymarket's published schedule" with no first-party check —
    the same docs-say-so provenance that produced the Kalshi cent-vs-centicent bug.]

    Θ comes from the market's own `feeCoefficient` (scanner._event_fee_rate), NOT a constant —
    Poly RAISED it 0.05 → 0.06 between 2026-06-17 and 2026-07-15 and the live path self-corrected
    because it reads the venue per-market. Surcharge is symmetric around p=0.5 and peaks there:
    at Θ=0.06, p=0.5 adds 0.015/share (<n> per 100 shares).

    ⚠️ NOTHING IS FEE-FREE. A previous version claimed "NBA/NHL (fee_rate=0): effective_cost =
    p_poly" — FALSE: all 29 live pairs, including all 17 NBA, report feeCoefficient=0.06. The
    fee_rate<=0 branch below is a defensive no-fee path, NOT a description of any live sport.

    NOT MODELLED: the round-to-cent on the order total (±<n>/order — ±<n>/share at C=8,
    ±<n> at C=100). It is a symmetric ROUND, so it is unbiased noise, not a systematic
    understatement — immaterial against the 2% floor at our sizes. Revisit if size drops to ~1.
    """
    if fee_rate <= 0:
        return p_poly
    return p_poly + fee_rate * p_poly * (1 - p_poly)


def _kalshi_taker_fee(price: float, count: int = 1) -> float:
    """Per-contract Kalshi taker fee, ceiled to the CENTICENT (<n>) on the ORDER TOTAL.

    Kalshi's published formula (docs/kalshi-fee-schedule.pdf, effective 2026-07-07), verbatim:
        fees = round up(M x 0.07 x C x P x (1-P))
        "round up = rounds up such that the fee + positionCost is rounded to a centicent"
    M (taker multiplier) defaults to 1, and is explicitly 1 for every series we trade
    (KXMLBGAME / KXWCGAME / KXWNBAGAME are listed at taker M=1; KXNBAGAME / KXNHLGAME are not
    listed at all, so they take the default 1) — so M is omitted here. Ceiling applies to the
    order TOTAL, not per contract (measured: 3 @ 0.45 → <n>, not <n>).

    ⚠️ CENTICENT (1e-4), **NOT** cent. The schedule's convenience table ("<n> → <n> for 1
    contract", pp.4-5) is DISPLAY-rounded up to the cent and is NOT the formula — the same
    table's 100-contract column shows the truth (<n> → <n> = 0.07·100·0.3·0.7 exactly).
    Implementing that table was the original bug (fixed 2026-07-14): it overstated the fee by
    up to ~0.9c/contract, which UNDERSTATED every edge — ~10% of `below_min_edge` rejects
    (327/3234) actually cleared the 2% net floor. Read the formula, never the table.

    VERIFIED against real fills 2026-07-14 (pinned in tests/test_kalshi_fee_model.py):
      prod  1 @ 0.4270 → <n>   (sub-cent price; centicent ceil of 0.017127)
      demo  3 @ 0.45   → <n>   (excludes per-contract ceiling, which gives 0.0522)
      demo  1 @ 0.30   → <n>   (exact centicent; excludes our old <n>)

    EXACT since 2026-07-19 (Decimal Phase 1). This ceil is the operation the whole money core exists
    for: `0.07*0.5*0.5*10000 == 175.00000000000003` in float, so a bare ceil() yields 176 → <n>
    instead of the true <n> (the schedule's own 100-contract column confirms <n> → <n>). That
    was previously held off by a hand-placed round() before the ceil; `ceil_to` makes it structural, so
    the guard cannot be dropped by a future edit. Float in/out is deliberate for now — callers still do
    `opp.kalshi_ask + _kalshi_taker_fee(...)`.

    Equivalence to the old float path is not merely asserted, it was swept: over ALL 9,999 four-decimal
    prices × counts {1,2,3,5,10,12,50,100,833,1000} plus 400k random full-precision floats, old and new
    are BIT-IDENTICAL. That is structural, not luck — with p = k·1e-4 the pre-ceil quantity
    `7·count·k·(10000−k)·1e-6` is an exact multiple of 1e-6, precisely the grid the old round() used.
    See kalshi_tick_floor for the boundary caveat on COMPUTED (non-wire) inputs. Direction is pinned:
    the fee may only be over-charged relative to the raw formula, never under — so `kalshi_effective` is
    never understated and `edge` never overstated.
    """
    total = kalshi_taker_fee_total_d(from_float(price), count)   # ceil on the ORDER TOTAL
    return float(total / D(int(count)))                          # ...then per contract


def kalshi_taker_fee_total_d(price: Decimal, count: int = 1) -> Decimal:
    """The Kalshi taker fee on the ORDER TOTAL, EXACT — `ceil(0.07 · C · P · (1−P))` to the
    CENTICENT (<n>, **not** cent).

    ⛔ **THE FORMULA LIVES HERE ONCE.** `_kalshi_taker_fee` above delegates to it and floats the
    per-contract result at its own documented boundary; nothing else may re-type it. Added
    2026-09-02 for `scripts/kalshi_hedge_shadow.py`, which is Decimal end-to-end (CLAUDE.md § Code
    style: prices, quantities and money are `Decimal`, never `float`) and had no exact entry point
    — the alternatives were laundering money through the float boundary or writing a SECOND copy
    of the formula, and a second copy of a fee formula is exactly how the display-table bug
    survived (it understated every logged edge by ~<n> for months).

    ⛔ The arithmetic is BYTE-IDENTICAL to what `_kalshi_taker_fee` did inline — same operand
    order, same single `ceil_to` on the total — so every value pinned in
    `tests/test_kalshi_fee_model.py` is unchanged. ⚠️ `price` is a `Decimal` here and is used
    VERBATIM: pass the venue's wire string (`D("0.4270")`), never `Decimal(0.427)`, which launders
    a float's error into the Decimal and defeats the point.

    ⚠️ TAKER only. The MAKER fee is this formula at 0.0175 instead of 0.07 and applies only to
    `fee_type == "quadratic_with_maker_fees"` series — read that from `/series/{s}`, never from
    the PDF's silence (`KalshiClient.series_charges_maker_fee`).
    """
    c = D(int(count))
    return ceil_to(D("0.07") * c * price * (D(1) - price), CENTICENT)


def kalshi_tick_floor(price: float, tick: float = 0.01) -> float:
    """Round a price DOWN to the nearest Kalshi price tick (`tick`, default <n>).

    Kalshi rejects orders priced off a whole-tick boundary (HTTP 400). We round the
    price we'd pay DOWN — never up — so a tick adjustment can only make the trade
    cheaper (more profitable), never push it past breakeven. The round() before
    floor() absorbs float noise (e.g. 0.29/0.01 == 28.999999996 → 29, not 28).
    `tick` is read live from the market (`price_ranges[].step`); the 0.01 default is the fallback.
    ⚠️ CORRECTED 2026-07-19: this used to say "every binary market is `linear_cent` today" and that
    "_parse_price_tick guarantees ≥ 0.01". BOTH are false. Kalshi's tick is a FUNCTION OF PRICE —
    `tapered_deci_cent` markets (elections/politics) step 0.001 below 0.10 and above 0.90 — so
    `scanner.price_tick_at` can legitimately return 0.001 and the ≥0.01 guarantee never existed for
    a tail price. What IS guaranteed is non-zero, which is all this function needs to avoid a
    divide-by-zero. Every series that has actually reached the fire path logged a 0.01 tick.

    EXACT since 2026-07-19 (Decimal Phase 1): the old `math.floor(round(price/tick, 6)) * tick` relied
    on a hand-placed round() to absorb float noise. `floor_to` makes that structural — the guard can no
    longer be forgotten. Float in/out is deliberate for now (callers still pass floats).

    ⚠️ BOUNDARY SEMANTICS — precise, because Phase 2 will feed these helpers many more computed values:
    `from_float` uses repr's shortest round-trip, so a value that came off the WIRE is recovered exactly
    ("0.4270" → Decimal('0.427')); for wire prices/ticks the conversion is lossless and old/new agree
    bit-for-bit. It is NOT a tolerance band: noise on a COMPUTED float is imported faithfully, and unlike
    the old `round(x, 6)` there is nothing to absorb it. Verified consequence: sweeping the full
    `_kalshi_breakeven_ask → kalshi_tick_floor` chain found exactly one divergence (poly_effective 0.7888
    at buffer 0.0 → old 0.20, new 0.19), unreachable in production because `_fok_buffer` never returns 0
    at the deployed 2% floor — 0 divergences over 1.5M randomized production-parameter draws. Rounding
    direction is preserved everywhere (new ≤ old), so any residual difference costs a fill, never money."""
    return float(floor_to(from_float(price), from_float(tick)))

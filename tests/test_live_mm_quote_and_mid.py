"""Two real-money surfaces of the live paper-MM: what we mark inventory at, and where we quote.

`_mid_from` feeds the LOSS CAP and the headline P&L. `_improve` decides whether a size-1 maker can
reach the front of the queue at all. Both were added/hardened 2026-07-19 after a pre-run review.
"""
from decimal import Decimal as D

from bot.kalshi.maker import _TICK, _improve, _mid_from


# ── _mid_from: a one-sided book is NOT a price ───────────────────────────────────────────
# In orderbook mode feed._derive fabricates the missing side (yes_ask=1.0 on an empty NO ladder,
# yes_bid=0.0 on an empty YES ladder). Both are positive and finite, so a `bid>0 and ask>0` test
# waves them through and invents a mid. That value marks inventory for the kill switch.

def test_mid_on_a_normal_two_sided_book():
    assert _mid_from(D("0.23"), D("0.31")) == D("0.27")


def test_empty_no_ladder_is_not_a_price():
    """feed._derive yields yes_ask=1.0 when nobody bids NO. That means 'nobody is bidding NO',
    NOT 'YES is worth a dollar'. Marking long 3 @0.23 against it invented +<n> of profit."""
    assert _mid_from(D("0.23"), D("1.0")) is None


def test_empty_yes_ladder_is_not_a_price():
    """The mirror: yes_bid=0.0 would mark a short as a total win and a long as a total loss."""
    assert _mid_from(D("0.0"), D("0.31")) is None


def test_crossed_book_is_not_a_price():
    assert _mid_from(D("0.60"), D("0.55")) is None


def test_missing_sides_are_not_prices():
    assert _mid_from(None, D("0.31")) is None
    assert _mid_from(D("0.23"), None) is None
    assert _mid_from(None, None) is None


def test_a_locked_book_still_prices():
    """bid == ask is degenerate but not fabricated — it prices to itself rather than to None."""
    assert _mid_from(D("0.40"), D("0.40")) == D("0.40")


# ── _improve: buying queue priority, symmetrically or not at all ─────────────────────────

def test_zero_ticks_quotes_at_the_touch():
    assert _improve(D("0.23"), D("0.31"), 0) == (D("0.23"), D("0.31"))


def test_one_tick_steps_inside_on_both_sides():
    bid, ask = _improve(D("0.23"), D("0.31"), 1)
    assert bid == D("0.24")
    assert ask == D("0.30")


def test_a_one_tick_spread_cannot_be_improved():
    """Nowhere to go: the next price level IS the other side."""
    assert _improve(D("0.40"), D("0.41"), 1) == (D("0.40"), D("0.41"))


def test_a_two_tick_spread_cannot_be_improved_without_locking():
    """Both sides would land on 0.41. Improving only the bid is not a neutral treatment — it buys
    with sole priority while selling from the back of the queue, i.e. a systematic long."""
    assert _improve(D("0.40"), D("0.42"), 1) == (D("0.40"), D("0.42"))


def test_three_ticks_is_the_minimum_spread_that_admits_one_tick_of_improvement():
    bid, ask = _improve(D("0.40"), D("0.43"), 1)
    assert bid == D("0.41")
    assert ask == D("0.42")
    assert bid < ask


def test_two_ticks_of_improvement_needs_five_ticks_of_spread():
    assert _improve(D("0.40"), D("0.44"), 2) == (D("0.40"), D("0.44"))   # 4 ticks — would lock
    bid, ask = _improve(D("0.40"), D("0.45"), 2)                          # 5 ticks — fits
    assert bid == D("0.42")
    assert ask == D("0.43")
    assert bid < ask


def test_improved_quotes_never_cross_across_the_whole_grid():
    """The property that matters: whatever the spread, we never lock or cross ourselves."""
    for lo in range(1, 99):
        for width in range(1, 99 - lo):
            bid, ask = _improve(lo * _TICK, (lo + width) * _TICK, 1)
            assert bid < ask, f"locked at touch {lo * _TICK}/{(lo + width) * _TICK}"


def test_negative_ticks_are_a_no_op_not_a_widening():
    assert _improve(D("0.23"), D("0.31"), -1) == (D("0.23"), D("0.31"))


# ── _fill_ts: markout horizons are only meaningful if anchored to the VENUE'S fill time ──────────
# The 2026-07-19 run recorded all 12 markout rows at age=151s: horizons were measured from when a
# fill was DETECTED, and fills were polled every 150s, so the 5s and 30s horizons fired together.

import time as _time

from bot.kalshi.maker import _fill_ts


def test_epoch_seconds_from_the_venue_are_used_verbatim():
    ts, src = _fill_ts({"ts": 1784502017})
    assert ts == 1784502017.0
    assert src == "venue"


def test_epoch_milliseconds_are_detected_and_rescaled():
    """A ms epoch taken at face value lands ~55,000 years out, which makes every horizon
    instantly 'due' and silently produces a full set of garbage markouts."""
    ts, src = _fill_ts({"ts": 1784502017000})
    assert ts == 1784502017.0
    assert src == "venue"


def test_iso_timestamps_are_accepted():
    ts, src = _fill_ts({"ts": "2026-07-19T23:00:17Z"})
    assert src == "venue"
    assert 1784500000 < ts < 1784600000


def test_created_time_is_the_fallback_field_before_giving_up():
    ts, src = _fill_ts({"created_time": 1784502017})
    assert ts == 1784502017.0
    assert src == "venue"


def test_a_missing_or_junk_timestamp_falls_back_but_SAYS_SO():
    """The fallback is the old broken behaviour. It must be labelled so an analysis can drop those
    rows rather than average them in and quietly reproduce the defect."""
    for bad in ({}, {"ts": None}, {"ts": "not-a-date"}, {"ts": ""}, {"ts": 0}):
        ts, src = _fill_ts(bad)
        assert src == "detected", f"{bad} should be flagged as detection-anchored"
        assert abs(ts - _time.time()) < 5


def test_an_implausible_epoch_is_rejected_rather_than_trusted():
    """Out-of-range values are venue drift, not timestamps — fail to 'detected', never to 1970."""
    for bad in ({"ts": 1}, {"ts": -1}, {"ts": 5e9}):
        _, src = _fill_ts(bad)
        assert src == "detected"


# ── _queue_ahead: the number that tests the improvement hypothesis ───────────────────────────────
# Under price-time priority, contracts resting ahead of us is what decides whether a size-1 maker
# ever trades. The 2026-07-19 run could not say whether a zero-fill market was thin or whether we
# were simply behind 35 contracts.

from bot.kalshi.maker import _fmt_ahead, _queue_ahead

TB, TA = D("0.40"), D("0.45")        # touch
DB, DA = D("120"), D("35")           # visible depth at each side of the touch


def test_quoting_at_the_touch_puts_the_whole_visible_level_ahead_of_us():
    assert _queue_ahead("buy", D("0.40"), TB, TA, DB, DA) == (DB, False)
    assert _queue_ahead("sell", D("0.45"), TB, TA, DB, DA) == (DA, False)


def test_improving_the_touch_means_an_empty_queue_and_first_place():
    assert _queue_ahead("buy", D("0.41"), TB, TA, DB, DA) == (0, True)
    assert _queue_ahead("sell", D("0.44"), TB, TA, DB, DA) == (0, True)


def test_a_worse_than_touch_price_has_UNKNOWN_depth_never_zero():
    """We only observe L1. Reporting 0 at a price we cannot see would read as 'first in an empty
    queue' — inventing priority we do not have, and flattering the very arm being tested."""
    ahead, improved = _queue_ahead("buy", D("0.38"), TB, TA, DB, DA)
    assert ahead is None and improved is False
    ahead, improved = _queue_ahead("sell", D("0.47"), TB, TA, DB, DA)
    assert ahead is None and improved is False


def test_representation_does_not_decide_whether_we_are_at_the_touch():
    """The sent price is a formatted string re-parsed while the touch comes from the feed, so `==`
    used to be a coin flip on representation — the trap that put 6 of 99 on-grid cent prices a full
    tick off the touch in the shadow probe. Both are exact Decimals now AND the half-tick tolerance
    is retained, so neither a differing SCALE ("0.4000" vs "0.40") nor sub-tick noise can move us
    off the touch."""
    for noisy in (D("0.4000"), D("0.4000000001"), D("0.3999999999")):
        assert _queue_ahead("buy", noisy, TB, TA, DB, DA) == (DB, False)


def test_unknown_renders_as_unknown_and_not_as_a_number():
    assert _fmt_ahead(None) == "unknown"
    assert _fmt_ahead(0.0) == "0"
    assert _fmt_ahead(35.0) == "35"


# ── _tick: rounding a quote is a MONEY decision, not a formatting one ────────────────────────────

from bot.kalshi.maker import _skew_guard, _skew_ticks, _tick as _tick_fn


def test_a_bid_floors_and_an_ask_ceils_so_discretisation_never_concedes():
    """Bare round() made a sub-tick skew a STEP FUNCTION: nothing at |inv|<=1, a full 1c concession
    at |inv|>=2. Against a measured edge of +0.25-0.5c/fill that hands over 2-4x the whole edge."""
    assert _tick_fn(D("0.406"), "buy") == "0.4000"     # would have been 0.41 — a 1c overpay
    assert _tick_fn(D("0.404"), "buy") == "0.4000"
    assert _tick_fn(D("0.456"), "sell") == "0.4600"
    assert _tick_fn(D("0.454"), "sell") == "0.4600"    # ask ceils — never sell cheaper than asked


def test_on_grid_prices_are_unchanged_on_both_sides():
    """The float trap: 0.41/0.01 is 40.99999999999999, so a bare floor drops an on-grid price a
    whole tick. Every cent must survive both roundings."""
    for cents in range(1, 100):
        px = D(cents).scaleb(-2)
        assert _tick_fn(px, "buy") == f"{px:.4f}", f"buy moved {px}"
        assert _tick_fn(px, "sell") == f"{px:.4f}", f"sell moved {px}"


def test_prices_stay_inside_the_venue_range():
    assert _tick_fn(D("-5.0"), "buy") == "0.0100"
    assert _tick_fn(D("5.0"), "sell") == "0.9900"


# ── _skew_ticks: the inventory/OBI lean is a WHOLE number of ticks, rounded once ─────────────────

def test_a_sub_half_tick_lean_rounds_to_zero_ticks_not_a_full_tick():
    """The defect this replaces: the old float shift fed floor/ceil, so a 0.3c lean moved the
    accumulating side a FULL tick (verified: _tick(0.45-0.003,'buy')==0.44). Discretising to whole
    ticks FIRST means a sub-half-tick intent is honestly zero, not a silent 1c concession."""
    assert _skew_ticks(D(0), D(1), 0.0, 0.003) == 0   # 0.3c intent -> 0 ticks (was a full-tick move)
    assert _skew_ticks(D(0), D(2), 0.0, 0.003) == -1  # 0.6c -> 1 tick, reached honestly
    assert _skew_ticks(D(0), D(3), 0.0, 0.003) == -1  # 0.9c -> 1 tick


def test_skew_leans_AWAY_from_inventory_and_both_sides_move_together():
    """Sign is load-bearing: long -> negative ticks -> both quotes DOWN (discourage buy, encourage
    sell). A flipped sign leans into the losing side, the class of bug the OBI-inversion fix warned
    of. One tick count is applied to both quotes, so they recenter together (Avellaneda-Stoikov),
    unlike the old floor-vs-ceil which moved the two sides by different counts."""
    assert _skew_ticks(D(0), D(5), 0.0, 0.01) < 0     # long -> down
    assert _skew_ticks(D(0), D(-5), 0.0, 0.01) > 0    # short -> up
    assert _skew_ticks(D(0), D(0), 0.0, 0.01) == 0    # flat -> no lean


def test_obi_and_inventory_fold_into_one_rounded_tick_count():
    assert _skew_ticks(D(1), D(1), 0.01, 0.01) == 0   # (0.01 - 0.01)/tick = 0
    assert _skew_ticks(D(1), D(0), 0.02, 0.0) == 2    # OBI alone: 0.02/tick = 2 ticks
    assert isinstance(_skew_ticks(D("0.5"), D("1.7"), 0.013, 0.007), int)


# ── _skew_guard: refuse a coef that NEVER moves a quote, not one that merely rounds sub-tick ─────

def test_the_default_config_is_allowed_because_it_reaches_a_tick_at_the_cap():
    """0.003 * 3.0 = 0.009 -> round(0.9) = 1 tick. The OLD guard wrongly refused this as 'inert',
    forcing the operator to run with skew OFF (the -<n> MILRGASSER run)."""
    assert _skew_guard(0.003, 3.0) is None


def test_a_coef_that_rounds_to_zero_ticks_even_at_the_cap_is_refused():
    reason = _skew_guard(0.001, 3.0)              # 0.003 -> round(0.3) = 0 ticks: never moves
    assert reason and "never moves" in reason


def test_skew_disabled_explicitly_is_allowed():
    assert _skew_guard(0.0, 3.0) is None


def test_a_skew_that_can_move_a_tick_at_the_cap_is_allowed():
    assert _skew_guard(0.01, 3.0) is None         # 3c at the cap
    assert _skew_guard(1.0 / 3.0 / 100, 3.0) is None


# ── Phase D step 2: the whole quote path is EXACT Decimal ────────────────────────────────────────
# CLAUDE.md § Code style: prices, quantities and money are Decimal, never float. The shortcuts these
# pin are the ones this repo has actually been bitten by — `0.47/0.01 == 46.99999999999999` moving a
# price a full tick off the touch, and a float shift turning a sub-tick lean into a one-sided step.
# Exactness makes both unrepresentable rather than guarded-against.

from bot.kalshi.maker import _own_at


def test_the_tick_grid_itself_is_exact():
    """`_TICK` is added to and subtracted from prices all over the quote path. As a float it makes
    every derived price approximate, so the exactness has to start here."""
    assert isinstance(_TICK, D) and _TICK == D("0.01")


def test_a_float_cannot_be_laundered_through_the_tick_snapper():
    """The observable half of `_tick`'s migration. Its OUTPUT is value-identical to the old float
    implementation over the whole 4-dp grid — measured, 0 divergences in 22,180 cases — so no
    assertion on the returned string can tell the two apart. What genuinely changed is that a float
    reaching the last hop before an order price now RAISES instead of being quietly snapped, which
    is what stops the exact chain (`_TICK` → `_skew_ticks` → `bid + shift`) being re-broken from
    the outside by one caller.

    ⚠️ SCOPE — it raises for IN-BAND floats only, and that is pinned below rather than left for a
    reader to discover. The `[_MIN_PX, _MAX_PX]` clamp runs first and Python compares a float against
    a Decimal happily, so an OUT-OF-BAND float is replaced by the Decimal bound and returns the clamp
    without ever reaching `floor_to`. That is the pre-existing safety net answering conservatively,
    not a hole — but "a float always raises here" would be an overclaim, and this file is where a
    future reader would check."""
    import pytest as _pytest
    for bad in (0.406, 0.41, 0.02, 0.98):          # inside [_MIN_PX, _MAX_PX] → the clamp is a no-op
        for side in ("buy", "sell"):
            with _pytest.raises(TypeError):
                _tick_fn(bad, side)
    # OUT of band: the clamp substitutes the Decimal bound and the float never reaches the snapper.
    assert _tick_fn(1.5, "sell") == "0.9900"
    assert _tick_fn(-0.5, "buy") == "0.0100"


def test_a_whole_tick_shift_off_an_on_grid_price_stays_on_the_grid():
    """`_skew_ticks` hands `_tick` an on-grid price plus a whole number of ticks — the ONLY shape
    production produces. `0.47 − 0.01` is 0.45999999999999996 in float, which floors to 0.45: a free
    tick given away on the accumulating side, silently."""
    assert _tick_fn(D("0.47") - _TICK, "buy") == "0.4600"
    assert _tick_fn(D("0.47") + _TICK, "sell") == "0.4800"
    assert _tick_fn(D("0.29") - 2 * _TICK, "buy") == "0.2700"
    assert _tick_fn(D("0.83") + 3 * _TICK, "sell") == "0.8600"


def test_mid_is_an_exact_decimal_not_a_float():
    """The mid marks inventory for the LOSS CAP and for the headline P&L, so it is money."""
    m = _mid_from(D("0.44"), D("0.45"))
    assert isinstance(m, D) and m == D("0.445")
    assert _mid_from(D("0.23"), D("0.31")) == D("0.27")


def test_improvement_is_exact_and_needs_no_epsilon_at_the_boundary():
    """The float version compared `(ask - bid) >= (2*imp + _TICK) - 1e-9`, an epsilon that existed
    only because `0.43 - 0.40` is 0.029999999999999995. Exactly three ticks of spread must admit
    exactly one tick of improvement, and exactly two ticks must not — with no fudge factor."""
    bid, ask = _improve(D("0.40"), D("0.43"), 1)
    assert (bid, ask) == (D("0.41"), D("0.42"))
    assert isinstance(bid, D) and isinstance(ask, D)
    assert _improve(D("0.40"), D("0.42"), 1) == (D("0.40"), D("0.42"))   # would lock
    assert _improve(D("0.40"), D("0.44"), 2) == (D("0.40"), D("0.44"))   # 4 ticks: would lock
    assert _improve(D("0.40"), D("0.45"), 2) == (D("0.42"), D("0.43"))   # 5 ticks: fits


def test_the_skew_lean_is_computed_exactly():
    """A tick count is a policy decision made from money inputs, so it is computed in Decimal.
    ⚠️ At an EXACT half-tick the exact path rounds half-to-even where the float path rounded in
    whichever direction its representation error happened to fall — see the step-2 report."""
    assert _skew_ticks(D(0), D(3), 0.0, 0.003) == -1      # 0.9c long lean → 1 tick down
    assert _skew_ticks(D(0), D(1), 0.0, 0.003) == 0       # 0.3c → honestly zero
    assert _skew_ticks(D(0), D(-5), 0.0, 0.01) > 0        # short → lean up
    assert isinstance(_skew_ticks(D("0.5"), D("1.7"), 0.013, 0.007), int)


def test_an_exact_half_tick_lean_rounds_half_to_even_deterministically():
    """THE ONE BEHAVIOUR THIS MIGRATION CHANGED, pinned so it is a decision rather than a side
    effect. `0.02 × 14.25 / 0.01` is EXACTLY 28.5, a tie. Exact arithmetic rounds half-to-even and
    gives 28; the float path computed 28.500000000000004 and gave 29 — and it was not consistently
    away-from-zero either (`0.05 × 2.30 / 0.01` = 11.5 rounded DOWN to 11 there), so the old answer
    was a property of the representation error, not a policy.

    Scope, measured with `--obi-coef 0` over |inv| ≤ 20.00 at 0.01 granularity: at the DEFAULT
    `--inv-coef 0.003` there are ZERO ties anywhere, so the shipped configuration does not reach one.
    ⚠️ That is a statement about the DEFAULT, not about reachability — an earlier version of this
    docstring claimed ties needed `inv_coef ≥ 0.02` AND `|inv| ≥ 14` and were therefore outside
    anything `--inv-cap` admits, which is false: raising the coef brings them inside the cap. The
    first tie within the default `--inv-cap 3` is at inv_coef 0.022, and at inv_coef 0.05 there are 6,
    the smallest at |inv| = 0.10 — one partial fill. See the step-2 report §4.2."""
    assert _skew_ticks(D(0), D("14.25"), 0.0, 0.02) == -28     # tie → even, not −29
    assert _skew_ticks(D(0), D("-14.25"), 0.0, 0.02) == 28     # and symmetric in sign
    assert _skew_ticks(D(0), D("14.75"), 0.0, 0.02) == -30     # −29.5 → even, not −29


def test_our_own_resting_size_is_netted_out_on_an_exact_price_match():
    """`_own_at` subtracts our last cycle's own size from the visible depth before asking how many
    contracts are ahead of us. A missed match fails SILENTLY, in the direction of not subtracting."""
    own = {("buy", D("0.40")): D("2"), ("sell", D("0.45")): D("1")}
    assert _own_at(own, "buy", D("0.4000")) == D("2")
    assert _own_at(own, "sell", D("0.45")) == D("1")
    # The NO-MATCH case must still be an exact zero, not a bare int from `sum([])` — it is
    # subtracted from an exact depth, and a mixed-type zero is how a float creeps back in.
    empty = _own_at(own, "buy", D("0.39"))
    assert empty == 0 and isinstance(empty, D)

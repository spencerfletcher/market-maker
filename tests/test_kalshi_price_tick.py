"""Kalshi's tick is a FUNCTION OF PRICE, not a per-market constant.

There is no tick field on a Kalshi market — each carries a `price_ranges` step ladder. Two shapes
exist [VERIFIED 2026-07-19 live, 15 series]: `linear_cent` (0.01 throughout) and
`tapered_deci_cent` (0.001 below 0.10 and above 0.90, 0.01 in between).

`_parse_price_tick` used to read `price_ranges[0].step`, which on a tapered market is the 0.00–0.10
TAIL — returning 0.001 for a market whose tick at any tradeable price is 0.01. Its output feeds
`kalshi_tick_floor` on the fire path, and Kalshi rejects an off-tick price with HTTP 400.
"""
from bot.kalshi.scanner import _parse_price_tick, price_tick_at

LINEAR = {"price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}]}
TAPERED = {"price_ranges": [                       # the SENATEIA / elections shape
    {"start": "0.0000", "end": "0.1000", "step": "0.0010"},
    {"start": "0.1000", "end": "0.9000", "step": "0.0100"},
    {"start": "0.9000", "end": "1.0000", "step": "0.0010"},
]}


def test_linear_cent_is_one_cent_everywhere():
    for p in (0.01, 0.05, 0.50, 0.95, 0.99):
        assert price_tick_at(LINEAR, p) == 0.01


def test_tapered_market_is_deci_cent_only_in_the_tails():
    assert price_tick_at(TAPERED, 0.05) == 0.001      # lower tail
    assert price_tick_at(TAPERED, 0.95) == 0.001      # upper tail
    assert price_tick_at(TAPERED, 0.50) == 0.01       # the band we actually quote in
    assert price_tick_at(TAPERED, 0.10) == 0.01       # boundary is start-inclusive
    assert price_tick_at(TAPERED, 0.61) == 0.01       # SENATEIA-28-R's live price


def test_scalar_tick_reports_the_contested_band_not_the_first_range():
    """THE REGRESSION. ranges[0] on a tapered market is the 0.001 tail; the tradeable tick is 0.01."""
    assert _parse_price_tick(TAPERED) == 0.01
    assert _parse_price_tick(LINEAR) == 0.01


def test_both_known_ladder_shapes_agree_at_every_price_the_fire_path_has_used():
    """What actually makes this safe to land: across the range of `kalshi_limit` values the fire
    path has ever produced (449 logged would_fire rows span 0.12-0.86), BOTH known ladder shapes
    return the same 0.01 — so the change cannot move a price that has ever been sent.

    Deliberately NOT asserted here: that every allowlisted series is `linear_cent`. Only 1 of the 12
    (KXNPBGAME) was in the 2026-07-19 ladder scan, and a test cannot establish a venue fact anyway.
    An earlier version of this test claimed exactly that while asserting a single unrelated line —
    green, and evidence of nothing."""
    for cents in range(12, 87):
        p = cents / 100.0
        assert price_tick_at(LINEAR, p) == price_tick_at(TAPERED, p) == 0.01


def test_never_returns_zero_or_none():
    """A zero tick divides-by-zero inside kalshi_tick_floor."""
    assert _parse_price_tick({}) == 0.01
    assert _parse_price_tick({"price_ranges": []}) == 0.01
    assert _parse_price_tick({"price_ranges": [{"start": "0", "end": "1", "step": "0"}]}) == 0.01
    assert _parse_price_tick({"price_ranges": [{"start": "0", "end": "1", "step": None}]}) == 0.01
    assert _parse_price_tick({"price_ranges": "not-a-list"}) == 0.01
    assert _parse_price_tick({"price_ranges": [{"step": "0.01"}]}) == 0.01   # no start/end keys


def test_a_malformed_range_is_skipped_rather_than_aborting_the_ladder():
    """One bad entry must not hide a good one behind it — the venue sends these as strings."""
    m = {"price_ranges": [
        {"start": "bad", "end": "0.1000", "step": "0.0010"},
        {"start": "0.1000", "end": "0.9000", "step": "0.0100"},
    ]}
    assert price_tick_at(m, 0.50) == 0.01


def test_price_outside_every_range_falls_back_to_a_cent():
    assert price_tick_at(TAPERED, 1.5) == 0.01
    assert price_tick_at(TAPERED, -0.1) == 0.01

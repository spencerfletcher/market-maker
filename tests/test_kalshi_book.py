"""Tests for Kalshi orderbook maintenance (orderbook mode) in kalshi_feed."""
import json
from decimal import Decimal

import pytest

from bot.kalshi.feed import KalshiOrderBookCache

T = "FED-23DEC-T3.00"


def _cache():
    # series_prefixes=["FED"] so the sample ticker passes the prefix filter.
    return KalshiOrderBookCache(client=None, series_prefixes=["FED"])


def _snapshot(seq=2):
    return json.dumps({
        "type": "orderbook_snapshot", "sid": 2, "seq": seq,
        "msg": {
            "market_ticker": T,
            "yes_dollars_fp": [["0.0800", "300.00"], ["0.2200", "333.00"]],
            "no_dollars_fp": [["0.5400", "20.00"], ["0.5600", "146.00"]],
        },
    })


def _delta(px, delta, side, seq=3):
    return json.dumps({
        "type": "orderbook_delta", "sid": 2, "seq": seq,
        "msg": {"market_ticker": T, "price_dollars": px, "delta_fp": delta, "side": side},
    })


@pytest.mark.asyncio
async def test_snapshot_builds_book_and_derives_prices():
    c = _cache()
    await c._handle_message(_snapshot())
    # best_yes_bid=0.22, best_no_bid=0.56 → yes_ask=1-0.56=0.44, no_ask=1-0.22=0.78
    assert c.get_best_bid(T, "yes") == pytest.approx(0.22)
    assert c.get_best_ask(T, "yes") == pytest.approx(0.44)
    assert c.get_best_bid(T, "no") == pytest.approx(0.56)
    assert c.get_best_ask(T, "no") == pytest.approx(0.78)
    # depth to BUY yes = size resting on the no bid we'd cross (146); buy no = 333
    assert c.get_depth(T, "yes") == pytest.approx(146.0)
    assert c.get_depth(T, "no") == pytest.approx(333.0)


@pytest.mark.asyncio
async def test_crossed_book_diagnostic_tracks_enter_and_exit():
    # A crossed book (best_yes_bid + best_no_bid > 1) is a no-arb violation that makes yes_ask =
    # 1-best_no_bid phantom-cheap — the suspected fire-path phantom-edge source. _derive tracks the
    # crossing in _crossed for the diagnostic; it must change nothing that fires.
    c = _cache()
    c._orderbook_mode = True
    crossed = json.dumps({
        "type": "orderbook_snapshot", "sid": 2, "seq": 2,
        "msg": {"market_ticker": T,
                "yes_dollars_fp": [["0.6600", "10.00"]],
                "no_dollars_fp": [["0.7100", "10.00"]]},   # 0.66 + 0.71 = 1.37 > 1 → crossed
    })
    await c._handle_message(crossed)
    assert T in c._crossed                                  # entered the crossed state
    assert c.get_best_ask(T, "yes") == pytest.approx(0.29)  # phantom-cheap: 1 - 0.71
    assert c.get_best_bid(T, "yes") == pytest.approx(0.66)  # > the ask → crossed, as observed live

    # book uncrosses (0.71 no bid removed, a 0.30 no bid added → sum 0.96) → flag clears
    await c._handle_message(_delta("0.7100", "-10.00", "no", seq=3))
    await c._handle_message(_delta("0.3000", "10.00", "no", seq=4))
    assert T not in c._crossed                              # exited the crossed state


@pytest.mark.asyncio
async def test_delta_below_dust_removes_level_no_crossing():
    # ROOT of KALSHI_CROSSED: a delta that leaves a residue must REMOVE the level, not keep it — else
    # max(price) selects the stale ~0-qty bid and crosses the book. Originally fixed by rounding qty to
    # _QTY_DP; since Decimal Phase 2 the book is exact, so sub-resolution residue is dropped by the
    # _QTY_STEP floor. NB: keys are Decimal now — a float key would never match, making a
    # `0.22 not in book` assertion pass VACUOUSLY.
    c = _cache()
    c._orderbook_mode = True
    await c._handle_message(_snapshot())                       # yes {0.08:300, 0.22:333}
    await c._handle_message(_delta("0.2200", "-332.999999999", "yes", seq=3))  # -> ~1e-9 residue
    assert Decimal("0.08") in c._books[T]["yes"]               # the surviving key IS Decimal-form...
    assert Decimal("0.22") not in c._books[T]["yes"]           # ...so this drop is real, not vacuous
    assert c.get_best_bid(T, "yes") == pytest.approx(0.08)     # bid falls to the real level, not stale 0.22
    assert T not in c._crossed                                 # book no longer crosses


@pytest.mark.asyncio
async def test_rounding_keeps_minimum_real_level():
    # Pin the MONEY-relevant direction: the smallest real Kalshi level (2-dp wire → 0.01 contracts)
    # must SURVIVE the dust rounding. Dropping a real level would hide an edge / under-report depth —
    # the failure direction _QTY_DP rounding must never hit (0.01 rounds to 0.01, dust to 0.0).
    c = _cache()
    c._orderbook_mode = True
    await c._handle_message(json.dumps({
        "type": "orderbook_snapshot", "sid": 2, "seq": 2,
        "msg": {"market_ticker": T,
                "yes_dollars_fp": [["0.4000", "0.01"], ["0.3000", "500.00"]],   # 0.40 = min real qty
                "no_dollars_fp": [["0.5000", "100.00"]]},
    }))
    # exact equality, not approx: the book is Decimal since Phase 2, so the wire value is preserved
    # bit-for-bit rather than approximately (pytest.approx also raises TypeError against Decimal)
    assert c._books[T]["yes"].get(Decimal("0.40")) == Decimal("0.01")  # min real qty kept, not dropped
    assert c.get_best_bid(T, "yes") == pytest.approx(0.40)             # and it is the real best bid


@pytest.mark.asyncio
async def test_exact_removal_leaves_no_dust_at_all():
    """Decimal Phase 2's actual guarantee — the one the float book could NOT provide.

    Previously `levels.get(px) + delta` on a full removal left ~1e-13 residue, and correctness
    depended on rounding it away afterwards. In exact arithmetic the removal lands on EXACTLY zero, so
    there is no dust to sweep and the crossed-book bug is unrepresentable rather than patched."""
    c = _cache()
    c._orderbook_mode = True
    await c._handle_message(_snapshot())                        # yes {0.08:300, 0.22:333}
    # remove the 0.22 level EXACTLY — the float path is what produced residue here
    await c._handle_message(_delta("0.2200", "-333.00", "yes", seq=3))
    assert Decimal("0.22") not in c._books[T]["yes"]            # gone, with no rounding step involved
    assert c.get_best_bid(T, "yes") == pytest.approx(0.08)      # best bid falls to the real level
    # and the surviving level is the exact wire value, not a float approximation of it
    assert c._books[T]["yes"][Decimal("0.08")] == Decimal("300.00")


@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity", "sNaN"])
@pytest.mark.asyncio
async def test_non_finite_wire_values_drop_the_level_and_never_raise(bad):
    """Decimal parses "NaN"/"Infinity" happily, but `Decimal("NaN") > 0` RAISES InvalidOperation and
    `hash(Decimal("NaN"))` raises TypeError — both OUTSIDE the parse try. Unguarded, a single bad field
    would escape _handle_message into the receive loop and cycle the whole Kalshi WS connection
    mid-game. The old float path just dropped the level (`float("nan") > 0` is False); preserve that."""
    c = _cache()
    c._orderbook_mode = True
    # snapshot carrying a non-finite qty must be absorbed, not raise
    await c._handle_message(json.dumps({
        "type": "orderbook_snapshot", "sid": 2, "seq": 2,
        "msg": {"market_ticker": T,
                "yes_dollars_fp": [["0.6500", bad], ["0.6100", "174.69"]],
                "no_dollars_fp": [["0.3000", "5.00"]]},
    }))
    assert Decimal("0.65") not in c._books[T]["yes"]        # non-finite level dropped
    assert c.get_best_bid(T, "yes") == pytest.approx(0.61)  # real level still selected

    # ...and a non-finite DELTA (qty or price) must be ignored without tearing down the socket
    await c._handle_message(_delta("0.6100", bad, "yes", seq=3))
    assert c.get_best_bid(T, "yes") == pytest.approx(0.61)  # untouched, connection intact
    await c._handle_message(_delta(bad, "1.00", "yes", seq=4))
    assert c.get_best_bid(T, "yes") == pytest.approx(0.61)


@pytest.mark.asyncio
async def test_book_keys_and_values_are_exact_decimals():
    """Pins the Phase-2 representation itself: a price is its own exact key, so two quotes of the same
    price can never occupy two dict slots (the float-key aliasing footgun)."""
    c = _cache()
    c._orderbook_mode = True
    await c._handle_message(_snapshot())
    yes = c._books[T]["yes"]
    assert all(isinstance(k, Decimal) for k in yes), "keys must be exact"
    assert all(isinstance(v, Decimal) for v in yes.values()), "quantities must be exact"
    # 1 - 0.55 is exactly 0.45 in Decimal (0.44999999999999996 in float) — the complement in _derive
    assert Decimal(1) - Decimal("0.55") == Decimal("0.45")


@pytest.mark.asyncio
async def test_derived_prices_are_exact_decimals_with_float_only_at_the_getters():
    """Phase D: the DERIVATION is exact too, not just the book it reads.

    `_derive` used to compute exact Decimal best-bids and then immediately `float()` them into
    `KalshiPriceData`, so the cached price — the thing every consumer reads and the thing
    `_note_price_change` compares for the frozen-book watchdog — was a float again. The exact island
    now extends to the cache; `get_best_bid`/`get_best_ask` are the named float boundary."""
    c = _cache()
    c._orderbook_mode = True
    await c._handle_message(_snapshot())     # yes bids {0.08:300, 0.22:333}, no bids {0.54:20, 0.56:146}
    pd = c._prices[T]
    assert isinstance(pd.yes_bid, Decimal) and isinstance(pd.yes_ask, Decimal)
    assert isinstance(pd.no_bid, Decimal) and isinstance(pd.no_ask, Decimal)
    # 1 - 0.56 is EXACTLY 0.44 here; in float it is 0.43999999999999995
    assert pd.yes_ask == Decimal("0.44") and pd.no_ask == Decimal("0.78")
    # the getters remain the float boundary (38 external call sites do float arithmetic)
    assert isinstance(c.get_best_ask(T, "yes"), float)
    assert isinstance(c.get_best_bid(T, "no"), float)


@pytest.mark.asyncio
async def test_touch_depth_is_exact_decimal_with_float_only_at_the_getter():
    """Touch depth is a QUANTITY read straight off the exact book — it must not be floated on the
    way into `_depth` and floated again on the way out."""
    c = _cache()
    c._orderbook_mode = True
    await c._handle_message(_snapshot())
    # isinstance on BOTH sides, and not `== Decimal("146.00")` — float(146.0) compares EQUAL to that,
    # so a value-only assertion is satisfied by the float the migration is removing.
    assert isinstance(c._depth[T]["yes"], Decimal) and isinstance(c._depth[T]["no"], Decimal)
    assert c._depth[T]["yes"] == Decimal("146.00") and c._depth[T]["no"] == Decimal("333.00")
    assert isinstance(c.get_depth(T, "yes"), float)  # boundary unchanged for consumers


@pytest.mark.asyncio
async def test_decimal_sibling_getters_expose_the_exact_cached_values():
    """Phase D step 2: the maker needs the EXACT price/depth, the arb bot still needs float.

    Flipping `get_best_bid`/`get_best_ask`/`get_depth` to Decimal is a money-path change to the
    STOPPED arb bot (`cross_arb.py`, `kalshi_arb.py` share them and cannot be smoke-tested), so the
    exact values are exposed through siblings instead and the float getters delegate.
    Scale matters, not just value: `float(0.44)` compares EQUAL to `Decimal("0.44")`, so a
    value-only assertion is satisfied by the float this exists to avoid."""
    c = _cache()
    c._orderbook_mode = True
    await c._handle_message(_snapshot())
    for got in (c.get_best_bid_d(T, "yes"), c.get_best_ask_d(T, "yes"),
                c.get_best_bid_d(T, "no"), c.get_best_ask_d(T, "no"),
                c.get_depth_d(T, "yes"), c.get_depth_d(T, "no")):
        assert isinstance(got, Decimal)
    assert c.get_best_bid_d(T, "yes") == Decimal("0.22")
    # 1 − 0.56 is EXACTLY 0.44 here; in float it is 0.43999999999999995
    assert c.get_best_ask_d(T, "yes") == Decimal("0.44")
    assert c.get_best_bid_d(T, "no") == Decimal("0.56")
    assert c.get_best_ask_d(T, "no") == Decimal("0.78")
    assert c.get_depth_d(T, "yes") == Decimal("146.00")
    assert c.get_depth_d(T, "no") == Decimal("333.00")
    # unknown ticker/side stays None on BOTH spellings — an absent price is not a zero price
    assert c.get_best_bid_d("NOPE", "yes") is None
    assert c.get_depth_d("NOPE", "yes") is None


@pytest.mark.asyncio
async def test_the_float_getters_delegate_to_the_decimal_siblings():
    """ONE derivation path, not two. If the float getter re-read `_prices`/`_depth` itself, a future
    change to either sibling would silently leave the two spellings of the same number disagreeing —
    which is the failure mode this repo has measured (two spellings of one price comparison sitting
    three lines apart put 6 of 99 on-grid cent prices a tick off the touch in the shadow probe).
    Pinned by substituting the sibling and requiring the float getter to follow it."""
    c = _cache()
    c._orderbook_mode = True
    await c._handle_message(_snapshot())
    c.get_best_bid_d = lambda ticker, side="yes": Decimal("0.07")
    c.get_best_ask_d = lambda ticker, side="yes": Decimal("0.93")
    c.get_depth_d = lambda ticker, side="yes": Decimal("11.00")
    assert c.get_best_bid(T, "yes") == 0.07 and isinstance(c.get_best_bid(T, "yes"), float)
    assert c.get_best_ask(T, "yes") == 0.93 and isinstance(c.get_best_ask(T, "yes"), float)
    assert c.get_depth(T, "yes") == 11.0 and isinstance(c.get_depth(T, "yes"), float)
    c.get_best_bid_d = lambda ticker, side="yes": None
    c.get_depth_d = lambda ticker, side="yes": None
    assert c.get_best_bid(T, "yes") is None
    assert c.get_depth(T, "yes") is None


@pytest.mark.asyncio
async def test_parse_drops_captured_dust_selects_real_level():
    # Evidence, not model: a snapshot carrying the exact dust magnitudes the KALSHI_CROSSED diagnostic
    # captured (~5.68e-14) must drop those levels at parse so max(price) selects the real 0.61 bid.
    c = _cache()
    c._orderbook_mode = True
    await c._handle_message(json.dumps({
        "type": "orderbook_snapshot", "sid": 2, "seq": 2,
        "msg": {"market_ticker": T,
                "yes_dollars_fp": [["0.6500", "5.68e-14"], ["0.6200", "3.4e-13"], ["0.6100", "174.69"]],
                "no_dollars_fp": [["0.3900", "1.20"]]},
    }))
    assert Decimal("0.61") in c._books[T]["yes"]                                # real level survives...
    assert (Decimal("0.65") not in c._books[T]["yes"]
            and Decimal("0.62") not in c._books[T]["yes"])                      # ...captured dust dropped
    assert c.get_best_bid(T, "yes") == pytest.approx(0.61)      # real bid, not the 0.65 dust
    assert T not in c._crossed                                  # no phantom crossing


@pytest.mark.asyncio
async def test_fillable_qty_at_limit():
    # Book: yes bids {0.08:300, 0.22:333}, no bids {0.54:20, 0.56:146}.
    # Buy NO lifts yes bids: a yes bid p is a NO offer at 1-p, takeable when 1-p<=limit
    # i.e. p>=1-limit. Real best NO offer = 1-0.22 = 0.78.
    c = _cache()
    await c._handle_message(_snapshot())
    # limit at the touch (0.78) → only the 0.22 yes bid qualifies → 333
    assert c.fillable_qty(T, "no", 0.78) == pytest.approx(333.0)
    # generous limit (0.92) reaches the 0.08 level too → 300+333 = 633
    assert c.fillable_qty(T, "no", 0.92) == pytest.approx(633.0)
    # limit BELOW the real offer (0.70 < 0.78) → nothing fillable (the phantom case)
    assert c.fillable_qty(T, "no", 0.70) == pytest.approx(0.0)
    # Buy YES lifts no bids: real best YES offer = 1-0.56 = 0.44.
    assert c.fillable_qty(T, "yes", 0.44) == pytest.approx(146.0)
    assert c.fillable_qty(T, "yes", 0.42) == pytest.approx(0.0)


def test_fillable_qty_none_without_book():
    # Ticker mode (no maintained book) → None so callers skip the gate.
    c = _cache()
    assert c.fillable_qty(T, "no", 0.50) is None


@pytest.mark.asyncio
async def test_divergent_from_rest_flags_stale_bid():
    c = _cache()
    c._orderbook_mode = True
    await c._handle_message(_snapshot())          # WS best yes bid = 0.22
    # REST says best yes bid 0.10 → 0.12 gap > tol → flagged (the stale-level bug)
    flagged = c.divergent_from_rest({T: (0.10, 0.50)}, tol=0.03)
    assert any(t[0] == T for t in flagged)
    # WS agrees with REST → not flagged
    assert c.divergent_from_rest({T: (0.22, 0.50)}, tol=0.03) == []
    # EXACTLY at tolerance flags [review CONCERN, deliberate `>=`]: on a cent grid an
    # exactly-<n> gap is common, this gates a real resnapshot, and the old float compare
    # flagged it only when float residue happened to tip `>` — now it always flags.
    assert any(t[0] == T for t in c.divergent_from_rest({T: (0.19, 0.50)}, tol=0.03))


def test_divergent_from_rest_empty_in_ticker_mode():
    c = _cache()  # _orderbook_mode False → integrity check disabled
    assert c.divergent_from_rest({T: (0.50, 0.60)}, tol=0.03) == []


@pytest.mark.asyncio
async def test_resnapshot_noop_when_disconnected():
    c = _cache()
    c._orderbook_mode = True
    await c.resnapshot()  # _ws is None → no error, no-op


@pytest.mark.asyncio
async def test_resnapshot_throttled(monkeypatch):
    from bot.kalshi.feed import config as feed_config
    monkeypatch.setattr(feed_config, "KALSHI_RESNAP_THROTTLE_SECONDS", 30.0)
    c = _cache()
    c._orderbook_mode = True
    assert await c.resnapshot() is True    # first fires
    assert await c.resnapshot() is False   # second within window → throttled
    assert c.book_health()["resnaps"] == 1  # storm prevented


def test_mark_all_suspect_covers_every_ticker():
    c = _cache()
    c.mark_all_suspect(5.0)
    assert c.is_suspect("ANY-TICKER") is True
    assert c.is_suspect(T) is True


@pytest.mark.asyncio
async def test_seq_gap_marks_all_suspect(monkeypatch):
    from bot.kalshi.feed import config as feed_config
    monkeypatch.setattr(feed_config, "KALSHI_BOOK_SUSPECT_SECONDS", 5.0)
    c = _cache()
    c._orderbook_mode = True
    await c._handle_message(_snapshot(seq=2))
    assert c.is_suspect(T) is False
    await c._handle_message(_delta("0.2200", "5.00", "yes", seq=5))  # gap 2→5
    assert c.is_suspect(T) is True  # gap on the shared stream → all suspect


@pytest.mark.asyncio
async def test_book_health_counts_gaps_and_resnaps():
    c = _cache()
    c._orderbook_mode = True
    await c._handle_message(_snapshot(seq=2))
    h = c.book_health()
    assert h["books"] == 1 and h["seq"] == 2 and h["gaps"] == 0 and h["resnaps"] == 0
    # seq jumps 2 → 5 (missed 3,4) → one gap counted
    await c._handle_message(_delta("0.2200", "5.00", "yes", seq=5))
    assert c.book_health()["gaps"] == 1
    # resnapshot (disconnected → no-op send, but counter still ticks the heal attempt)
    await c.resnapshot()
    assert c.book_health()["resnaps"] == 1


@pytest.mark.asyncio
async def test_delta_removes_top_level_and_redrives():
    c = _cache()
    await c._handle_message(_snapshot())
    # Remove the entire 0.22 yes level → best yes bid drops to 0.08.
    await c._handle_message(_delta("0.2200", "-333.00", "yes", seq=3))
    assert c.get_best_bid(T, "yes") == pytest.approx(0.08)
    assert c.get_best_ask(T, "no") == pytest.approx(0.92)  # 1 - 0.08
    assert c.get_depth(T, "no") == pytest.approx(300.0)


@pytest.mark.asyncio
async def test_delta_adds_and_increments_level():
    c = _cache()
    await c._handle_message(_snapshot())
    # Add a better yes bid at 0.30.
    await c._handle_message(_delta("0.3000", "50.00", "yes", seq=3))
    assert c.get_best_bid(T, "yes") == pytest.approx(0.30)
    assert c.get_depth(T, "no") == pytest.approx(50.0)


@pytest.mark.asyncio
async def test_delta_before_snapshot_is_ignored():
    c = _cache()
    await c._handle_message(_delta("0.2200", "-333.00", "yes"))
    assert c.get_best_bid(T, "yes") is None  # no book yet


def test_global_seq_no_false_gap_across_tickers():
    # Kalshi seq is one global counter across all markets — consecutive global
    # seqs (from different tickers interleaved) must NOT flag a gap.
    c = _cache()
    c._check_seq({"seq": 1})
    c._check_seq({"seq": 2})   # different market, next global seq
    c._check_seq({"seq": 3})
    assert c._seq_global == 3
    assert c._last_gap_action == 0.0  # no gap/resubscribe triggered


# ── the per-level observer (the private design notes M23) ─────────────────────────────────
#
# The maker needs to know WHICH level moved and by how much, because the queue ahead of a resting
# quote is a single level. `set_callback` only says "something changed", and a consumer that polls
# the derived book cannot see an add and a remove that cancel out between polls.


@pytest.mark.asyncio
async def test_level_observer_reports_the_applied_delta_and_the_resulting_level():
    c = _cache()
    seen = []
    c.set_level_observer(seen.append)
    await c._handle_message(_snapshot())
    await c._handle_message(_delta("0.2200", "-33.00", "yes", seq=3))

    assert seen[0] == {"kind": "snapshot", "ticker": T}
    assert seen[1] == {"kind": "delta", "ticker": T, "side": "yes",
                       "price": Decimal("0.2200"), "delta": Decimal("-33.00"),
                       "qty_before": Decimal("333.00"), "qty_after": Decimal("300.00")}


@pytest.mark.asyncio
async def test_a_fully_removed_level_reports_qty_after_zero_not_the_pre_pop_value():
    """`qty_after` must be what the level IS, so a consumer summing removals against it agrees with
    the book. The pop branch is where a stale local would have leaked the dust value."""
    c = _cache()
    seen = []
    c.set_level_observer(seen.append)
    await c._handle_message(_snapshot())
    await c._handle_message(_delta("0.2200", "-333.00", "yes", seq=3))
    assert seen[-1]["qty_after"] == Decimal("0")
    assert c.get_best_bid(T, "yes") == pytest.approx(0.08)   # the level really is gone


@pytest.mark.asyncio
async def test_an_observer_that_raises_cannot_break_the_book_or_the_socket():
    """The observer runs inline on the receive loop that feeds the arb fire path. An escape here
    would propagate out of _handle_message and cycle the WS connection."""
    c = _cache()
    c.set_level_observer(lambda ev: 1 / 0)
    await c._handle_message(_snapshot())
    await c._handle_message(_delta("0.2200", "-33.00", "yes", seq=3))

    assert c.get_best_bid(T, "yes") == pytest.approx(0.22)   # book still maintained
    assert c.get_depth(T, "no") == pytest.approx(300.0)      # 333 - 33, delta really applied
    # and the failure is REPORTED, not lost — one snapshot event + one delta event
    assert sum(c.level_observer_errors.values()) == 2


@pytest.mark.asyncio
async def test_no_observer_means_no_work_and_a_clean_fault_record():
    """Default None keeps every existing caller — the arb bot included — paying nothing."""
    c = _cache()
    await c._handle_message(_snapshot())
    await c._handle_message(_delta("0.2200", "-33.00", "yes", seq=3))
    assert c.level_observer_errors == {}


@pytest.mark.asyncio
async def test_observer_sees_nothing_for_a_ticker_outside_the_prefix_filter():
    c = _cache()
    seen = []
    c.set_level_observer(seen.append)
    await c._handle_message(json.dumps({
        "type": "orderbook_snapshot", "sid": 2, "seq": 2,
        "msg": {"market_ticker": "KXNBAGAME-OTHER", "yes_dollars_fp": [["0.10", "5.00"]],
                "no_dollars_fp": [["0.10", "5.00"]]},
    }))
    assert seen == []
